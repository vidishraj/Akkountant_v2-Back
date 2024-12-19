import os
import base64
import requests
from urllib.parse import urlparse, parse_qs
from werkzeug.utils import secure_filename
from bs4 import BeautifulSoup
from enums.StatementPatternEnum import StatementPatternEnum
from utils.DateTimeUtil import DateTimeUtil
from utils.logger import Logger

TEMP_DIR = os.getcwd() + '/tmp'
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 " \
             "Safari/537.36 "


class StatementDownloadService:
    _instance = None  # Class-level variable to hold the singleton instance

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(StatementDownloadService, cls).__new__(cls)
        return cls._instance

    def __init__(self, password=None, gmailService=None):
        # Avoid reinitializing if already initialized
        if not hasattr(self, 'initialized'):
            self.logger = Logger(__name__).get_logger()
            self.gmail_service = gmailService
            self.password = password
            self.initialized = True  # Mark the instance as initialized

    def route_download_process(self, bank_type, date_to=None, date_from=None):
        statement_pattern = StatementPatternEnum[bank_type].value
        date_from = date_from or DateTimeUtil.currentMonthDatesForEmail()
        date_to = date_to or date_from
        os.makedirs(TEMP_DIR, exist_ok=True)
        files = []
        if bank_type == StatementPatternEnum.HDFC_DEBIT.name:
            hrefs = self.download_pdf_from_smart_statement(statement_pattern, date_to, date_from)
            if hrefs:
                files = self.download_files_from_hrefs(hrefs)
            else:
                self.logger.warning("No hrefs found for download.")
        else:
            files = self.download_to_temp(statement_pattern, date_to, date_from)

        self.logger.info("Finished downloading files to temp")
        return files

    def download_to_temp(self, search_string, date_to, date_from):
        messages = self._fetch_emails(search_string, date_from, date_to)
        files = []

        for index, message in enumerate(messages):
            attachments = self._extract_attachments(message)
            for attachment in attachments:
                filename = self._save_attachment(attachment, index)
                files.append(filename)

        self.logger.info(f"Downloaded {len(files)} files to temp")
        return files

    def download_pdf_from_smart_statement(self, search_string, date_to, date_from):
        messages = self._fetch_emails(search_string, date_from, date_to)
        hrefs = []

        for index, message in enumerate(messages):
            href = self._extract_smart_statement_link(message)
            if href:
                hrefs.append(href)

        self.logger.info(f"Extracted {len(hrefs)} links from emails")
        return hrefs

    def download_files_from_hrefs(self, hrefs):
        job_req_list = [self._parse_href(link) for link in hrefs if link]
        return self._download_hdfc_statements(job_req_list)

    def _fetch_emails(self, search_string, date_from, date_to):
        query = f"{search_string} after:{date_from} before:{date_to}"
        try:
            results = self.gmail_service.users().messages().list(userId='me', q=query).execute()
            emails = results.get('messages', [])
            self.logger.info(f"Found {len(emails)} statements in the range")
            return emails
        except Exception as e:
            self.logger.error(f"Error fetching emails: {e}")
            return []

    def _extract_attachments(self, message):
        try:
            msg = self.gmail_service.users().messages().get(userId='me', id=message['id']).execute()
            parts = msg['payload'].get('parts', [])
            attachments = []
            for part in parts:
                if part['filename']:
                    attachment_data = self._get_attachment_data(part, message['id'])
                    if attachment_data:
                        attachments.append((part['filename'], attachment_data))
            return attachments
        except Exception as e:
            self.logger.error(f"Error extracting attachments: {e}")
            return []

    def _get_attachment_data(self, part, message_id):
        try:
            if 'data' in part['body']:
                return base64.urlsafe_b64decode(part['body']['data'].encode('UTF-8'))
            elif 'attachmentId' in part['body']:
                attachment_id = part['body']['attachmentId']
                attachment = self.gmail_service.users().messages().attachments().get(
                    userId='me', messageId=message_id, id=attachment_id
                ).execute()
                return base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))
        except Exception as e:
            self.logger.error(f"Error fetching attachment data: {e}")
        return None

    def _save_attachment(self, attachment, index):
        filename, file_data = attachment
        ext = filename.split('.')[-1]
        secure_name = secure_filename(f"file_{index}.{ext}")
        file_path = os.path.join(TEMP_DIR, secure_name)
        with open(file_path, 'wb') as file:
            file.write(file_data)

        self.logger.info(f"Attachment {secure_name} downloaded.")
        return secure_name

    def _extract_smart_statement_link(self, message):
        try:
            msg = self.gmail_service.users().messages().get(userId='me', id=message['id']).execute()
            parts = msg['payload'].get('parts', [])
            part = None
            if len(parts) > 0:
                part = parts[0]
            while part:
                data = part['body'].get('data')
                if data:
                    decoded_data = base64.urlsafe_b64decode(data).decode('utf-8')
                    soup = BeautifulSoup(decoded_data, 'html.parser')
                    td_tag = soup.find('td', style="background-color: #004b8d; padding: 12px; font-size: 14px; "
                                                   "letter-spacing: 1px; border-radius: 5px;")
                    if td_tag:
                        a_tag = td_tag.find('a')
                        if a_tag and 'href' in a_tag.attrs:
                            return a_tag['href']
                parts = part.get('parts', [])
                if len(parts) > 0:
                    part = parts[0]
                else:
                    part = None
        except Exception as e:
            self.logger.error(f"Error extracting statement link: {e}")
        return None

    @staticmethod
    def _parse_href(link):
        parsed_url = urlparse(link)
        query_params = parse_qs(parsed_url.query)
        job_key = query_params.get('jobkey', [None])[0]
        response = requests.get(link, headers={"User-Agent": USER_AGENT})
        soup = BeautifulSoup(response.text, 'html.parser')
        seq_element = soup.find('input', {'type': 'hidden', 'name': 'seqence', 'id': 'seqence'})

        if seq_element and 'value' in seq_element.attrs:
            return [seq_element['value'], job_key]
        return None

    def _download_hdfc_statements(self, job_req_list):
        downloaded_files = []

        for req in job_req_list:
            if req:
                req_id, job_key = req
                link = f"https://smartstatements.hdfcbank.com/HDFCRestFulService/webresources/app/pdfformat?jobkey=" \
                       f"{job_key}&reqid={req_id}&format=pdf"
                response = requests.post(link, headers={"User-Agent": USER_AGENT})

                if response.status_code == 200:
                    filename = f"HDFC_Statement_{job_key}_{req_id}.pdf"
                    file_path = os.path.join(TEMP_DIR, filename)

                    with open(file_path, 'wb') as pdf_file:
                        pdf_file.write(response.content)

                    downloaded_files.append(filename)
                    self.logger.info(f"Downloaded file {filename}")
                else:
                    self.logger.error(f"Failed to download for jobKey={job_key}, reqId={req_id}.")

        self.logger.info(f"Successfully downloaded {len(downloaded_files)} files")
        return downloaded_files
