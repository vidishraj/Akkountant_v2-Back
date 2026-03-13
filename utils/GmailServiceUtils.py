from concurrent.futures import ThreadPoolExecutor, as_completed

from services.StatementDownloadService import StatementDownloadService
from utils.GoogleServiceSingleton import GoogleServiceSingleton
from utils.logger import Logger
from datetime import datetime
from typing import Iterator, Dict, Any, List

# Concurrency settings for metadata fetching
METADATA_FETCH_WORKERS = 10
METADATA_BATCH_SIZE = 50


class GmailServiceUtils:

    def __init__(self):
        self.googleService = GoogleServiceSingleton()
        self.logger = Logger(__name__).get_logger()
        self.PAGE_SIZE = 100  # Number of emails to fetch per page

    def findEmailInIntervalForPattern(self, userId, token, pattern, dateFrom, dateTo):
        gmailService = self.googleService.get_gmail_service(userId, token)
        emailSnippets = gmailService.users().messages().list(
            userId='me',
            q=pattern + f" after:{dateFrom} before:{dateTo}"
        ).execute().get('messages', [])

        emails_with_timestamps = []

        for email in emailSnippets:
            # Get full email details
            email_data = gmailService.users().messages().get(userId="me", id=email['id']).execute()
            snippet = email_data['snippet']
            internal_date = email_data.get('internalDate')  # Timestamp in milliseconds

            if internal_date:
                # Convert timestamp to human-readable format
                email_time = datetime.utcfromtimestamp(int(internal_date) / 1000).strftime('%Y-%m-%d %H:%M:%S')
                emails_with_timestamps.append({'time': email_time, 'message': snippet})
            else:
                # Handle cases where 'internalDate' is missing
                emails_with_timestamps.append({'time': None, 'message': snippet})

        return emails_with_timestamps

    def downloadFilesInRange(self, userId, token, password, bankType, dateTo, dateFrom):
        gmailService = self.googleService.get_gmail_service(userId, token)
        statementDownloader = StatementDownloadService(gmailService=gmailService, password=password)
        return statementDownloader.route_download_process(bankType, dateTo, dateFrom)

    def checkStatus(self, token):
        return self.googleService.is_token_valid(token)

    def _get_email_details(self, gmailService: Any, email_id: str) -> Dict[str, Any]:
        """Get essential email details + PDF attachment check in a single API call.

        Uses format='full' with a fields mask so we get headers AND the parts
        structure (filenames/mimeTypes) without downloading body data.
        """
        email_data = gmailService.users().messages().get(
            userId="me",
            id=email_id,
            format='full',
            fields='id,internalDate,snippet,payload/headers,payload/mimeType,'
                   'payload/parts(filename,mimeType,parts/filename,parts/mimeType)'
        ).execute()

        # Get subject, sender, and message-id from headers
        headers = email_data.get('payload', {}).get('headers', [])
        subject = next(
            (header['value'] for header in headers
             if header['name'].lower() == 'subject'),
            ''
        )
        sender = next(
            (header['value'] for header in headers
             if header['name'].lower() == 'from'),
            ''
        )
        header_message_id = next(
            (header['value'] for header in headers
             if header['name'].lower() == 'message-id'),
            None
        )

        # Get snippet instead of full body
        snippet = email_data.get('snippet', '')
        internal_date = email_data.get('internalDate')

        if internal_date:
            email_time = datetime.utcfromtimestamp(int(internal_date) / 1000).strftime('%Y-%m-%d %H:%M:%S')
        else:
            email_time = None

        # Check for PDF attachments from the parts structure (no extra API call)
        has_pdf = self._check_for_pdf_in_payload(email_data.get('payload', {}))

        return {
            'time': email_time,
            'subject': subject,
            'sender': sender,
            'message': snippet,
            'message_id': email_id,
            'gmail_id': email_id,
            'header_message_id': header_message_id,
            '_has_pdf': has_pdf,
        }

    @staticmethod
    def _check_for_pdf_in_payload(payload: Dict[str, Any]) -> bool:
        """Recursively check if the payload contains any PDF attachment parts."""
        parts = payload.get('parts', [])
        for part in parts:
            filename = part.get('filename', '')
            if filename and filename.lower().endswith('.pdf'):
                return True
            mime = part.get('mimeType', '')
            if mime == 'application/pdf':
                return True
            # Check nested parts (e.g. multipart/mixed → multipart/alternative → parts)
            if GmailServiceUtils._check_for_pdf_in_payload(part):
                return True
        return False

    def _build_gmail_service(self, token: Any) -> Any:
        """Build a fresh Gmail API service instance (not cached).

        Used to create per-thread clients since httplib2 is not thread-safe.
        """
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build

        credentials = Credentials(
            token=token['token'],
            refresh_token=token.get('refresh_token'),
            client_id=token.get('client_id'),
            client_secret=token.get('client_secret'),
            token_uri="https://oauth2.googleapis.com/token",
            scopes=['https://www.googleapis.com/auth/gmail.readonly'],
        )
        return build('gmail', 'v1', credentials=credentials)

    def _fetch_email_details_with_own_client(self, token: Any, email_id: str) -> Dict[str, Any]:
        """Build a thread-local Gmail client and fetch a single email's details.

        Each call creates its own httplib2 transport so there are no SSL
        conflicts when called from multiple threads concurrently.
        """
        gmail_svc = self._build_gmail_service(token)
        return self._get_email_details(gmail_svc, email_id)

    def iter_emails_in_interval(self, userId: str, token: str, dateFrom: str, dateTo: str) -> Iterator[Dict[str, Any]]:
        """Fetch emails concurrently using ThreadPoolExecutor.

        Phase 1: Collect all message IDs via pagination (sequential, lightweight).
        Phase 2: Fetch full details in concurrent batches of METADATA_BATCH_SIZE
                 with METADATA_FETCH_WORKERS threads. Each thread builds its own
                 Gmail API client to avoid httplib2 SSL conflicts.
        """
        gmailService = self.googleService.get_gmail_service(userId, token)

        # ── Phase 1: Collect all message IDs ──────────────────────────
        all_message_ids: List[str] = []
        request = gmailService.users().messages().list(
            userId='me',
            q=f"after:{dateFrom} before:{dateTo}",
            maxResults=min(500, self.PAGE_SIZE * 5),
            includeSpamTrash=True,
        )

        while request is not None:
            response = request.execute()
            messages = response.get('messages', [])
            if messages:
                all_message_ids.extend(m['id'] for m in messages)
            request = gmailService.users().messages().list_next(request, response)

        if not all_message_ids:
            return

        self.logger.info(
            f"Collected {len(all_message_ids)} message IDs, "
            f"fetching details with {METADATA_FETCH_WORKERS} workers"
        )

        # ── Phase 2: Fetch details concurrently in batches ────────────
        # Each thread gets its own Gmail API client (httplib2 is NOT thread-safe)
        for batch_start in range(0, len(all_message_ids), METADATA_BATCH_SIZE):
            batch_ids = all_message_ids[batch_start:batch_start + METADATA_BATCH_SIZE]

            with ThreadPoolExecutor(max_workers=METADATA_FETCH_WORKERS) as executor:
                future_to_id = {
                    executor.submit(self._fetch_email_details_with_own_client, token, mid): mid
                    for mid in batch_ids
                }
                for future in as_completed(future_to_id):
                    mid = future_to_id[future]
                    try:
                        yield future.result()
                    except Exception as e:
                        self.logger.warning(f"Error processing email {mid}: {str(e)}")

    def findAllEmailsInInterval(self, userId: str, token: str, dateFrom: str, dateTo: str) -> Iterator[Dict[str, Any]]:
        """Memory-efficient version that returns an iterator instead of a list"""
        # OPTIMIZATION: Break large date ranges into 3-day chunks for better performance
        date_chunks = self._split_date_range_into_chunks(dateFrom, dateTo, chunk_days=3)
        
        if len(date_chunks) > 1:
            self.logger.info(f"Breaking date range {dateFrom} to {dateTo} into {len(date_chunks)} chunks of 3 days each")
            
            # Process chunks sequentially to avoid SSL/connection issues
            # Note: Parallel Gmail API calls were causing SSL conflicts and worker crashes
            for i, (chunk_start, chunk_end) in enumerate(date_chunks):
                self.logger.debug(f"Processing chunk {i+1}/{len(date_chunks)}: {chunk_start} to {chunk_end}")
                chunk_emails = self.iter_emails_in_interval(userId, token, chunk_start, chunk_end)
                for email in chunk_emails:
                    yield email
        else:
            # Single chunk, process normally
            yield from self.iter_emails_in_interval(userId, token, dateFrom, dateTo)


    def _split_date_range_into_chunks(self, dateFrom: str, dateTo: str, chunk_days: int = 3):
        """Split date range into smaller chunks for better Gmail API performance"""
        from datetime import datetime, timedelta
        
        try:
            # Parse dates (assuming YYYY/M/D format)
            start_date = datetime.strptime(dateFrom, '%Y/%m/%d')
            end_date = datetime.strptime(dateTo, '%Y/%m/%d')
            
            # Calculate total days
            total_days = (end_date - start_date).days
            
            # If range is <= chunk_days, return single chunk
            if total_days <= chunk_days:
                return [(dateFrom, dateTo)]
            
            # Split into chunks
            chunks = []
            current_date = start_date
            
            while current_date < end_date:
                chunk_end_date = min(current_date + timedelta(days=chunk_days), end_date)
                
                chunk_start_str = current_date.strftime('%Y/%m/%d')
                chunk_end_str = chunk_end_date.strftime('%Y/%m/%d')
                
                chunks.append((chunk_start_str, chunk_end_str))
                current_date = chunk_end_date + timedelta(days=1)  # Move to next day after chunk end
            
            return chunks
            
        except Exception as e:
            self.logger.warning(f"Date chunking failed: {str(e)}, using original range")
            return [(dateFrom, dateTo)]


