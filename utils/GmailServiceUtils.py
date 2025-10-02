from services.StatementDownloadService import StatementDownloadService
from utils.GoogleServiceSingleton import GoogleServiceSingleton
from utils.logger import Logger
from datetime import datetime
from typing import Iterator, Dict, Any


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
        """Get essential email details with memory-efficient fields"""
        email_data = gmailService.users().messages().get(
            userId="me", 
            id=email_id,
            format='metadata',
            metadataHeaders=['subject', 'from', 'message-id']
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
        message_id = next(
            (header['value'] for header in headers
             if header['name'].lower() == 'message-id'),
            email_id  # Fallback to Gmail ID if Message-Id header not found
        )

        # Get snippet instead of full body
        snippet = email_data.get('snippet', '')
        internal_date = email_data.get('internalDate')

        if internal_date:
            email_time = datetime.utcfromtimestamp(int(internal_date) / 1000).strftime('%Y-%m-%d %H:%M:%S')
        else:
            email_time = None

        return {
            'time': email_time,
            'subject': subject,
            'sender': sender,  # Add sender information
            'message': snippet,  # Using snippet instead of full body
            'message_id': message_id  # Use actual Message-Id header for uniqueness
        }

    def iter_emails_in_interval(self, userId: str, token: str, dateFrom: str, dateTo: str) -> Iterator[Dict[str, Any]]:
        """Iterator that yields emails one at a time to prevent memory buildup"""
        gmailService = self.googleService.get_gmail_service(userId, token)
        
        # OPTIMIZATION: Initial request with deduplication and larger page size
        request = gmailService.users().messages().list(
            userId='me',
            q=f"after:{dateFrom} before:{dateTo} in:inbox -is:sent",
            maxResults=min(500, self.PAGE_SIZE * 5),  # Larger batches for faster fetching
            includeSpamTrash=False,
            labelIds=["INBOX"]
        )

        while request is not None:
            response = request.execute()
            messages = response.get('messages', [])
            
            # OPTIMIZATION: Batch fetch email details using proper Gmail batch API
            if messages:
                try:
                    # Process in batches of up to 100 (Gmail API limit)
                    batch_size = min(100, len(messages))
                    for i in range(0, len(messages), batch_size):
                        batch_messages = messages[i:i + batch_size]
                        email_batch = self._get_batch_email_details(gmailService, batch_messages)
                        for email_details in email_batch:
                            if email_details:  # Only yield valid results
                                yield email_details
                except Exception as e:
                    # Fallback to individual fetching if batch fails
                    self.logger.warning(f"Batch fetch failed, falling back to individual: {str(e)}")
                    for message in messages:
                        try:
                            email_details = self._get_email_details(gmailService, message['id'])
                            yield email_details
                        except Exception as e:
                            self.logger.warning(f"Error processing email {message['id']}: {str(e)}")
                            continue

            # Get the next page of emails
            request = gmailService.users().messages().list_next(request, response)

    def findAllEmailsInInterval(self, userId: str, token: str, dateFrom: str, dateTo: str) -> Iterator[Dict[str, Any]]:
        """Memory-efficient version that returns an iterator instead of a list"""
        return self.iter_emails_in_interval(userId, token, dateFrom, dateTo)

    def _get_batch_email_details(self, gmailService, messages):
        """FIXED: Proper Gmail batch API implementation following official guide"""
        try:
            from googleapiclient.http import BatchHttpRequest
            
            email_results = []
            
            def callback(request_id, response, exception):
                if exception is not None:
                    self.logger.warning(f"Batch request {request_id} failed: {exception}")
                    return
                    
                try:
                    # Parse the response using existing method
                    email_details = self._parse_email_response(response)
                    if email_details:
                        email_results.append(email_details)
                except Exception as e:
                    self.logger.warning(f"Error parsing batch response {request_id}: {str(e)}")
            
            # Create batch request
            batch = BatchHttpRequest(callback=callback)
            
            # Add individual requests to batch
            for message in messages:
                request = gmailService.users().messages().get(
                    userId='me', 
                    id=message['id'],
                    format='full'  # Get full email content
                )
                batch.add(request)
            
            # Execute the batch
            batch.execute(http=gmailService._http)
            
            return email_results
                    
        except Exception as e:
            # If batch processing fails, raise exception to trigger fallback
            raise Exception(f"Batch processing failed: {str(e)}")

    def _parse_email_response(self, email_response):
        """Parse Gmail API email response into our standard format"""
        try:
            headers = email_response.get('payload', {}).get('headers', [])
            header_dict = {header['name'].lower(): header['value'] for header in headers}
            
            # Extract email content
            body = self._extract_email_body(email_response.get('payload', {}))
            
            # Get internal date and convert to readable format
            internal_date = email_response.get('internalDate')
            if internal_date:
                email_time = datetime.utcfromtimestamp(int(internal_date) / 1000).strftime('%Y-%m-%d %H:%M:%S')
            else:
                email_time = header_dict.get('date', '')
            
            return {
                'message_id': header_dict.get('message-id', ''),
                'subject': header_dict.get('subject', ''),
                'sender': header_dict.get('from', ''),
                'time': email_time,
                'message': body or email_response.get('snippet', ''),  # Fallback to snippet if body extraction fails
                'headers': header_dict
            }
        except Exception as e:
            self.logger.warning(f"Error parsing email response: {str(e)}")
            return None

    def _extract_email_body(self, payload):
        """Extract email body from Gmail payload"""
        try:
            # Handle multipart messages
            if 'parts' in payload:
                for part in payload['parts']:
                    if part.get('mimeType') == 'text/plain':
                        data = part.get('body', {}).get('data', '')
                        if data:
                            import base64
                            return base64.urlsafe_b64decode(data.encode('utf-8')).decode('utf-8')
                    elif part.get('mimeType') == 'text/html':
                        data = part.get('body', {}).get('data', '')
                        if data:
                            import base64
                            try:
                                from bs4 import BeautifulSoup
                                html_content = base64.urlsafe_b64decode(data.encode('utf-8')).decode('utf-8')
                                soup = BeautifulSoup(html_content, 'html.parser')
                                return soup.get_text()
                            except ImportError:
                                # If BeautifulSoup not available, return raw HTML
                                return base64.urlsafe_b64decode(data.encode('utf-8')).decode('utf-8')
            
            # Handle single part messages
            elif payload.get('mimeType') == 'text/plain':
                data = payload.get('body', {}).get('data', '')
                if data:
                    import base64
                    return base64.urlsafe_b64decode(data.encode('utf-8')).decode('utf-8')
                    
            return ""
        except Exception as e:
            self.logger.warning(f"Error extracting email body: {str(e)}")
            return ""

