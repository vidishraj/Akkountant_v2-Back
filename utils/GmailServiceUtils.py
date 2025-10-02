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
            
            # Process emails individually - batch API has configuration issues
            if messages:
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


