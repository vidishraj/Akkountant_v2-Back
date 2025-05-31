from services.StatementDownloadService import StatementDownloadService
from utils.GoogleServiceSingleton import GoogleServiceSingleton
from datetime import datetime
from typing import Iterator, Dict, Any


class GmailServiceUtils:

    def __init__(self):
        self.googleService = GoogleServiceSingleton()
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
            metadataHeaders=['subject']
        ).execute()

        # Get subject from headers
        subject = next(
            (header['value'] for header in email_data.get('payload', {}).get('headers', [])
             if header['name'].lower() == 'subject'),
            ''
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
            'message': snippet  # Using snippet instead of full body
        }

    def iter_emails_in_interval(self, userId: str, token: str, dateFrom: str, dateTo: str) -> Iterator[Dict[str, Any]]:
        """Iterator that yields emails one at a time to prevent memory buildup"""
        gmailService = self.googleService.get_gmail_service(userId, token)
        
        # Initial request
        request = gmailService.users().messages().list(
            userId='me',
            q=f"after:{dateFrom} before:{dateTo}",
            maxResults=self.PAGE_SIZE
        )

        while request is not None:
            response = request.execute()
            messages = response.get('messages', [])
            
            # Yield one email at a time
            for message in messages:
                try:
                    email_details = self._get_email_details(gmailService, message['id'])
                    yield email_details
                except Exception as e:
                    print(f"Error processing email {message['id']}: {str(e)}")
                    continue

            # Get the next page of emails
            request = gmailService.users().messages().list_next(request, response)

    def findAllEmailsInInterval(self, userId: str, token: str, dateFrom: str, dateTo: str) -> Iterator[Dict[str, Any]]:
        """Memory-efficient version that returns an iterator instead of a list"""
        return self.iter_emails_in_interval(userId, token, dateFrom, dateTo)
