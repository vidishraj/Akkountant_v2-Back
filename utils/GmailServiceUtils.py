from services.StatementDownloadService import StatementDownloadService
from utils.GoogleServiceSingleton import GoogleServiceSingleton
from datetime import datetime

class GmailServiceUtils:

    def __init__(self):
        self.googleService = GoogleServiceSingleton()


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
