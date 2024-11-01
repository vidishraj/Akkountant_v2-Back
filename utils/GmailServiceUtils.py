from utils.GoogleServiceSingleton import GoogleServiceSingleton


class GmailServiceUtils:

    def __init__(self):
        self.googleService = GoogleServiceSingleton()

    def findEmailInIntervalForPattern(self, userId, token, pattern, dateFrom, dateTo):
        gmailService = self.googleService.get_gmail_service(userId, token)
        emailSnippets = gmailService.users().messages().list(userId='me', q=pattern + f" after:{dateFrom}"
                                                        f" before:{dateTo}").execute().get('messages', [])

        return [gmailService.users().messages().get(userId="me", id=email['id']).execute()['snippet'] for email in
                emailSnippets]
