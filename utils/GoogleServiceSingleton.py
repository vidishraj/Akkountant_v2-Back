import os

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from utils.logger import Logger
from google.auth.exceptions import RefreshError
from google_auth_oauthlib.flow import InstalledAppFlow


class GoogleServiceSingleton:
    """Singleton class to provide authenticated Gmail and Google Drive service instances using an externally provided
    token. """

    _instance = None
    _user_services = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(GoogleServiceSingleton, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        # Avoid reinitializing if already initialized
        if not hasattr(self, 'initialized'):
            self.logger = Logger(__name__).get_logger()

    def _initialize_service(self, token_info, service_name, api_version, scopes):
        """Initializes a Google service if token is valid or renews it if expired."""
        try:
            credentials = Credentials(
                token=token_info['token'],
                refresh_token=token_info.get('refresh_token'),
                client_id=token_info.get('client_id'),
                client_secret=token_info.get('client_secret'),
                token_uri="https://oauth2.googleapis.com/token",
                scopes=scopes
            )

            # Check if token needs refreshing
            if credentials.expired and credentials.refresh_token:
                self.logger.info(f"Token expired for {service_name}. Refreshing token.")

                try:
                    credentials.refresh(Request())
                    # Update the token info with the refreshed token
                    token_info['token'] = credentials.token
                    token_info['expiry'] = credentials.expiry
                except RefreshError as e:
                    self.logger.error(f"Failed to refresh token for {service_name}. Reason: {e}")
                    return None
            elif credentials.expired:
                self.logger.info(self.start_fresh_auth_flow(scopes))
                self.logger.error(f"Token for {service_name} expired and no refresh token is available.")
                return None

            # Initialize and cache the Google service
            service = build(service_name, api_version, credentials=credentials)
            return service
        except RefreshError as e:
            self.logger.error(
                f"Failed to initialize {service_name} service due to invalid grant (token expired or revoked): {e}")
            return None
        except Exception as e:
            self.logger.error(f"Failed to initialize {service_name} service: {e}")
            return None

    def get_gmail_service(self, user_id, token_info):
        """Return the Gmail service instance using the provided token info."""
        if user_id in self._user_services and 'gmail' in self._user_services[user_id]:
            return self._user_services[user_id]['gmail']

        gmail_service = self._initialize_service(
            token_info,
            service_name='gmail',
            api_version='v1',
            scopes=['https://www.googleapis.com/auth/gmail.readonly']
        )

        if gmail_service:
            self._user_services.setdefault(user_id, {})['gmail'] = gmail_service
            self.logger.info(f"Gmail service initialized for user {user_id}.")

        return gmail_service

    def get_drive_service(self, user_id, token_info):
        """Return the Google Drive service instance using the provided token info."""
        if user_id in self._user_services and 'drive' in self._user_services[user_id]:
            return self._user_services[user_id]['drive']

        drive_service = self._initialize_service(
            token_info,
            service_name='drive',
            api_version='v3',
            scopes=['https://www.googleapis.com/auth/drive.file']
        )

        if drive_service:
            self._user_services.setdefault(user_id, {})['drive'] = drive_service
            self.logger.info(f"Drive service initialized for user {user_id}.")

        return drive_service

    def start_fresh_auth_flow(self, scopes):
        """
        Start a fresh OAuth 2.0 flow for Gmail or Google Drive and return the new token information.
        """
        try:
            client_secrets_file = os.path.join(os.getcwd(), "client_secret.json")
            flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file, scopes)
            return {"Auth": flow.client_config}
        except Exception as e:
            self.logger.error(f"Failed to start fresh auth flow: {e}")
            return None

    def is_token_valid(self, token_info: dict) -> bool:
        """
        Checks if a user's Google API token is still valid.

        Args:
            token_info (dict): A dictionary containing the user's token information with keys:
                - 'token': Access token string
                - 'refresh_token': Refresh token string (optional)
                - 'client_id': OAuth client ID
                - 'client_secret': OAuth client secret
                - 'token_uri': Token URI (default: "https://oauth2.googleapis.com/token")
                - 'scopes': List of required scopes (optional)

        Returns:
            bool: True if the token is valid, False otherwise.
        """
        try:
            credentials = Credentials(
                token=token_info.get("token"),
                refresh_token=token_info.get("refresh_token"),
                client_id=token_info.get("client_id"),
                client_secret=token_info.get("client_secret"),
                token_uri="https://oauth2.googleapis.com/token",
                scopes=token_info.get("scopes", [])
            )

            # Check if token is expired
            if credentials.expired:
                if credentials.refresh_token:
                    # Attempt to refresh the token
                    credentials.refresh(Request())
                    return True
                else:
                    # Token is expired and no refresh token is available
                    return False

            # Token is valid and not expired
            return True

        except RefreshError:
            # Refresh failed (e.g., invalid or revoked refresh token)
            return False
        except Exception as e:
            # Handle other errors (e.g., malformed token info)
            self.logger.error(f"Error checking token validity: {e}")
            return False
    @staticmethod
    def getGmailScope():
        return ['https://www.googleapis.com/auth/gmail.readonly']

    @staticmethod
    def getDriveScope():
        return ['https://www.googleapis.com/auth/drive.file']
