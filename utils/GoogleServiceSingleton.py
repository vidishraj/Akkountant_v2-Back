from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from utils.logger import Logger


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
                credentials.refresh(Request())
                # Update the token info with the refreshed token
                token_info['token'] = credentials.token
                token_info['expiry'] = credentials.expiry
            elif credentials.expired:
                self.logger.error(f"Token for {service_name} expired and no refresh token is available.")
                return None

            # Initialize and cache the Google service
            service = build(service_name, api_version, credentials=credentials)
            return service
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
            scopes=['https://www.googleapis.com/auth/drive']
        )

        if drive_service:
            self._user_services.setdefault(user_id, {})['drive'] = drive_service
            self.logger.info(f"Drive service initialized for user {user_id}.")

        return drive_service
