import os
from flask import request, jsonify
from firebase_admin import auth, credentials, initialize_app
from functools import wraps
from utils.logger import Logger
from dotenv import load_dotenv

load_dotenv()


class FirebaseAuthenticator:
    """Class to handle Firebase Authentication."""

    def __init__(self):
        """Initialize Firebase Admin SDK using credentials from the environment."""
        cred_path = os.getenv('FIREBASE_CREDENTIALS_PATH')
        cred = credentials.Certificate(cred_path)
        initialize_app(cred)
        self.logger = Logger(__name__).get_logger()

    def verify_token(self, token: str):
        """
        Verifying the Firebase token passed from the client.
        /:param token: Firebase token from the request header.
        :return: Decoded token if valid, raises exception if invalid.
        """
        try:
            decoded_token = auth.verify_id_token(token)
            self.logger.info(f"Token verified for user: {decoded_token['uid']}")
            return decoded_token
        except Exception as e:
            self.logger.error(f"Token verification failed: {str(e)}")
            raise


def require_authentication(f):
    """
    Decorator function to ensure that a valid Firebase token is present in the request.
    """

    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return jsonify({"error": "Authorization header is missing"}), 401

        token = auth_header.split(" ")[1]  # Token should be passed as Bearer token
        authenticator = FirebaseAuthenticator()

        try:
            decoded_token = authenticator.verify_token(token)
            request.user = decoded_token  # Attach user info to the request object
        except Exception:
            return jsonify({"error": "Invalid or expired token"}), 401

        return f(*args, **kwargs)

    return decorated_function
