import os
import base64
from flask import g
from werkzeug.utils import secure_filename
from models.freelance_management import Signature
from services.Base_Service import BaseService
from utils.logger import Logger


class SignatureService(BaseService):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SignatureService, cls).__new__(cls)
            cls.logger = Logger(__name__).get_logger()
        return cls._instance

    def __init__(self):
        super().__init__()
        self.upload_folder = os.path.join(os.getcwd(), 'tmp', 'signatures')
        self._ensure_upload_folder()

    def _ensure_upload_folder(self):
        if not os.path.exists(self.upload_folder):
            os.makedirs(self.upload_folder)

    def create_signature(self, signature_data, name, is_default=False):
        try:
            user_id = g.get('firebase_id')
            if not user_id:
                raise ValueError("User ID is required")

            # Reset default signature if this is being set as default
            if is_default:
                self.db.session.query(Signature).filter_by(
                    user_id=user_id, 
                    is_default=True
                ).update({"is_default": False})

            signature = Signature(
                user_id=user_id,
                name=name,
                signature_data=signature_data,
                signature_type='image',
                is_default=is_default
            )

            self.db.session.add(signature)
            self.db.session.commit()
            
            self.logger.info(f"Signature created successfully: {signature.id}")
            return signature

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error creating signature: {str(e)}")
            raise

    def upload_signature_file(self, file, name, is_default=False):
        try:
            if not file or file.filename == '':
                raise ValueError("No file provided")

            # Validate file type
            allowed_extensions = {'png', 'jpg', 'jpeg', 'gif', 'svg'}
            if not ('.' in file.filename and 
                    file.filename.rsplit('.', 1)[1].lower() in allowed_extensions):
                raise ValueError("Invalid file type. Allowed: PNG, JPG, JPEG, GIF, SVG")

            # Secure filename
            filename = secure_filename(file.filename)
            user_id = g.get('firebase_id')
            filename = f"{user_id}_{filename}"
            
            # Save file
            filepath = os.path.join(self.upload_folder, filename)
            file.save(filepath)

            # Convert to base64 for storage
            with open(filepath, 'rb') as f:
                signature_data = base64.b64encode(f.read()).decode('utf-8')

            # Create signature record
            print(signature_data)
            signature = self.create_signature(signature_data, name, is_default)

            # Clean up temp file
            os.remove(filepath)

            return signature

        except Exception as e:
            self.logger.error(f"Error uploading signature file: {str(e)}")
            raise

    def get_signatures(self):
        try:
            user_id = g.get('firebase_id')
            if not user_id:
                raise ValueError("User ID is required")

            signatures = self.db.session.query(Signature).filter_by(user_id=user_id).all()
            return signatures

        except Exception as e:
            self.logger.error(f"Error fetching signatures: {str(e)}")
            raise

    def get_signature_by_id(self, signature_id):
        try:
            user_id = g.get('firebase_id')
            signature = self.db.session.query(Signature).filter_by(
                id=signature_id, 
                user_id=user_id
            ).first()

            if not signature:
                raise ValueError("Signature not found")

            return signature

        except Exception as e:
            self.logger.error(f"Error fetching signature: {str(e)}")
            raise

    def delete_signature(self, signature_id):
        try:
            user_id = g.get('firebase_id')
            signature = self.db.session.query(Signature).filter_by(
                id=signature_id, 
                user_id=user_id
            ).first()

            if not signature:
                raise ValueError("Signature not found")

            self.db.session.delete(signature)
            self.db.session.commit()
            
            self.logger.info(f"Signature deleted successfully: {signature_id}")
            return True

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error deleting signature: {str(e)}")
            raise

    def set_default_signature(self, signature_id):
        try:
            user_id = g.get('firebase_id')
            
            # Reset all signatures as non-default
            self.db.session.query(Signature).filter_by(
                user_id=user_id
            ).update({"is_default": False})

            # Set the specified signature as default
            signature = self.db.session.query(Signature).filter_by(
                id=signature_id, 
                user_id=user_id
            ).first()

            if not signature:
                raise ValueError("Signature not found")

            signature.is_default = True
            self.db.session.commit()
            
            self.logger.info(f"Default signature updated: {signature_id}")
            return signature

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error setting default signature: {str(e)}")
            raise