import os
import uuid
import base64
from io import BytesIO
from flask import g
from werkzeug.utils import secure_filename
from models.user_files import UserFile, UserFolder
from services.Base_Service import BaseService
from utils.logger import Logger

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

try:
    from PIL import Image
except ImportError:
    Image = None

ALLOWED_EXTENSIONS = {'pdf', 'png', 'jpg', 'jpeg', 'gif', 'bmp', 'webp'}
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
THUMBNAIL_SIZE = (200, 200)


class FileStorageService(BaseService):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FileStorageService, cls).__new__(cls)
            cls.logger = Logger(__name__).get_logger()
        return cls._instance

    def __init__(self):
        super().__init__()
        self.base_upload_dir = os.path.join(os.getcwd(), 'uploads')
        os.makedirs(self.base_upload_dir, exist_ok=True)

    def _get_user_dir(self, user_id):
        user_dir = os.path.join(self.base_upload_dir, user_id)
        os.makedirs(user_dir, exist_ok=True)
        return user_dir

    def _validate_file(self, file):
        if not file or file.filename == '':
            raise ValueError("No file provided")
        ext = file.filename.rsplit('.', 1)[-1].lower() if '.' in file.filename else ''
        if ext not in ALLOWED_EXTENSIONS:
            raise ValueError(f"File type '{ext}' not allowed. Allowed: {', '.join(ALLOWED_EXTENSIONS)}")
        file.seek(0, 2)
        size = file.tell()
        file.seek(0)
        if size > MAX_FILE_SIZE:
            raise ValueError(f"File too large. Maximum size is {MAX_FILE_SIZE // (1024 * 1024)}MB")
        return ext, size

    def _generate_thumbnail(self, file_path, file_extension):
        try:
            if file_extension == 'pdf':
                return self._generate_pdf_thumbnail(file_path)
            elif file_extension in {'png', 'jpg', 'jpeg', 'gif', 'bmp', 'webp'}:
                return self._generate_image_thumbnail(file_path)
            return None
        except Exception as e:
            self.logger.warning(f"Thumbnail generation failed for {file_path}: {e}")
            return None

    def _generate_pdf_thumbnail(self, file_path):
        if not fitz or not Image:
            return None
        doc = fitz.open(file_path)
        page = doc[0]
        mat = fitz.Matrix(1.5, 1.5)
        pix = page.get_pixmap(matrix=mat)
        img_data = pix.tobytes("png")
        doc.close()
        img = Image.open(BytesIO(img_data))
        img.thumbnail(THUMBNAIL_SIZE, Image.LANCZOS)
        buffer = BytesIO()
        img.save(buffer, format='PNG')
        return base64.b64encode(buffer.getvalue()).decode('utf-8')

    def _generate_image_thumbnail(self, file_path):
        if not Image:
            return None
        img = Image.open(file_path)
        img.thumbnail(THUMBNAIL_SIZE, Image.LANCZOS)
        buffer = BytesIO()
        fmt = 'PNG' if img.mode == 'RGBA' else 'JPEG'
        img.save(buffer, format=fmt)
        return base64.b64encode(buffer.getvalue()).decode('utf-8')

    def upload_file(self, file, label=None, folder_id=None):
        user_id = g.get('firebase_id')
        if not user_id:
            raise ValueError("User ID is required")

        if folder_id:
            folder = self.db.session.query(UserFolder).filter_by(
                id=folder_id, user_id=user_id
            ).first()
            if not folder:
                raise ValueError("Folder not found")

        ext, size = self._validate_file(file)
        original_filename = secure_filename(file.filename)
        stored_filename = f"{uuid.uuid4().hex}.{ext}"
        user_dir = self._get_user_dir(user_id)
        file_path = os.path.join(user_dir, stored_filename)
        relative_path = os.path.join('uploads', user_id, stored_filename)

        file.save(file_path)

        thumbnail_data = self._generate_thumbnail(file_path, ext)

        user_file = UserFile(
            user_id=user_id,
            original_filename=original_filename,
            stored_filename=stored_filename,
            file_type=file.content_type or f"application/{ext}",
            file_extension=ext,
            file_size=size,
            label=label,
            thumbnail_data=thumbnail_data,
            storage_path=relative_path,
            folder_id=folder_id,
        )
        self.db.session.add(user_file)
        self.db.session.commit()
        return user_file

    def list_files(self, folder_id=None):
        user_id = g.get('firebase_id')
        if not user_id:
            raise ValueError("User ID is required")
        return self.db.session.query(UserFile).filter_by(
            user_id=user_id, folder_id=folder_id
        ).order_by(UserFile.created_at.desc()).all()

    def move_file(self, file_id, folder_id):
        user_id = g.get('firebase_id')
        if not user_id:
            raise ValueError("User ID is required")
        user_file = self.db.session.query(UserFile).filter_by(
            id=file_id, user_id=user_id
        ).first()
        if not user_file:
            raise ValueError("File not found")
        if folder_id:
            folder = self.db.session.query(UserFolder).filter_by(
                id=folder_id, user_id=user_id
            ).first()
            if not folder:
                raise ValueError("Folder not found")
        user_file.folder_id = folder_id
        self.db.session.commit()
        return user_file

    # Folder operations

    def create_folder(self, name, parent_folder_id=None):
        user_id = g.get('firebase_id')
        if not user_id:
            raise ValueError("User ID is required")
        if not name or not name.strip():
            raise ValueError("Folder name is required")
        if parent_folder_id:
            parent = self.db.session.query(UserFolder).filter_by(
                id=parent_folder_id, user_id=user_id
            ).first()
            if not parent:
                raise ValueError("Parent folder not found")
        folder = UserFolder(
            user_id=user_id,
            name=name.strip(),
            parent_folder_id=parent_folder_id,
        )
        self.db.session.add(folder)
        self.db.session.commit()
        return folder

    def list_folders(self, parent_folder_id=None):
        user_id = g.get('firebase_id')
        if not user_id:
            raise ValueError("User ID is required")
        return self.db.session.query(UserFolder).filter_by(
            user_id=user_id, parent_folder_id=parent_folder_id
        ).order_by(UserFolder.name).all()

    def rename_folder(self, folder_id, name):
        user_id = g.get('firebase_id')
        if not user_id:
            raise ValueError("User ID is required")
        folder = self.db.session.query(UserFolder).filter_by(
            id=folder_id, user_id=user_id
        ).first()
        if not folder:
            raise ValueError("Folder not found")
        folder.name = name.strip()
        self.db.session.commit()
        return folder

    def delete_folder(self, folder_id):
        user_id = g.get('firebase_id')
        if not user_id:
            raise ValueError("User ID is required")
        folder = self.db.session.query(UserFolder).filter_by(
            id=folder_id, user_id=user_id
        ).first()
        if not folder:
            raise ValueError("Folder not found")
        # Move contained files to parent folder
        self.db.session.query(UserFile).filter_by(
            user_id=user_id, folder_id=folder_id
        ).update({'folder_id': folder.parent_folder_id})
        # Move contained subfolders to parent folder
        self.db.session.query(UserFolder).filter_by(
            user_id=user_id, parent_folder_id=folder_id
        ).update({'parent_folder_id': folder.parent_folder_id})
        self.db.session.delete(folder)
        self.db.session.commit()
        return True

    def get_file(self, file_id):
        user_id = g.get('firebase_id')
        if not user_id:
            raise ValueError("User ID is required")
        user_file = self.db.session.query(UserFile).filter_by(
            id=file_id, user_id=user_id
        ).first()
        if not user_file:
            raise ValueError("File not found")
        full_path = os.path.join(os.getcwd(), user_file.storage_path)
        if not os.path.exists(full_path):
            raise ValueError("File not found on disk")
        return user_file, full_path

    def delete_file(self, file_id):
        user_id = g.get('firebase_id')
        if not user_id:
            raise ValueError("User ID is required")
        user_file = self.db.session.query(UserFile).filter_by(
            id=file_id, user_id=user_id
        ).first()
        if not user_file:
            raise ValueError("File not found")
        full_path = os.path.join(os.getcwd(), user_file.storage_path)
        if os.path.exists(full_path):
            os.remove(full_path)
        self.db.session.delete(user_file)
        self.db.session.commit()
        return True

    def update_label(self, file_id, label):
        user_id = g.get('firebase_id')
        if not user_id:
            raise ValueError("User ID is required")
        user_file = self.db.session.query(UserFile).filter_by(
            id=file_id, user_id=user_id
        ).first()
        if not user_file:
            raise ValueError("File not found")
        user_file.label = label
        self.db.session.commit()
        return user_file
