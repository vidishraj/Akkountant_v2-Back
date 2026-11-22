import os
import uuid
import base64
from datetime import datetime, timedelta
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

ALLOWED_EXTENSIONS = {'pdf', 'png', 'jpg', 'jpeg', 'gif', 'bmp', 'webp', 'zip'}
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
THUMBNAIL_SIZE = (200, 200)

# ak-kxdr: soft-delete retention — hard-delete fires on files whose
# deleted_at is strictly OLDER than this window. Matches brief 07's
# "30-day restore" semantic. VaultTrashCleanupTask reads this
# constant; service-layer trash list reads it for days_remaining.
VAULT_RETENTION_DAYS = 30


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
        """List LIVE files only (soft-deleted files excluded).

        ak-kxdr: live-file surface filters ``deleted_at IS NULL``.
        Trashed files surface via ``list_trash()`` under a separate
        endpoint — keeps the main grid uncluttered.
        """
        user_id = g.get('firebase_id')
        if not user_id:
            raise ValueError("User ID is required")
        return self.db.session.query(UserFile).filter(
            UserFile.user_id == user_id,
            UserFile.folder_id == folder_id,
            UserFile.deleted_at.is_(None),  # ak-kxdr: live only
        ).order_by(UserFile.created_at.desc()).all()

    def move_file(self, file_id, folder_id):
        """Move a file to another folder.

        ak-kxdr: refuses to move a soft-deleted file (filter
        includes ``deleted_at IS NULL``) — trashed files cannot
        be edited; restore first.
        """
        user_id = g.get('firebase_id')
        if not user_id:
            raise ValueError("User ID is required")
        user_file = self.db.session.query(UserFile).filter(
            UserFile.id == file_id,
            UserFile.user_id == user_id,
            UserFile.deleted_at.is_(None),  # ak-kxdr: not trashed
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
        """Get a LIVE file for download / view.

        ak-kxdr: excludes soft-deleted files — a trashed file is
        NOT downloadable. Restore first to re-enable download.
        """
        user_id = g.get('firebase_id')
        if not user_id:
            raise ValueError("User ID is required")
        user_file = self.db.session.query(UserFile).filter(
            UserFile.id == file_id,
            UserFile.user_id == user_id,
            UserFile.deleted_at.is_(None),  # ak-kxdr: not trashed
        ).first()
        if not user_file:
            raise ValueError("File not found")
        full_path = os.path.join(os.getcwd(), user_file.storage_path)
        if not os.path.exists(full_path):
            raise ValueError("File not found on disk")
        return user_file, full_path

    def delete_file(self, file_id):
        """ak-kxdr: SOFT-DELETE. Marks ``deleted_at = utcnow()``;
        row stays in DB and disk file is preserved until either:
          (a) VaultTrashCleanupTask hard-DROPs after 30 days, or
          (b) operator explicitly empties from /files/trash/<id>
              via ``hard_delete_file`` with the confirm-id gate.

        Behavior change from the pre-ak-kxdr hard-DROP. Rationale:
        identity docs in the vault deserve a restore window — brief
        07 §5 "accidental delete is catastrophic". Restore via
        ``restore_file``.

        Idempotent: re-deleting an already-trashed file refreshes
        ``deleted_at`` to now (extends the restore window by one
        operator action — matches user expectation of "click
        delete, get 30 days").
        """
        user_id = g.get('firebase_id')
        if not user_id:
            raise ValueError("User ID is required")
        user_file = self.db.session.query(UserFile).filter_by(
            id=file_id, user_id=user_id
        ).first()
        if not user_file:
            raise ValueError("File not found")
        # Soft-delete: set deleted_at, DO NOT remove disk file, DO
        # NOT drop the row. Both are required for restore. The
        # cleanup task owns the eventual hard-delete.
        user_file.deleted_at = datetime.utcnow()
        self.db.session.commit()
        return True

    def update_label(self, file_id, label):
        """ak-kxdr: refuses to update a trashed file's label —
        trashed files are read-only. Restore first.
        """
        user_id = g.get('firebase_id')
        if not user_id:
            raise ValueError("User ID is required")
        user_file = self.db.session.query(UserFile).filter(
            UserFile.id == file_id,
            UserFile.user_id == user_id,
            UserFile.deleted_at.is_(None),  # ak-kxdr: not trashed
        ).first()
        if not user_file:
            raise ValueError("File not found")
        user_file.label = label
        self.db.session.commit()
        return user_file

    # ─── ak-kxdr: trash surface ────────────────────────────────

    def list_trash(self):
        """Return all soft-deleted files for the user, newest-
        trashed first. Each row has ``days_remaining`` =
        ``VAULT_RETENTION_DAYS - (now - deleted_at).days`` clamped
        to [0, VAULT_RETENTION_DAYS].

        Returns a list of ``(user_file, days_remaining)`` tuples
        so the controller can format days_remaining into the
        response alongside the normal file shape.
        """
        user_id = g.get('firebase_id')
        if not user_id:
            raise ValueError("User ID is required")
        rows = self.db.session.query(UserFile).filter(
            UserFile.user_id == user_id,
            UserFile.deleted_at.isnot(None),
        ).order_by(UserFile.deleted_at.desc()).all()
        now = datetime.utcnow()
        out = []
        for row in rows:
            elapsed_days = (now - row.deleted_at).days
            days_remaining = VAULT_RETENTION_DAYS - elapsed_days
            if days_remaining < 0:
                days_remaining = 0
            elif days_remaining > VAULT_RETENTION_DAYS:
                days_remaining = VAULT_RETENTION_DAYS
            out.append((row, days_remaining))
        return out

    def restore_file(self, file_id):
        """Clear ``deleted_at`` — file re-appears in main grid.

        ak-kxdr: user_id-scoped. 404 (ValueError) on BOTH not-
        found AND wrong-user (same shape — IDOR-leak guard from
        ak-05af; must NOT leak existence across users).

        Idempotent: restoring an already-restored file is a
        no-op that returns the row unchanged.
        """
        user_id = g.get('firebase_id')
        if not user_id:
            raise ValueError("User ID is required")
        user_file = self.db.session.query(UserFile).filter_by(
            id=file_id, user_id=user_id
        ).first()
        if not user_file:
            raise ValueError("File not found")
        user_file.deleted_at = None
        self.db.session.commit()
        return user_file

    def hard_delete_file(self, file_id, confirm_id):
        """§23 #5 destructive-gate HARD-DELETE from trash.

        Preconditions (all required):
          * ``confirm_id`` matches ``file_id`` EXACTLY — the
            destructive-gate pattern. The operator must echo
            the file id to prove intent; stray / automated
            DELETE calls that don't carry it get a 400. Pattern
            precedent: GitHub's "type the repo name to delete".
          * File is user-scoped (user_id match).
          * File IS in the trash (``deleted_at IS NOT NULL``).
            A live file cannot be hard-deleted via this path —
            FE must soft-delete it first. This prevents a
            hostile client from bypassing the trash window.

        ValueError raised on any precondition miss. Controller
        maps `confirm_id` misses to 400 and the rest to 404.
        """
        user_id = g.get('firebase_id')
        if not user_id:
            raise ValueError("User ID is required")
        # Destructive-gate: must echo the file id back. Explicit
        # exact-match (not "truthy check") so {"confirm_id": true}
        # or {"confirm_id": "yes"} can't bypass.
        if not isinstance(confirm_id, str) or confirm_id != file_id:
            raise ValueError(
                "confirm_id mismatch — hard-delete requires echoing "
                "the file_id back as confirm_id (destructive-gate)"
            )
        user_file = self.db.session.query(UserFile).filter(
            UserFile.id == file_id,
            UserFile.user_id == user_id,
            # Must already be in the trash. Live-file hard-delete
            # would bypass the soft-delete restore window.
            UserFile.deleted_at.isnot(None),
        ).first()
        if not user_file:
            raise ValueError("File not found")
        full_path = os.path.join(os.getcwd(), user_file.storage_path)
        if os.path.exists(full_path):
            os.remove(full_path)
        self.db.session.delete(user_file)
        self.db.session.commit()
        return True

    def cleanup_expired_trash(self, retention_days=VAULT_RETENTION_DAYS):
        """VaultTrashCleanupTask entrypoint: hard-DROP every file
        whose ``deleted_at < NOW - retention_days``.

        Returns an int: count of rows hard-deleted. Caller logs the
        count + user_ids; MUST NOT log filenames or file contents
        per bead §23 #4 (receipt data is personal financial data —
        the vault often holds identity docs).

        Not user-scoped — called from a background task sweeping
        all users. Sees every user's expired trash.

        Disk cleanup: removes the backing file on disk before the
        row DROP. Missing disk file is NOT an error (prior failed
        run may have cleaned disk but not row, or vice versa);
        logged WARN and the DB row is still dropped so subsequent
        sweeps don't keep rediscovering the orphan.
        """
        threshold = datetime.utcnow() - timedelta(days=retention_days)
        expired = self.db.session.query(UserFile).filter(
            UserFile.deleted_at.isnot(None),
            UserFile.deleted_at < threshold,
        ).all()
        dropped = 0
        for row in expired:
            full_path = os.path.join(os.getcwd(), row.storage_path)
            try:
                if os.path.exists(full_path):
                    os.remove(full_path)
            except OSError as exc:
                # Log but continue — keep sweep deterministic. Per
                # §23 #4, don't echo the filename into logs; the
                # user_id + file_id are enough to diagnose.
                self.logger.warning(
                    f"Trash cleanup: disk unlink failed for "
                    f"user_id={row.user_id} file_id={row.id}: "
                    f"{type(exc).__name__}"
                )
            self.db.session.delete(row)
            dropped += 1
        if dropped > 0:
            self.db.session.commit()
        return dropped
