from sqlalchemy import Column, String, Text, Integer, DateTime, Index, ForeignKey, func
from sqlalchemy.dialects.mysql import CHAR
from .Base import Base
import uuid


def generate_uuid():
    return str(uuid.uuid4())


class UserFolder(Base):
    __tablename__ = 'user_folders'

    id = Column(CHAR(36), primary_key=True, default=generate_uuid)
    user_id = Column(CHAR(36), ForeignKey('users.userID', ondelete='CASCADE'), nullable=False)
    name = Column(String(255), nullable=False)
    parent_folder_id = Column(CHAR(36), ForeignKey('user_folders.id', ondelete='SET NULL'), nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index('idx_ufolder_user_id', 'user_id'),
        Index('idx_ufolder_parent', 'parent_folder_id'),
    )


class UserFile(Base):
    __tablename__ = 'user_files'

    id = Column(CHAR(36), primary_key=True, default=generate_uuid)
    user_id = Column(CHAR(36), ForeignKey('users.userID', ondelete='CASCADE'), nullable=False)
    original_filename = Column(String(500), nullable=False)
    stored_filename = Column(String(300), nullable=False)
    file_type = Column(String(50), nullable=False)
    file_extension = Column(String(10), nullable=False)
    file_size = Column(Integer, nullable=False)
    label = Column(String(255), nullable=True)
    thumbnail_data = Column(Text, nullable=True)
    storage_path = Column(String(500), nullable=False)
    folder_id = Column(CHAR(36), ForeignKey('user_folders.id', ondelete='SET NULL'), nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    # ak-kxdr (2026-11-22): soft-delete timestamp. NULL = live file;
    # non-NULL = in the 30-day trash window pending hard-delete via
    # VaultTrashCleanupTask or operator-driven /files/trash/<id> with
    # the confirm-id destructive gate.
    #
    # Nullable so existing rows stay valid post-ALTER (defaults to
    # NULL = live). Schema addition requires an INFRA ALTER on an
    # EXISTING table — db.create_all() does NOT ALTER existing
    # tables (ak-cipj precedent, NOT ak-v55t SAFE #9). Coordinate
    # with akkountant_infra for the ``ALTER TABLE user_files ADD
    # COLUMN deleted_at DATETIME NULL`` before deploy.
    deleted_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index('idx_uf_user_id', 'user_id'),
        Index('idx_uf_folder_id', 'folder_id'),
        # ak-kxdr: covers the two primary query shapes:
        #   * Live-file list: WHERE user_id = ? AND deleted_at IS NULL
        #   * Trash list:     WHERE user_id = ? AND deleted_at IS NOT NULL
        #                        ORDER BY deleted_at DESC
        # + the cleanup-task scan: WHERE deleted_at < NOW - 30 DAYS.
        # MySQL's BTREE index supports IS NULL / IS NOT NULL / <
        # predicates on the second column when user_id (first column)
        # is also constrained OR when used as a range scan on the
        # second column alone.
        Index('idx_uf_user_deleted_at', 'user_id', 'deleted_at'),
    )
