from sqlalchemy import Column, String, Text, Integer, DateTime, Index, func
from sqlalchemy.dialects.mysql import CHAR
from .Base import Base
import uuid


def generate_uuid():
    return str(uuid.uuid4())


class UserFolder(Base):
    __tablename__ = 'user_folders'

    id = Column(CHAR(36), primary_key=True, default=generate_uuid)
    user_id = Column(CHAR(36), nullable=False)
    name = Column(String(255), nullable=False)
    parent_folder_id = Column(CHAR(36), nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index('idx_ufolder_user_id', 'user_id'),
        Index('idx_ufolder_parent', 'parent_folder_id'),
    )


class UserFile(Base):
    __tablename__ = 'user_files'

    id = Column(CHAR(36), primary_key=True, default=generate_uuid)
    user_id = Column(CHAR(36), nullable=False)
    original_filename = Column(String(500), nullable=False)
    stored_filename = Column(String(300), nullable=False)
    file_type = Column(String(50), nullable=False)
    file_extension = Column(String(10), nullable=False)
    file_size = Column(Integer, nullable=False)
    label = Column(String(255), nullable=True)
    thumbnail_data = Column(Text, nullable=True)
    storage_path = Column(String(500), nullable=False)
    folder_id = Column(CHAR(36), nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index('idx_uf_user_id', 'user_id'),
        Index('idx_uf_folder_id', 'folder_id'),
    )
