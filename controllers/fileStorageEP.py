from flask import request, jsonify, send_file
from services.fileStorageService import FileStorageService
from utils.logger import Logger


class FileStorageController:
    def __init__(self, file_storage_service):
        self.file_storage_service = file_storage_service
        self.logger = Logger(__name__).get_logger()

    @staticmethod
    def _format_file(user_file, include_thumbnail=True):
        result = {
            "id": user_file.id,
            "original_filename": user_file.original_filename,
            "file_type": user_file.file_type,
            "file_extension": user_file.file_extension,
            "file_size": user_file.file_size,
            "label": user_file.label,
            "folder_id": user_file.folder_id,
            "created_at": user_file.created_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if user_file.created_at else "",
            "updated_at": user_file.updated_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if user_file.updated_at else "",
        }
        if include_thumbnail:
            result["thumbnail"] = user_file.thumbnail_data
        return result

    @staticmethod
    def _format_folder(folder):
        return {
            "id": folder.id,
            "name": folder.name,
            "parent_folder_id": folder.parent_folder_id,
            "created_at": folder.created_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if folder.created_at else "",
            "updated_at": folder.updated_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if folder.updated_at else "",
        }

    @Logger.standardLogger
    def upload_file(self):
        try:
            if 'file' not in request.files:
                return jsonify({"error": "No file provided"}), 400
            file = request.files['file']
            label = request.form.get('label', None)
            folder_id = request.form.get('folder_id', None)
            user_file = self.file_storage_service.upload_file(file, label, folder_id)
            return jsonify({"file": self._format_file(user_file)}), 201
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            self.logger.error(f"Error uploading file: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def list_files(self):
        try:
            folder_id = request.args.get('folder_id', None)
            files = self.file_storage_service.list_files(folder_id)
            folders = self.file_storage_service.list_folders(folder_id)
            return jsonify({
                "files": [self._format_file(f) for f in files],
                "folders": [self._format_folder(fd) for fd in folders],
            }), 200
        except Exception as e:
            self.logger.error(f"Error listing files: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def download_file(self, fileId):
        try:
            user_file, full_path = self.file_storage_service.get_file(fileId)
            return send_file(
                full_path,
                as_attachment=True,
                download_name=user_file.original_filename,
                mimetype=user_file.file_type,
            )
        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error downloading file: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def view_file(self, fileId):
        try:
            user_file, full_path = self.file_storage_service.get_file(fileId)
            return send_file(
                full_path,
                as_attachment=False,
                download_name=user_file.original_filename,
                mimetype=user_file.file_type,
            )
        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error viewing file: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def delete_file(self, fileId):
        try:
            self.file_storage_service.delete_file(fileId)
            return jsonify({"message": "File deleted successfully"}), 200
        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error deleting file: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def update_label(self, fileId):
        try:
            data = request.get_json(force=True)
            label = data.get('label')
            user_file = self.file_storage_service.update_label(fileId, label)
            return jsonify({"file": self._format_file(user_file)}), 200
        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error updating label: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def move_file(self, fileId):
        try:
            data = request.get_json(force=True)
            folder_id = data.get('folder_id')
            user_file = self.file_storage_service.move_file(fileId, folder_id)
            return jsonify({"file": self._format_file(user_file)}), 200
        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error moving file: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    # Folder endpoints

    @Logger.standardLogger
    def create_folder(self):
        try:
            data = request.get_json(force=True)
            name = data.get('name')
            parent_folder_id = data.get('parent_folder_id')
            folder = self.file_storage_service.create_folder(name, parent_folder_id)
            return jsonify({"folder": self._format_folder(folder)}), 201
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            self.logger.error(f"Error creating folder: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def rename_folder(self, folderId):
        try:
            data = request.get_json(force=True)
            name = data.get('name')
            if not name:
                return jsonify({"error": "Name is required"}), 400
            folder = self.file_storage_service.rename_folder(folderId, name)
            return jsonify({"folder": self._format_folder(folder)}), 200
        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error renaming folder: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def delete_folder(self, folderId):
        try:
            self.file_storage_service.delete_folder(folderId)
            return jsonify({"message": "Folder deleted successfully"}), 200
        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error deleting folder: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500
