from flask import request, jsonify, send_file
from services.fileStorageService import FileStorageService
from utils.logger import Logger


class FileStorageController:
    def __init__(self, file_storage_service):
        self.file_storage_service = file_storage_service
        self.logger = Logger(__name__).get_logger()

    @staticmethod
    def _format_file(user_file, include_thumbnail=True, days_remaining=None):
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
            # ak-kxdr: deleted_at is always present on the wire;
            # NULL for live files, ISO-8601 for trashed. FE's
            # Recently-deleted toggle filters on this field.
            "deleted_at": (
                user_file.deleted_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                if user_file.deleted_at else None
            ),
        }
        # ak-kxdr: days_remaining ONLY on trash responses — live
        # files don't carry a countdown. FE uses presence/absence
        # as the signal for "Days remaining" badge rendering.
        if days_remaining is not None:
            result["days_remaining"] = days_remaining
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
        """ak-kxdr: SOFT-DELETE. Response message updated to
        communicate the new behavior — file moved to trash, not
        gone forever.
        """
        try:
            self.file_storage_service.delete_file(fileId)
            return jsonify({
                "message": "File moved to trash",
                # FE uses this to render the "Restore" affordance on
                # the just-deleted-file toast without re-polling.
                "trashed": True,
            }), 200
        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error deleting file: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    # ─── ak-kxdr: trash handlers ───────────────────────────────

    @Logger.standardLogger
    def list_trash(self):
        """GET /files/trash — list soft-deleted files with
        ``days_remaining`` counter per row (newest-trashed first).
        """
        try:
            trashed = self.file_storage_service.list_trash()
            return jsonify({
                "files": [
                    self._format_file(f, days_remaining=days)
                    for f, days in trashed
                ],
                "count": len(trashed),
            }), 200
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            self.logger.error(f"Error listing trash: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def restore_file(self, fileId):
        """POST /files/trash/<fileId>/restore — clear
        ``deleted_at`` so the file returns to the main grid.

        user_id-scoped: 404 on wrong-user id (NOT 403) — same
        IDOR-leak shape as ak-05af watchlist delete (must not
        enumerate ids across users). Idempotent: restoring an
        already-live file is a no-op 200.
        """
        try:
            user_file = self.file_storage_service.restore_file(fileId)
            return jsonify({"file": self._format_file(user_file)}), 200
        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error restoring file: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def hard_delete_file(self, fileId):
        """DELETE /files/trash/<fileId> — §23 #5 destructive-gate
        HARD-DELETE from the trash. Body MUST carry::

            {"confirm_id": "<fileId>"}

        where confirm_id EXACTLY matches the URL fileId. This is
        the "type-the-id-to-confirm" pattern (precedent: GitHub's
        "type the repo name to delete"). Stray / automated DELETE
        calls that don't echo the id get a 400.

        Preconditions (all required):
          * File is in the trash (``deleted_at IS NOT NULL``) —
            404 if it's live; must soft-delete first via DELETE
            /files/<id>.
          * File is user-scoped (404 on wrong-user, not 403 —
            IDOR-leak guard).
          * Body carries ``confirm_id == fileId``.
        """
        try:
            # Body is REQUIRED — a bare DELETE without the
            # confirm-id envelope must 400 before touching
            # anything. Even an empty body gets a 400 (vs 404
            # or 500) so the FE sees a deterministic
            # destructive-gate error shape.
            if not request.is_json:
                return jsonify({
                    "error": (
                        "Request body must be application/json and "
                        "carry confirm_id (destructive-gate)"
                    ),
                }), 400
            data = request.get_json(silent=True) or {}
            if not isinstance(data, dict):
                return jsonify({
                    "error": "Request body must be a JSON object",
                }), 400
            confirm_id = data.get("confirm_id")
            if not confirm_id:
                return jsonify({
                    "error": (
                        "confirm_id is required and must equal the "
                        "URL fileId (destructive-gate)"
                    ),
                }), 400
            try:
                self.file_storage_service.hard_delete_file(
                    fileId, confirm_id,
                )
            except ValueError as e:
                # Split: confirm_id mismatch → 400 (destructive-
                # gate failure); everything else → 404 (not found
                # OR wrong-user — same shape, no enumeration).
                if "confirm_id" in str(e):
                    return jsonify({"error": str(e)}), 400
                return jsonify({"error": str(e)}), 404
            return jsonify({
                "message": "File permanently deleted",
                "id": fileId,
            }), 200
        except Exception as e:
            self.logger.error(f"Error hard-deleting file: {str(e)}")
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
