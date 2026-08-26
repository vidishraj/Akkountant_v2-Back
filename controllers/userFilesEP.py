"""ak-9dz: GET /api/user-files/<uuid> — download an agent-produced file.

Serves files written by the `attach_file_to_chat` MCP tool. Auth-scoped:
the row's user_id must match `g.firebase_id` set by the before_request
middleware; cross-user requests + missing uuids both return a generic
404 so existence never leaks (same pattern as
AgentConversationsController.get).

Streams the on-disk file with:
  * Content-Type: `mime_type` from the row (falls back to
    `application/octet-stream` if the row's mime is NULL — happens on
    unknown extensions from `mimetypes.guess_type`).
  * Content-Disposition: `attachment; filename=<display_name>` so the
    browser saves with the same name the user saw in the chat card,
    not the on-disk `{uuid}_{name}` shape.
  * Content-Length: from the row's cached `size_bytes` so the client
    can show download progress.
"""

from __future__ import annotations  # PEP 604 (`dict | None`) on 3.9

from urllib.parse import quote as _urlquote

from flask import g, jsonify, send_file

from services.agentFileAttachmentService import AgentFileAttachmentService
from utils.logger import Logger


class UserFilesController:
    def __init__(self, service: AgentFileAttachmentService):
        self.service = service
        self.logger = Logger(__name__).get_logger()

    @Logger.standardLogger
    def download(self, uuid: str):
        """GET /api/user-files/<uuid> — stream the file if auth-scoped.

        404 on: missing uuid, cross-user request, disk file missing,
        pre-migration deploy window (table missing → service returns
        None gracefully).
        """
        user_id = g.get("firebase_id")
        if not user_id:
            return jsonify({"error": "Unauthorized"}), 401
        if not uuid:
            return jsonify({"error": "File not found"}), 404

        pair = self.service.get_for_download(user_id, uuid)
        if pair is None:
            return jsonify({"error": "File not found"}), 404
        meta, disk_path = pair

        # Content-Disposition filename: preserve the user-facing
        # display_name. `quote` handles unicode + spaces safely; the
        # `filename*=UTF-8''...` extended form is RFC 5987 for browsers
        # that need explicit encoding. Standard `filename=` retained
        # as a fallback for older UAs.
        display_name = meta["display_name"]
        quoted = _urlquote(display_name, safe="")

        try:
            response = send_file(
                disk_path,
                mimetype=meta["mime_type"],
                as_attachment=True,
                download_name=display_name,
                conditional=True,
            )
        except Exception:
            self.logger.exception(
                "user_files: send_file failed for uuid=%s...", uuid[:8]
            )
            return jsonify({"error": "Internal server error"}), 500

        # Explicit Content-Disposition with both plain + RFC-5987
        # filename for unicode safety. Flask's send_file sets the
        # download_name via a plain filename attribute; overwrite to
        # add the extended form.
        response.headers["Content-Disposition"] = (
            f"attachment; filename=\"{display_name}\"; "
            f"filename*=UTF-8''{quoted}"
        )
        # Best-effort Content-Length from the DB-cached size. send_file
        # sets this from the file's on-disk size which matches, but
        # we mirror the row's cached value for consistency.
        response.headers.setdefault("Content-Length", str(meta["size_bytes"]))
        return response
