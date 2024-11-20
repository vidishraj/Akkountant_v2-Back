from utils.GoogleServiceSingleton import GoogleServiceSingleton
from io import BytesIO
from flask import send_file
from googleapiclient.http import MediaFileUpload

from googleapiclient.errors import HttpError


class GdriveServiceUtils:

    def __init__(self):
        self.googleService = GoogleServiceSingleton()

    def downloadFile(self, file_id, userId, token):
        driveService = self.googleService.get_drive_service(userId, token)
        file_metadata = driveService.files().get(fileId=file_id).execute()
        request = driveService.files().get_media(fileId=file_id)
        file_content = request.execute()

        return send_file(
            BytesIO(file_content),
            as_attachment=True,
            download_name=file_metadata['name']
        )

    def renameFile(self, file_id, new_name, userId, token):
        driveService = self.googleService.get_drive_service(userId, token)
        file = driveService.files().get(fileId=file_id).execute()
        file['name'] = new_name
        driveService.files().update(fileId=file_id, body={'name': new_name}).execute()

    def deleteFile(self, file_id, userId, token):
        driveService = self.googleService.get_drive_service(userId, token)
        driveService.files().delete(fileId=file_id).execute()

    @staticmethod
    def getOrCreateFolder(drive_service, folder_name, parent_id=None):
        # Check if the folder exists in the specified parent folder
        query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
        if parent_id:
            query += f" and '{parent_id}' in parents"

        results = drive_service.files().list(q=query, spaces='drive').execute()
        folders = results.get('files', [])

        # Return the existing folder ID if found
        if folders:
            return folders[0]['id']

        # Otherwise, create a new folder
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_id] if parent_id else []
        }
        folder = drive_service.files().create(body=folder_metadata, fields='id').execute()
        return folder['id']

    def getFolderIdByPath(self, drive_service, path):
        # Split the path and navigate or create each folder
        folders = path.strip("/").split("/")
        parent_id = None
        for folder_name in folders:
            parent_id = self.getOrCreateFolder(drive_service, folder_name, parent_id)
        return parent_id

    def uploadFileToDrive(self, fileName: str, parentFolderPath: str, userId, token, filePath):
        # Initialize Google Drive service
        drive_service = self.googleService.get_drive_service(userId, token)

        # Get the ID of the final folder in the path
        parent_folder_id = self.getFolderIdByPath(drive_service, parentFolderPath)

        # Set up file metadata and upload
        file_metadata = {'name': fileName, 'parents': [parent_folder_id]}
        media = MediaFileUpload(filePath, mimetype='text/plain')

        try:
            uploaded_file = drive_service.files().create(body=file_metadata, media_body=media).execute()
            return uploaded_file["id"]
        except HttpError:
            return None

    def checkStatus(self, token):
        return self.googleService.is_token_valid(token)