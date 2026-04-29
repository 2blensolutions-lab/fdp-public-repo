"""
utils/drive.py — Wrapper Google Drive API con Service Account
Doblen Solutions x Farmacias del Pueblo

Usa service account cuyo JSON viene en GOOGLE_CREDENTIALS_B64 (base64).
La service account tiene que tener permisos de Editor en el Shared Drive.

Variables de entorno requeridas:
    GOOGLE_CREDENTIALS_B64   — JSON de service account en base64
    DRIVE_ROOT_FOLDER_ID     — ID de la carpeta raíz en Drive
"""

import os
import base64
import json

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


SCOPES               = ["https://www.googleapis.com/auth/drive"]
DRIVE_ROOT_FOLDER_ID = os.getenv("DRIVE_ROOT_FOLDER_ID", "1aTcnp_pD7mBCGUV5UglRfeu9NjcJ8z5y")


def _build_service():
    creds_b64  = os.environ.get("GOOGLE_CREDENTIALS_B64")
    if not creds_b64:
        raise EnvironmentError("Variable GOOGLE_CREDENTIALS_B64 no encontrada en Railway.")
    creds_json = json.loads(base64.b64decode(creds_b64))
    creds      = service_account.Credentials.from_service_account_info(creds_json, scopes=SCOPES)
    return build("drive", "v3", credentials=creds)


class DriveClient:

    def __init__(self):
        self.service = _build_service()

    def get_or_create_folder(self, name: str, parent_id: str) -> str:
        query = (
            f"name='{name}' "
            f"and mimeType='application/vnd.google-apps.folder' "
            f"and '{parent_id}' in parents "
            f"and trashed=false"
        )
        results = (
            self.service.files()
            .list(q=query, fields="files(id, name)", spaces="drive",
                  supportsAllDrives=True, includeItemsFromAllDrives=True)
            .execute()
        )
        files = results.get("files", [])
        if files:
            return files[0]["id"]

        metadata = {
            "name":    name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }
        folder = (
            self.service.files()
            .create(body=metadata, fields="id", supportsAllDrives=True)
            .execute()
        )
        return folder["id"]

    def get_or_create_run_folder(self, fecha: str) -> str:
        runs_id = self.get_or_create_folder("runs", DRIVE_ROOT_FOLDER_ID)
        return self.get_or_create_folder(fecha, runs_id)

    def upload_file(self, filename: str, local_path: str, folder_id: str) -> str:
        mime_map = {
            ".csv":  "text/csv",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".log":  "text/plain",
            ".txt":  "text/plain",
        }
        ext       = os.path.splitext(filename)[1].lower()
        mime_type = mime_map.get(ext, "application/octet-stream")
        media     = MediaFileUpload(local_path, mimetype=mime_type, resumable=True)

        existing_id = self._find_file(filename, folder_id)
        if existing_id:
            file = (
                self.service.files()
                .update(fileId=existing_id, media_body=media, fields="id",
                        supportsAllDrives=True)
                .execute()
            )
        else:
            metadata = {"name": filename, "parents": [folder_id]}
            file = (
                self.service.files()
                .create(body=metadata, media_body=media, fields="id",
                        supportsAllDrives=True)
                .execute()
            )
        return file["id"]

    def _find_file(self, filename: str, folder_id: str) -> str | None:
        query = (
            f"name='{filename}' "
            f"and '{folder_id}' in parents "
            f"and trashed=false"
        )
        results = (
            self.service.files()
            .list(q=query, fields="files(id, name)", spaces="drive",
                  supportsAllDrives=True, includeItemsFromAllDrives=True)
            .execute()
        )
        files = results.get("files", [])
        return files[0]["id"] if files else None

    def upload_many(self, archivos: list[tuple[str, str]], folder_id: str) -> dict:
        resultado = {}
        for filename, local_path in archivos:
            if os.path.exists(local_path):
                file_id = self.upload_file(filename, local_path, folder_id)
                resultado[filename] = file_id
                print(f"  ✅ Drive: subido {filename}")
            else:
                print(f"  ⚠️  Drive: no encontrado {local_path}, salteando")
        return resultado
