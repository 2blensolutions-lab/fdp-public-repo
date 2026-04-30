"""
utils/drive.py — Wrapper Google Drive API con OAuth2
Doblen Solutions x Farmacias del Pueblo

Usa el token OAuth2 de la cuenta 2blen.solutions@gmail.com.
El token se guarda en Railway como GOOGLE_TOKEN_B64 (base64 del token.json).

IMPORTANTE: Para que el token no venza, la app tiene que estar publicada
en Google Cloud Console → APIs & Services → OAuth consent screen → Publish App.

Variable de entorno requerida:
    GOOGLE_TOKEN_B64     — contenido de token.json en base64 (generado con generar_token.py)
    DRIVE_ROOT_FOLDER_ID — ID de la carpeta raíz en Drive
"""

import os
import base64
import json

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/gmail.send",
]

DRIVE_ROOT_FOLDER_ID = os.getenv("DRIVE_ROOT_FOLDER_ID", "1aTcnp_pD7mBCGUV5UglRfeu9NjcJ8z5y")


def _build_credentials() -> Credentials:
    token_b64 = os.environ.get("GOOGLE_TOKEN_B64")
    if not token_b64:
        raise EnvironmentError(
            "Variable de entorno GOOGLE_TOKEN_B64 no encontrada. "
            "Correr generar_token.py localmente y cargar el resultado en Railway."
        )
    token_data = json.loads(base64.b64decode(token_b64))
    creds = Credentials.from_authorized_user_info(token_data, SCOPES)

    # Refrescar si venció
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    return creds


def _build_service():
    return build("drive", "v3", credentials=_build_credentials())


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
            .list(q=query, fields="files(id, name)", spaces="drive")
            .execute()
        )
        files = results.get("files", [])
        if files:
            return files[0]["id"]

        metadata = {
            "name":     name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents":  [parent_id],
        }
        folder = self.service.files().create(body=metadata, fields="id").execute()
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
                .update(fileId=existing_id, media_body=media, fields="id")
                .execute()
            )
        else:
            metadata = {"name": filename, "parents": [folder_id]}
            file = (
                self.service.files()
                .create(body=metadata, media_body=media, fields="id")
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
            .list(q=query, fields="files(id, name)", spaces="drive")
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
