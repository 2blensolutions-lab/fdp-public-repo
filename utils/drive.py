"""
utils/drive.py — Wrapper Google Drive API
Doblen Solutions x Farmacias del Pueblo

Usa service account cuyo JSON viene en la variable de entorno
GOOGLE_CREDENTIALS_B64 (el JSON del service account en base64).

La carpeta raíz en Drive viene de DRIVE_ROOT_FOLDER_ID.
Dentro de esa carpeta, cada run genera una subcarpeta YYYYMMDD/.

Uso:
    from utils.drive import DriveClient
    drive = DriveClient()
    folder_id = drive.get_or_create_run_folder("20260403")
    file_id   = drive.upload_file("mi_archivo.csv", "/tmp/mi_archivo.csv", folder_id)
"""

import os
import base64
import json

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


SCOPES = ["https://www.googleapis.com/auth/drive"]

# ID de la carpeta raíz en Drive ("Farmacias del Pueblo")
# https://drive.google.com/drive/folders/1aTcnp_pD7mBCGUV5UglRfeu9NjcJ8z5y
DRIVE_ROOT_FOLDER_ID = os.getenv("DRIVE_ROOT_FOLDER_ID", "1aTcnp_pD7mBCGUV5UglRfeu9NjcJ8z5y")


def _build_service():
    """Construye el cliente de Drive desde la variable de entorno."""
    creds_b64 = os.environ.get("GOOGLE_CREDENTIALS_B64")
    if not creds_b64:
        raise EnvironmentError(
            "Variable de entorno GOOGLE_CREDENTIALS_B64 no encontrada. "
            "Encodear el JSON de la service account en base64 y cargarla en Railway."
        )
    creds_json = json.loads(base64.b64decode(creds_b64))
    creds = service_account.Credentials.from_service_account_info(creds_json, scopes=SCOPES)
    return build("drive", "v3", credentials=creds)


class DriveClient:

    def __init__(self):
        self.service = _build_service()

    # ------------------------------------------------------------------
    # Carpetas
    # ------------------------------------------------------------------

    def get_or_create_folder(self, name: str, parent_id: str) -> str:
        """
        Devuelve el ID de una carpeta con ese nombre dentro de parent_id.
        Si no existe, la crea.
        """
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

        # No existe → crear
        metadata = {
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }
        folder = self.service.files().create(body=metadata, fields="id").execute()
        return folder["id"]

    def get_or_create_run_folder(self, fecha: str) -> str:
        """
        Devuelve el ID de la carpeta runs/YYYYMMDD dentro de la raíz.
        La crea si no existe.
        fecha: string en formato YYYYMMDD, ej: "20260403"
        """
        runs_id = self.get_or_create_folder("runs", DRIVE_ROOT_FOLDER_ID)
        return self.get_or_create_folder(fecha, runs_id)

    # ------------------------------------------------------------------
    # Archivos
    # ------------------------------------------------------------------

    def upload_file(self, filename: str, local_path: str, folder_id: str) -> str:
        """
        Sube un archivo a Drive dentro de folder_id.
        Si ya existe un archivo con ese nombre en esa carpeta, lo reemplaza.
        Devuelve el file_id del archivo subido.

        filename:   nombre que va a tener en Drive
        local_path: ruta local del archivo a subir
        folder_id:  ID de la carpeta destino en Drive
        """
        # Detectar MIME type por extensión
        mime_map = {
            ".csv":  "text/csv",
            ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ".log":  "text/plain",
            ".txt":  "text/plain",
        }
        ext = os.path.splitext(filename)[1].lower()
        mime_type = mime_map.get(ext, "application/octet-stream")

        media = MediaFileUpload(local_path, mimetype=mime_type, resumable=True)

        # Buscar si ya existe para hacer update en vez de create
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
        """Devuelve el ID del archivo si existe en esa carpeta, sino None."""
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
        """
        Sube una lista de archivos al mismo folder.
        archivos: lista de (filename, local_path)
        Devuelve dict {filename: file_id}
        """
        resultado = {}
        for filename, local_path in archivos:
            if os.path.exists(local_path):
                file_id = self.upload_file(filename, local_path, folder_id)
                resultado[filename] = file_id
                print(f"  ✅ Drive: subido {filename}")
            else:
                print(f"  ⚠️  Drive: no encontrado localmente {local_path}, salteando")
        return resultado
