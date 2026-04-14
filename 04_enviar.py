"""
04_enviar.py — Doblen Solutions x Farmacias del Pueblo
1. Sube el Excel de conciliación más reciente a Google Drive
2. Manda mail a Joaco y Fran con el archivo adjunto

Requiere variables de entorno:
    GOOGLE_CREDENTIALS_FILE  → ruta al JSON de la Service Account (default: credentials.json)
    DRIVE_CONCILIACION_ID    → ID de la carpeta de Drive donde subir
    GMAIL_USER               → cuenta que manda el mail (2blen.solutions@gmail.com)
    GMAIL_APP_PASSWORD       → App Password de Gmail (no la contraseña normal)
    MAIL_DESTINATARIOS       → emails separados por coma (ej: joaco@...,fran@...)

Correr con: python 04_enviar.py
"""

import os
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_DIR             = os.path.dirname(os.path.abspath(__file__))
CONCILIACION_DIR     = os.path.join(BASE_DIR, "4_conciliacion")

CREDENTIALS_FILE     = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
DRIVE_CONCILIACION_ID = os.getenv("DRIVE_CONCILIACION_ID")

GMAIL_USER           = os.getenv("GMAIL_USER",          "2blen.solutions@gmail.com")
GMAIL_APP_PASSWORD   = os.getenv("GMAIL_APP_PASSWORD",  "")
MAIL_DESTINATARIOS   = os.getenv("MAIL_DESTINATARIOS")


# ---------------------------------------------------------------------------
# Buscar el archivo de conciliación más reciente
# ---------------------------------------------------------------------------
def buscar_conciliacion() -> str:
    archivos = sorted(
        [f for f in os.listdir(CONCILIACION_DIR)
         if f.startswith("conciliacion_") and f.endswith(".xlsx")],
        reverse=True
    )
    if not archivos:
        raise FileNotFoundError(
            f"No se encontró ningún archivo de conciliación en {CONCILIACION_DIR}\n"
            f"Corré primero 03_conciliar.py"
        )
    return os.path.join(CONCILIACION_DIR, archivos[0])


# ---------------------------------------------------------------------------
# Subir a Google Drive
# ---------------------------------------------------------------------------
def subir_a_drive(filepath: str) -> str:
    """Sube el archivo a Drive y devuelve el link."""
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload

    creds = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE,
        scopes=["https://www.googleapis.com/auth/drive.file"],
    )
    service  = build("drive", "v3", credentials=creds)
    nombre   = os.path.basename(filepath)

    # Verificar si ya existe un archivo con ese nombre y reemplazarlo
    query   = f"name='{nombre}' and '{DRIVE_CONCILIACION_ID}' in parents and trashed=false"
    existentes = service.files().list(q=query, fields="files(id, name)").execute().get("files", [])

    media = MediaFileUpload(
        filepath,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        resumable=True,
    )

    if existentes:
        # Actualizar el existente
        file_id = existentes[0]["id"]
        service.files().update(fileId=file_id, media_body=media).execute()
        print(f"  Drive: actualizado {nombre} (id: {file_id})")
    else:
        # Crear nuevo
        metadata = {"name": nombre, "parents": [DRIVE_CONCILIACION_ID]}
        resultado = service.files().create(body=metadata, media_body=media, fields="id").execute()
        file_id  = resultado["id"]
        print(f"  Drive: subido {nombre} (id: {file_id})")

    return f"https://drive.google.com/file/d/{file_id}/view"


# ---------------------------------------------------------------------------
# Mandar mail con adjunto
# ---------------------------------------------------------------------------
def mandar_mail(filepath: str, drive_link: str):
    nombre_archivo = os.path.basename(filepath)

    # Extraer fecha del nombre (conciliacion_YYYYMMDD.xlsx)
    try:
        slug = nombre_archivo.replace("conciliacion_", "").replace(".xlsx", "")
        fecha_dt = datetime.strptime(slug, "%Y%m%d")
        fecha_legible = fecha_dt.strftime("%d/%m/%Y")
    except ValueError:
        fecha_legible = slug

    destinatarios = [m.strip() for m in MAIL_DESTINATARIOS.split(",") if m.strip()]

    msg = MIMEMultipart()
    msg["From"]    = GMAIL_USER
    msg["To"]      = ", ".join(destinatarios)
    msg["Subject"] = f"Conciliación Payway × Zetti — {fecha_legible}"

    cuerpo = f"""Hola,

Se adjunta el reporte de conciliación Payway × Zetti correspondiente al {fecha_legible}.

También disponible en Drive:
{drive_link}

—
Doblen Solutions
(mail automático)
"""
    msg.attach(MIMEText(cuerpo, "plain"))

    # Adjunto
    with open(filepath, "rb") as f:
        adjunto = MIMEBase("application", "octet-stream")
        adjunto.set_payload(f.read())
    encoders.encode_base64(adjunto)
    adjunto.add_header("Content-Disposition", f'attachment; filename="{nombre_archivo}"')
    msg.attach(adjunto)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_USER, destinatarios, msg.as_string())

    print(f"  Mail enviado a: {', '.join(destinatarios)}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("  04 — Enviar conciliación")
    print("  Doblen Solutions x Farmacias del Pueblo")
    print("=" * 60)

    print("\nBuscando archivo de conciliación...")
    filepath = buscar_conciliacion()
    print(f"  {os.path.basename(filepath)}")

    print("\nSubiendo a Google Drive...")
    drive_link = subir_a_drive(filepath)
    print(f"  {drive_link}")

    print("\nMandando mail...")
    mandar_mail(filepath, drive_link)

    print("\n✓ Listo")
    print("=" * 60)


if __name__ == "__main__":
    main()
