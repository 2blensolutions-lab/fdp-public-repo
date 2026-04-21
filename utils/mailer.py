"""
utils/mailer.py — Mails de éxito y error via Gmail API (OAuth2)
Doblen Solutions x Farmacias del Pueblo

Usa el mismo token OAuth2 que drive.py (GOOGLE_TOKEN_B64).
No requiere SMTP — usa la API de Gmail directamente.

Variables de entorno requeridas:
    GOOGLE_TOKEN_B64   — mismo token que para Drive
    MAIL_DESTINATARIOS — emails separados por coma para el mail de éxito
"""

import os
import base64
import json
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build


GMAIL_USER         = "2blen.solutions@gmail.com"
MAIL_DESTINATARIOS = os.getenv("MAIL_DESTINATARIOS", GMAIL_USER)

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/gmail.send",
]


def _build_gmail():
    token_b64  = os.environ.get("GOOGLE_TOKEN_B64")
    token_data = json.loads(base64.b64decode(token_b64))
    creds = Credentials.from_authorized_user_info(token_data, SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    return build("gmail", "v1", credentials=creds)


def _send(mensaje_bytes: bytes):
    service = _build_gmail()
    encoded = base64.urlsafe_b64encode(mensaje_bytes).decode("utf-8")
    service.users().messages().send(
        userId="me",
        body={"raw": encoded}
    ).execute()


def mail_exito(fecha: str, resumen: dict, adjunto_path: str | None = None):
    fecha_fmt      = f"{fecha[6:8]}/{fecha[4:6]}/{fecha[:4]}"
    destinatarios  = [d.strip() for d in MAIL_DESTINATARIOS.split(",") if d.strip()]

    msg = MIMEMultipart()
    msg["Subject"] = f"✅ Conciliación lista — {fecha_fmt}"
    msg["From"]    = GMAIL_USER
    msg["To"]      = ", ".join(destinatarios)

    cuerpo = f"""Conciliación del {fecha_fmt} generada correctamente.

Resumen del run:
  • Sucursales procesadas : {resumen.get('sucursales', '—')}
  • Filas Payway          : {resumen.get('filas_payway', '—')}
  • Cupones Zetti         : {resumen.get('cupones_zetti', '—')}
  • Filas conciliadas     : {resumen.get('filas_conciliacion', '—')}

Los archivos del día están en Drive > Farmacias del Pueblo > runs > {fecha}/

—
Doblen Solutions · Pipeline automático
"""
    msg.attach(MIMEText(cuerpo, "plain"))

    if adjunto_path and os.path.exists(adjunto_path):
        with open(adjunto_path, "rb") as f:
            parte = MIMEBase("application", "vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            parte.set_payload(f.read())
        encoders.encode_base64(parte)
        parte.add_header(
            "Content-Disposition",
            f"attachment; filename={os.path.basename(adjunto_path)}"
        )
        msg.attach(parte)

    _send(msg.as_bytes())
    print(f"  ✅ Mail de éxito enviado a: {', '.join(destinatarios)}")


def mail_error(
    fecha: str,
    paso: str,
    error: Exception,
    archivos_guardados: list[str] | None = None,
):
    import traceback
    fecha_fmt    = f"{fecha[6:8]}/{fecha[4:6]}/{fecha[:4]}"
    tb           = traceback.format_exc()
    archivos_str = (
        "\n".join(f"  • {a}" for a in archivos_guardados)
        if archivos_guardados else "  (ninguno)"
    )

    msg = MIMEMultipart()
    msg["Subject"] = f"❌ Conciliación FALLIDA — {fecha_fmt} | Paso: {paso}"
    msg["From"]    = GMAIL_USER
    msg["To"]      = GMAIL_USER  # solo interno

    cuerpo = f"""El pipeline falló en el paso: {paso}
Fecha: {fecha_fmt}

Error:
{str(error)}

Traceback completo:
{tb}

Archivos que SÍ se guardaron en Drive antes del fallo:
{archivos_str}

—
Doblen Solutions · Pipeline automático
"""
    msg.attach(MIMEText(cuerpo, "plain"))
    _send(msg.as_bytes())
    print(f"  ❌ Mail de error enviado a {GMAIL_USER}")


def mail_sin_datos(motivo: str):
    """
    Manda un aviso cuando no hay datos para procesar (domingo, feriado).
    Es informativo — no indica error del sistema.
    Va solo a Doblen, no al cliente.
    """
    from datetime import date
    hoy = date.today().strftime("%d/%m/%Y")

    msg = MIMEMultipart()
    msg["Subject"] = f"ℹ️ Sin datos para procesar — {hoy}"
    msg["From"]    = GMAIL_USER
    msg["To"]      = GMAIL_USER  # solo interno

    cuerpo = f"""El pipeline corrió correctamente pero no encontró datos para procesar.

Motivo: {motivo}

Esto es esperado los domingos y feriados. No se requiere ninguna acción.

—
Doblen Solutions · Pipeline automático
"""
    msg.attach(MIMEText(cuerpo, "plain"))
    _send(msg)
    print(f"  ℹ️  Mail de aviso enviado a {GMAIL_USER}")
