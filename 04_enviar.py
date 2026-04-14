"""
04_enviar.py — Doblen Solutions x Farmacias del Pueblo
Manda el Excel de conciliación más reciente por mail a Joaco y Fran.

Requiere variables de entorno:
    GMAIL_USER           → 2blen.solutions@gmail.com
    GMAIL_APP_PASSWORD   → App Password de Gmail
    MAIL_DESTINATARIOS   → emails separados por coma

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
BASE_DIR         = os.path.dirname(os.path.abspath(__file__))
CONCILIACION_DIR = os.path.join(BASE_DIR, "4_conciliacion")

GMAIL_USER         = os.getenv("GMAIL_USER", "2blen.solutions@gmail.com")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")
MAIL_DESTINATARIOS = os.getenv("MAIL_DESTINATARIOS")


def buscar_conciliacion() -> str:
    if not os.path.isdir(CONCILIACION_DIR):
        raise FileNotFoundError(f"No existe la carpeta {CONCILIACION_DIR} — corré primero 03_conciliar.py")
    archivos = sorted(
        [f for f in os.listdir(CONCILIACION_DIR)
         if f.startswith("conciliacion_") and f.endswith(".xlsx")],
        reverse=True
    )
    if not archivos:
        raise FileNotFoundError("No se encontró ningún archivo de conciliación — corré primero 03_conciliar.py")
    return os.path.join(CONCILIACION_DIR, archivos[0])


def mandar_mail(filepath: str):
    if not GMAIL_APP_PASSWORD:
        raise ValueError("Falta la variable de entorno GMAIL_APP_PASSWORD")
    if not MAIL_DESTINATARIOS:
        raise ValueError("Falta la variable de entorno MAIL_DESTINATARIOS")

    nombre_archivo = os.path.basename(filepath)
    try:
        slug          = nombre_archivo.replace("conciliacion_", "").replace(".xlsx", "")
        fecha_legible = datetime.strptime(slug, "%Y%m%d").strftime("%d/%m/%Y")
    except ValueError:
        fecha_legible = slug

    destinatarios = [m.strip() for m in MAIL_DESTINATARIOS.split(",") if m.strip()]

    msg            = MIMEMultipart()
    msg["From"]    = GMAIL_USER
    msg["To"]      = ", ".join(destinatarios)
    msg["Subject"] = f"Conciliación Payway × Zetti — {fecha_legible}"

    cuerpo = f"""Hola,

Se adjunta el reporte de conciliación Payway × Zetti del {fecha_legible}.

—
Doblen Solutions
(mail automático)
"""
    msg.attach(MIMEText(cuerpo, "plain"))

    with open(filepath, "rb") as f:
        adjunto = MIMEBase("application", "octet-stream")
        adjunto.set_payload(f.read())
    encoders.encode_base64(adjunto)
    adjunto.add_header("Content-Disposition", f'attachment; filename="{nombre_archivo}"')
    msg.attach(adjunto)

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.ehlo()
        server.starttls()
        server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_USER, destinatarios, msg.as_string())

    print(f"  Mail enviado a: {', '.join(destinatarios)}")


def main():
    print("=" * 60)
    print("  04 — Enviar conciliación por mail")
    print("  Doblen Solutions x Farmacias del Pueblo")
    print("=" * 60)

    print("\nBuscando archivo de conciliación...")
    filepath = buscar_conciliacion()
    print(f"  {os.path.basename(filepath)}")

    print("\nMandando mail...")
    mandar_mail(filepath)

    print("\n✓ Listo")
    print("=" * 60)


if __name__ == "__main__":
    main()
