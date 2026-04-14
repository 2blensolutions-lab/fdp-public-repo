"""
04_enviar.py — Doblen Solutions x Farmacias del Pueblo
Manda el Excel de conciliación más reciente por mail a Joaco y Fran.
Usa Resend (resend.com) — sin restricciones de red en Railway.

Requiere variables de entorno:
    RESEND_API_KEY       → API key de Resend (empieza con re_)
    GMAIL_USER           → dirección que aparece como remitente
    MAIL_DESTINATARIOS   → emails separados por coma

Correr con: python 04_enviar.py
"""

import os
import base64
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_DIR         = os.path.dirname(os.path.abspath(__file__))
CONCILIACION_DIR = os.path.join(BASE_DIR, "4_conciliacion")

RESEND_API_KEY     = os.getenv("RESEND_API_KEY")
GMAIL_USER         = os.getenv("GMAIL_USER", "2blen.solutions@gmail.com")
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
    if not RESEND_API_KEY:
        raise ValueError("Falta la variable de entorno RESEND_API_KEY")
    if not MAIL_DESTINATARIOS:
        raise ValueError("Falta la variable de entorno MAIL_DESTINATARIOS")

    nombre_archivo = os.path.basename(filepath)
    try:
        slug          = nombre_archivo.replace("conciliacion_", "").replace(".xlsx", "")
        fecha_legible = datetime.strptime(slug, "%Y%m%d").strftime("%d/%m/%Y")
    except ValueError:
        fecha_legible = slug

    destinatarios = [m.strip() for m in MAIL_DESTINATARIOS.split(",") if m.strip()]

    # Leer el archivo y convertir a base64
    with open(filepath, "rb") as f:
        contenido_b64 = base64.b64encode(f.read()).decode("utf-8")

    payload = {
        "from":    f"Doblen Solutions <onboarding@resend.dev>",
        "to":      destinatarios,
        "subject": f"Conciliación Payway × Zetti — {fecha_legible}",
        "text":    f"Hola,\n\nSe adjunta el reporte de conciliación Payway × Zetti del {fecha_legible}.\n\n—\nDoblen Solutions\n(mail automático)",
        "attachments": [
            {
                "filename": nombre_archivo,
                "content":  contenido_b64,
            }
        ],
    }

    r = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type":  "application/json",
        },
        json=payload,
        timeout=30,
    )

    if r.status_code not in (200, 201):
        raise Exception(f"Error Resend: {r.status_code} — {r.text}")

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
