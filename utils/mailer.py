"""
utils/mailer.py — Mails de éxito y error
Doblen Solutions x Farmacias del Pueblo

Variables de entorno requeridas:
    GMAIL_APP_PASSWORD   — App Password de la cuenta 2blen.solutions@gmail.com
    MAIL_DESTINATARIOS   — Emails separados por coma para el mail de éxito
                           ej: "joaco@example.com,fran@example.com"

El mail de error SIEMPRE va solo a 2blen.solutions@gmail.com (interno).
"""

import os
import smtplib
import traceback
from email.message import EmailMessage
from datetime import date


GMAIL_USER         = "2blen.solutions@gmail.com"
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
MAIL_DESTINATARIOS = os.getenv("MAIL_DESTINATARIOS", GMAIL_USER)


def _send(msg: EmailMessage):
    """Envía el mensaje via Gmail SMTP."""
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        smtp.send_message(msg)


def mail_exito(
    fecha: str,
    resumen: dict,
    adjunto_path: str | None = None,
):
    """
    Manda el mail de éxito con el Excel de conciliación adjunto.

    fecha:        YYYYMMDD, ej: "20260403"
    resumen:      dict con stats del run, ej:
                  {
                    "sucursales": 12,
                    "filas_payway": 340,
                    "cupones_zetti": 298,
                    "filas_conciliacion": 109,
                  }
    adjunto_path: ruta local al Excel de conciliación (opcional)
    """
    fecha_fmt = f"{fecha[6:8]}/{fecha[4:6]}/{fecha[:4]}"
    destinatarios = [d.strip() for d in MAIL_DESTINATARIOS.split(",") if d.strip()]

    msg = EmailMessage()
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
    msg.set_content(cuerpo)

    # Adjuntar Excel si existe
    if adjunto_path and os.path.exists(adjunto_path):
        with open(adjunto_path, "rb") as f:
            msg.add_attachment(
                f.read(),
                maintype="application",
                subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=os.path.basename(adjunto_path),
            )

    _send(msg)
    print(f"  ✅ Mail de éxito enviado a: {', '.join(destinatarios)}")


def mail_error(
    fecha: str,
    paso: str,
    error: Exception,
    archivos_guardados: list[str] | None = None,
):
    """
    Manda el mail de error interno (solo a Doblen).

    fecha:             YYYYMMDD
    paso:              nombre del paso que falló, ej: "Descarga Payway"
    error:             la excepción capturada
    archivos_guardados: lista de nombres de archivos que sí se subieron a Drive
    """
    fecha_fmt = f"{fecha[6:8]}/{fecha[4:6]}/{fecha[:4]}"
    tb = traceback.format_exc()

    archivos_str = (
        "\n".join(f"  • {a}" for a in archivos_guardados)
        if archivos_guardados
        else "  (ninguno)"
    )

    msg = EmailMessage()
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

Revisar Drive > Farmacias del Pueblo > runs > {fecha}/ para ver el estado parcial.

—
Doblen Solutions · Pipeline automático
"""
    msg.set_content(cuerpo)
    _send(msg)
    print(f"  ❌ Mail de error enviado a {GMAIL_USER}")
