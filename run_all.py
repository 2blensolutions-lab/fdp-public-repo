"""
run_all.py — Orquestador principal
Doblen Solutions x Farmacias del Pueblo

Pasos:
  1. Descarga CSVs de Payway vía SFTP (3 sociedades)
  2. Procesa y genera Excel de Payway
  3. Descarga cupones de Zetti (genera CSV completo + CSV resumen)
  4. Concilia Payway × Zetti
  5. Sube todos los archivos a Google Drive
  6. Manda mail de éxito

Si cualquier paso falla:
  - Sube a Drive lo que se generó hasta ese punto
  - Manda mail de error interno (solo a Doblen)
  - Sale con código 1

Cron en Railway: 0 11 * * * (8hs ARG)
"""

import os
import sys
import traceback
from datetime import date

from dotenv import load_dotenv
load_dotenv()

from steps import payway_download
from steps import payway_procesar
from steps import zetti_cupones
from steps import conciliar as step_conciliar

from utils.drive  import DriveClient
from utils.mailer import mail_exito, mail_error


def main():
    print("=" * 60)
    print("  Pipeline Conciliación — Doblen Solutions x Farmacias del Pueblo")
    print("=" * 60)

    archivos_generados = []  # lista de (nombre_drive, ruta_local)
    fecha = None
    fecha_str = None

    # ------------------------------------------------------------------
    # PASO 1 — Descarga Payway SFTP
    # ------------------------------------------------------------------
    try:
        fecha, rutas_csv = payway_download.run()
        fecha_str = fecha.strftime("%Y%m%d")
        for ruta in rutas_csv:
            archivos_generados.append((os.path.basename(ruta), ruta))
    except Exception as e:
        _fallo("Descarga Payway SFTP", e, fecha, archivos_generados)

    # ------------------------------------------------------------------
    # PASO 2 — Procesar Payway
    # ------------------------------------------------------------------
    payway_xlsx = f"/tmp/{fecha_str}_payway_procesado.xlsx"
    try:
        payway_path, df_pay, fecha = payway_procesar.run(
            rutas_csv=[r for _, r in archivos_generados],
            output_path=payway_xlsx,
        )
        archivos_generados.append((os.path.basename(payway_path), payway_path))
    except Exception as e:
        _fallo("Procesar Payway", e, fecha, archivos_generados)

    # ------------------------------------------------------------------
    # PASO 3 — Cupones Zetti (genera dos CSVs)
    # ------------------------------------------------------------------
    zetti_todos_csv   = f"/tmp/{fecha_str}_cupones_zetti_todos.csv"
    zetti_resumen_csv = f"/tmp/{fecha_str}_cupones_zetti.csv"
    try:
        todos_path, resumen_path, df_zet = zetti_cupones.run(
            fecha=fecha,
            output_todos=zetti_todos_csv,
            output_resumen=zetti_resumen_csv,
        )
        archivos_generados.append((os.path.basename(todos_path),   todos_path))
        archivos_generados.append((os.path.basename(resumen_path), resumen_path))
    except Exception as e:
        _fallo("Descarga Zetti", e, fecha, archivos_generados)

    # ------------------------------------------------------------------
    # PASO 4 — Conciliación
    # ------------------------------------------------------------------
    conciliacion_xlsx = f"/tmp/{fecha_str}_conciliacion.xlsx"
    try:
        conc_path, resumen = step_conciliar.run(
            payway_path=payway_path,
            zetti_path=zetti_resumen_csv,
            output_path=conciliacion_xlsx,
            fecha=fecha,
        )
        archivos_generados.append((os.path.basename(conc_path), conc_path))
    except Exception as e:
        _fallo("Conciliación", e, fecha, archivos_generados)

    # ------------------------------------------------------------------
    # PASO 5 — Subir todo a Drive
    # ------------------------------------------------------------------
    try:
        print(f"\n{'='*60}")
        print(f"  PASO 5 — Subir archivos a Drive")
        print(f"{'='*60}")
        drive  = DriveClient()
        folder = drive.get_or_create_run_folder(fecha_str)
        drive.upload_many(archivos_generados, folder)
        print(f"  ✅ {len(archivos_generados)} archivos subidos a Drive / runs / {fecha_str}/")
    except Exception as e:
        # Drive falló pero la conciliación está — loguear y seguir
        print(f"  ⚠️  Error subiendo a Drive: {e}")
        print(f"  Continuando con el mail de todas formas...")

    # ------------------------------------------------------------------
    # PASO 6 — Mail de éxito
    # ------------------------------------------------------------------
    try:
        mail_exito(
            fecha=fecha_str,
            resumen=resumen,
            adjunto_path=conc_path,
        )
    except Exception as e:
        print(f"  ⚠️  Error enviando mail de éxito: {e}")

    print(f"\n{'='*60}")
    print(f"  ✅ Pipeline completado — {fecha.strftime('%d/%m/%Y')}")
    print(f"{'='*60}")


def _fallo(paso: str, error: Exception, fecha, archivos_generados: list):
    print(f"\n❌ FALLO EN PASO: {paso}")
    print(f"   {type(error).__name__}: {error}")
    traceback.print_exc()

    fecha_str = fecha.strftime("%Y%m%d") if fecha else "DESCONOCIDA"

    nombres_subidos = []
    if archivos_generados:
        try:
            print("\n  Intentando subir archivos parciales a Drive...")
            drive  = DriveClient()
            folder = drive.get_or_create_run_folder(fecha_str)
            drive.upload_many(archivos_generados, folder)
            nombres_subidos = [n for n, _ in archivos_generados]
            print(f"  Subidos: {nombres_subidos}")
        except Exception as drive_err:
            print(f"  ⚠️  No se pudo subir a Drive: {drive_err}")

    try:
        mail_error(
            fecha=fecha_str,
            paso=paso,
            error=error,
            archivos_guardados=nombres_subidos,
        )
    except Exception as mail_err:
        print(f"  ⚠️  No se pudo mandar el mail de error: {mail_err}")

    sys.exit(1)


if __name__ == "__main__":
    main()
