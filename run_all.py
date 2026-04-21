"""
run_all.py — Orquestador principal
Doblen Solutions x Farmacias del Pueblo

Cron en Railway: 0 11 * * * (8hs ARG)
"""

import os
import sys
import traceback
from datetime import date

from dotenv import load_dotenv
load_dotenv()

from steps import payway_download
from steps.payway_download import SinDatosException
from steps import payway_procesar
from steps import zetti_cupones
from steps import conciliar as step_conciliar

from utils.drive  import DriveClient
from utils.mailer import mail_exito, mail_error, mail_sin_datos


def main():
    print("=" * 60)
    print("  Pipeline Conciliación — Doblen Solutions x Farmacias del Pueblo")
    print("=" * 60)

    archivos_generados = []
    fecha_datos = None
    fecha_str   = None

    # ------------------------------------------------------------------
    # PASO 1 — Descarga Payway SFTP
    # ------------------------------------------------------------------
    rutas_csv    = []
    fecha_run_str = None
    try:
        fecha_run, rutas_csv = payway_download.run()
        fecha_run_str = fecha_run.strftime("%Y%m%d")
        for ruta in rutas_csv:
            archivos_generados.append((os.path.basename(ruta), ruta))

    except SinDatosException as e:
        # Caso esperado — domingo, feriado, etc.
        # Terminar limpiamente sin crashear
        print(f"\n  ℹ️  {e}")
        print("  Pipeline terminado sin procesar — no es un error.")
        try:
            mail_sin_datos(str(e))
        except Exception as mail_err:
            print(f"  ⚠️  No se pudo mandar el mail de aviso: {mail_err}")
        sys.exit(0)  # exit 0 = éxito, Railway no lo marca como crash

    except Exception as e:
        _fallo("Descarga Payway SFTP", e, fecha_datos, archivos_generados)

    # ------------------------------------------------------------------
    # PASO 2 — Procesar Payway (determina la fecha real de los datos)
    # ------------------------------------------------------------------
    try:
        payway_xlsx_tmp = f"/tmp/{fecha_run_str}_payway_procesado_tmp.xlsx"
        payway_path_tmp, df_pay, fecha_datos = payway_procesar.run(
            rutas_csv=[r for _, r in archivos_generados],
            output_path=payway_xlsx_tmp,
        )

        fecha_str   = fecha_datos.strftime("%Y%m%d")
        payway_xlsx = f"/tmp/{fecha_str}_payway_procesado.xlsx"
        os.rename(payway_path_tmp, payway_xlsx)

        # Renombrar CSVs crudos con fecha correcta
        rutas_renombradas = []
        total = len(rutas_csv)
        for i, (nombre_viejo, ruta_vieja) in enumerate(archivos_generados, 1):
            nombre_nuevo = f"{fecha_str}_Movimientos_{i}de{total}.csv"
            ruta_nueva   = f"/tmp/{nombre_nuevo}"
            os.rename(ruta_vieja, ruta_nueva)
            rutas_renombradas.append((nombre_nuevo, ruta_nueva))

        archivos_generados = rutas_renombradas
        archivos_generados.append((f"{fecha_str}_payway_procesado.xlsx", payway_xlsx))
        print(f"  📅 Fecha de los datos: {fecha_datos.strftime('%d/%m/%Y')}")

    except Exception as e:
        _fallo("Procesar Payway", e, fecha_datos, archivos_generados)

    # ------------------------------------------------------------------
    # PASO 3 — Cupones Zetti
    # ------------------------------------------------------------------
    zetti_todos_csv   = f"/tmp/{fecha_str}_cupones_zetti_todos.csv"
    zetti_resumen_csv = f"/tmp/{fecha_str}_cupones_zetti.csv"
    try:
        todos_path, resumen_path, df_zet = zetti_cupones.run(
            fecha=fecha_datos,
            output_todos=zetti_todos_csv,
            output_resumen=zetti_resumen_csv,
        )
        archivos_generados.append((os.path.basename(todos_path),   todos_path))
        archivos_generados.append((os.path.basename(resumen_path), resumen_path))
    except Exception as e:
        _fallo("Descarga Zetti", e, fecha_datos, archivos_generados)

    # ------------------------------------------------------------------
    # PASO 4 — Conciliación
    # ------------------------------------------------------------------
    conciliacion_xlsx = f"/tmp/{fecha_str}_conciliacion.xlsx"
    try:
        conc_path, resumen = step_conciliar.run(
            payway_path=payway_xlsx,
            zetti_path=zetti_resumen_csv,
            output_path=conciliacion_xlsx,
            fecha=fecha_datos,
        )
        archivos_generados.append((os.path.basename(conc_path), conc_path))
    except Exception as e:
        _fallo("Conciliación", e, fecha_datos, archivos_generados)

    # ------------------------------------------------------------------
    # PASO 5 — Subir a Drive
    # ------------------------------------------------------------------
    try:
        print(f"\n{'='*60}")
        print(f"  PASO 5 — Subir archivos a Drive — {fecha_datos.strftime('%d/%m/%Y')}")
        print(f"{'='*60}")
        drive  = DriveClient()
        folder = drive.get_or_create_run_folder(fecha_str)
        drive.upload_many(archivos_generados, folder)
        print(f"  ✅ {len(archivos_generados)} archivos subidos a Drive / runs / {fecha_str}/")
    except Exception as e:
        print(f"  ⚠️  Error subiendo a Drive: {e}")

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
    print(f"  ✅ Pipeline completado — {fecha_datos.strftime('%d/%m/%Y')}")
    print(f"{'='*60}")


def _fallo(paso: str, error: Exception, fecha_datos, archivos_generados: list):
    print(f"\n❌ FALLO EN PASO: {paso}")
    print(f"   {type(error).__name__}: {error}")
    traceback.print_exc()

    fecha_str = fecha_datos.strftime("%Y%m%d") if fecha_datos else "DESCONOCIDA"

    nombres_subidos = []
    if archivos_generados:
        try:
            drive  = DriveClient()
            folder = drive.get_or_create_run_folder(fecha_str)
            drive.upload_many(archivos_generados, folder)
            nombres_subidos = [n for n, _ in archivos_generados]
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
