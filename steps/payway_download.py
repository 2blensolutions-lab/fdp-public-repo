"""
steps/01_payway_download.py — Descarga CSVs de Payway vía SFTP
Doblen Solutions x Farmacias del Pueblo

Conecta a sftp.payway.com.ar con cada una de las 3 cuentas (una por sociedad),
busca el archivo Simil_Lote del día a conciliar (ayer), lo descarga y lo
renombra a YYYYMMDD_Movimientos_Nde3.csv en /tmp/.

Variables de entorno requeridas:
    PAYWAY_SFTP_HOST   — default: sftp.payway.com.ar
    PAYWAY_USER_1      — CUIT sociedad 1 (Salvado Hermanos): 30-71839947-1
    PAYWAY_PASS_1
    PAYWAY_USER_2      — CUIT sociedad 2 (ADB): 30-70792120-6
    PAYWAY_PASS_2
    PAYWAY_USER_3      — CUIT sociedad 3: 30-67265440-4
    PAYWAY_PASS_3

Retorna lista de rutas locales /tmp/YYYYMMDD_Movimientos_Nde3.csv
"""

import os
import paramiko
from datetime import date, timedelta


SFTP_HOST = os.getenv("PAYWAY_SFTP_HOST", "sftp.payway.com.ar")
SFTP_PORT = int(os.getenv("PAYWAY_SFTP_PORT", "22"))
SFTP_PATH = "/reporteria/out/movimientos"

SOCIEDADES = [
    {
        "numero":  1,
        "nombre":  "Salvado Hermanos",
        "usuario": os.getenv("PAYWAY_USER_1", "30-71839947-1"),
        "key":     os.getenv("PAYWAY_KEY_1", ""),
    },
    {
        "numero":  2,
        "nombre":  "ADB",
        "usuario": os.getenv("PAYWAY_USER_2", "30-70792120-6"),
        "key":     os.getenv("PAYWAY_KEY_2", ""),
    },
    {
        "numero":  3,
        "nombre":  "Tercera sociedad",
        "usuario": os.getenv("PAYWAY_USER_3", "30-67265440-4"),
        "key":     os.getenv("PAYWAY_KEY_3", ""),
    },
]


def _fecha_a_conciliar() -> date:
    """
    Devuelve la fecha a conciliar: ayer.
    El job corre a las 8am ARG — los archivos de Payway del día anterior
    ya están disponibles en el SFTP.
    """
    return date.today() - timedelta(days=1)


def _buscar_archivo(sftp: paramiko.SFTPClient, fecha: date) -> str | None:
    """
    Lista el directorio SFTP y devuelve el nombre del archivo que:
      1. Contiene 'Simil_Lote' en el nombre
      2. Fue modificado en la fecha indicada

    Si hay más de uno (poco probable), toma el más reciente.
    Si no hay ninguno, devuelve None.
    """
    try:
        archivos = sftp.listdir_attr(SFTP_PATH)
    except FileNotFoundError:
        raise Exception(f"El path SFTP no existe: {SFTP_PATH}")

    candidatos = []
    for attr in archivos:
        nombre = attr.filename

        # Filtrar por nombre
        if "Simil_Lote" not in nombre:
            continue

        # Filtrar por fecha de modificación
        if attr.st_mtime is None:
            continue
        fecha_modificacion = date.fromtimestamp(attr.st_mtime)
        if fecha_modificacion != fecha:
            continue

        candidatos.append(attr)

    if not candidatos:
        return None

    # Si hubiera más de uno, tomar el más reciente
    candidatos.sort(key=lambda a: a.st_mtime, reverse=True)
    return candidatos[0].filename


def descargar_sociedad(
    sociedad: dict,
    fecha: date,
    local_path: str,
) -> str:
    nombre_soc = sociedad["nombre"]
    usuario    = sociedad["usuario"]
    numero     = sociedad["numero"]
    key_str    = sociedad["key"]

    if not key_str:
        raise EnvironmentError(
            f"PAYWAY_KEY_{numero} no está configurada en las variables de entorno."
        )

    print(f"  Conectando SFTP — {nombre_soc} ({usuario})...")

    # Cargar la private key desde el string (formato OpenSSH)
    import io as _io
    key_file = _io.StringIO(key_str)
    try:
        pkey = paramiko.Ed25519Key.from_private_key(key_file)
    except paramiko.ssh_exception.SSHException:
        key_file.seek(0)
        try:
            pkey = paramiko.RSAKey.from_private_key(key_file)
        except paramiko.ssh_exception.SSHException:
            key_file.seek(0)
            pkey = paramiko.ECDSAKey.from_private_key(key_file)

    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    try:
        transport.connect(username=usuario, pkey=pkey)
        sftp = paramiko.SFTPClient.from_transport(transport)

        nombre_remoto = _buscar_archivo(sftp, fecha)

        if nombre_remoto is None:
            raise FileNotFoundError(
                f"No se encontró archivo Simil_Lote con fecha {fecha.isoformat()} "
                f"en {SFTP_PATH} para {nombre_soc} ({usuario}). "
                f"Verificar que Payway haya generado el reporte."
            )

        ruta_remota = f"{SFTP_PATH}/{nombre_remoto}"
        print(f"  Encontrado: {nombre_remoto} → descargando como {os.path.basename(local_path)}")
        sftp.get(ruta_remota, local_path)
        print(f"  ✅ Descargado: {os.path.basename(local_path)}")
        return local_path

    finally:
        transport.close()


def run(fecha: date | None = None) -> tuple[date, list[str]]:
    """
    Punto de entrada del paso 1.
    Descarga los 3 CSVs de Payway y los deja en /tmp/.

    Retorna (fecha_conciliada, [lista de rutas locales])
    Lanza Exception si falla cualquier sociedad.
    """
    if fecha is None:
        fecha = _fecha_a_conciliar()

    fecha_str = fecha.strftime("%Y%m%d")
    total     = len(SOCIEDADES)

    print(f"\n{'='*60}")
    print(f"  PASO 1 — Descarga Payway SFTP — {fecha.strftime('%d/%m/%Y')}")
    print(f"{'='*60}")

    rutas_locales = []

    for soc in SOCIEDADES:
        numero     = soc["numero"]
        local_path = f"/tmp/{fecha_str}_Movimientos_{numero}de{total}.csv"

        ruta = descargar_sociedad(soc, fecha, local_path)
        rutas_locales.append(ruta)

    print(f"\n  Paso 1 completo — {len(rutas_locales)} archivos descargados.")
    return fecha, rutas_locales


# ---------------------------------------------------------------------------
# Ejecución directa para testing
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    fecha, rutas = run()
    print("\nArchivos generados:")
    for r in rutas:
        size_kb = os.path.getsize(r) / 1024
        print(f"  {r}  ({size_kb:.1f} KB)")
