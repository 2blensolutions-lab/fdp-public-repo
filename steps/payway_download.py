"""
steps/payway_download.py — Descarga CSVs de Payway vía SFTP
Doblen Solutions x Farmacias del Pueblo

Variables de entorno requeridas:
    PAYWAY_SFTP_HOST   — default: sftp.payway.com.ar
    PAYWAY_USER_1      — CUIT sociedad 1 (Salvado Hermanos): 30-71839947-1
    PAYWAY_KEY_1       — private key SSH en base64 (generada con fix_key.py)
    PAYWAY_USER_2      — CUIT sociedad 2 (ADB): 30-70792120-6
    PAYWAY_KEY_2
    PAYWAY_USER_3      — CUIT sociedad 3: 30-67265440-4
    PAYWAY_KEY_3
"""

import os
import base64
import tempfile
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
    return date.today() - timedelta(days=1)


def _buscar_archivo(sftp: paramiko.SFTPClient, fecha: date) -> str | None:
    try:
        archivos = sftp.listdir_attr(SFTP_PATH)
    except FileNotFoundError:
        raise Exception(f"El path SFTP no existe: {SFTP_PATH}")

    candidatos = []
    for attr in archivos:
        nombre = attr.filename
        if "Simil_Lote" not in nombre:
            continue
        if attr.st_mtime is None:
            continue
        fecha_modificacion = date.fromtimestamp(attr.st_mtime)
        if fecha_modificacion != fecha:
            continue
        candidatos.append(attr)

    if not candidatos:
        return None

    candidatos.sort(key=lambda a: a.st_mtime, reverse=True)
    return candidatos[0].filename


def _cargar_pkey(key_str: str) -> paramiko.PKey:
    """
    Carga la private key desde un string en base64.
    1. Decodifica el base64 para obtener el PEM original
    2. Escribe a archivo temporal
    3. Carga con paramiko
    """
    try:
        key_pem = base64.b64decode(key_str).decode("utf-8")
    except Exception as e:
        raise paramiko.ssh_exception.SSHException(
            f"No se pudo decodificar PAYWAY_KEY de base64: {e}"
        )

    print(f"  Key decodificada: {len(key_pem)} chars, empieza con: {repr(key_pem[:50])}")

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".pem", delete=False, newline="\n"
    ) as tmp:
        tmp.write(key_pem.strip())
        tmp.write("\n")
        tmp_path = tmp.name

    try:
        try:
            return paramiko.Ed25519Key.from_private_key_file(tmp_path)
        except paramiko.ssh_exception.SSHException:
            pass
        try:
            return paramiko.RSAKey.from_private_key_file(tmp_path)
        except paramiko.ssh_exception.SSHException:
            pass
        try:
            return paramiko.ECDSAKey.from_private_key_file(tmp_path)
        except paramiko.ssh_exception.SSHException:
            pass
        raise paramiko.ssh_exception.SSHException(
            "No se pudo parsear la private key. "
            "Verificar que PAYWAY_KEY_N sea el output de fix_key.py (base64 sin comentarios)."
        )
    finally:
        os.unlink(tmp_path)


def descargar_sociedad(sociedad: dict, fecha: date, local_path: str) -> str:
    nombre_soc = sociedad["nombre"]
    usuario    = sociedad["usuario"]
    numero     = sociedad["numero"]
    key_str    = sociedad["key"]

    if not key_str:
        raise EnvironmentError(
            f"PAYWAY_KEY_{numero} no está configurada en las variables de entorno."
        )

    print(f"  Conectando SFTP — {nombre_soc} ({usuario})...")

    pkey = _cargar_pkey(key_str)

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


if __name__ == "__main__":
    fecha, rutas = run()
    print("\nArchivos generados:")
    for r in rutas:
        size_kb = os.path.getsize(r) / 1024
        print(f"  {r}  ({size_kb:.1f} KB)")


class SinDatosException(Exception):
    """
    Se lanza cuando no hay archivos en el SFTP para la fecha indicada.
    Es un caso esperado (domingos, feriados) — no es un error del sistema.
    El orquestador la captura y termina limpiamente sin crashear.
    """
    pass
