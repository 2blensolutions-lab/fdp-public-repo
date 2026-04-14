"""
02_zetti_cupones.py — Doblen Solutions x Farmacias del Pueblo
Busca cupones individuales (card-installments) para una fecha dada.
Filtra por estado: INGR (ingresado).
Genera CSV de detalle y resumen en 3_cupones/.

Correr con: python 02_zetti_cupones.py
"""

import os
import json
import logging
from datetime import datetime, timedelta

import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.WARNING)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
ZETTI_BASE_URL      = os.getenv("ZETTI_BASE_URL")
ZETTI_OAUTH_URL     = os.getenv("ZETTI_OAUTH_URL")
ZETTI_CLIENT_ID     = os.getenv("ZETTI_CLIENT_ID")
ZETTI_CLIENT_SECRET = os.getenv("ZETTI_CLIENT_SECRET")
ZETTI_USERNAME      = os.getenv("ZETTI_USERNAME")
ZETTI_PASSWORD      = os.getenv("ZETTI_PASSWORD")
ZETTI_NODE_ID_RAIZ  = os.getenv("ZETTI_NODE_ID")

# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "3_cupones")


class ZettiClient:

    def __init__(self):
        self.base_url          = ZETTI_BASE_URL.rstrip("/")
        self.oauth_url         = ZETTI_OAUTH_URL
        self._access_token     = None
        self._token_expires_at = None
        self._encode           = None

    def _get_encode(self) -> str:
        if self._encode:
            return self._encode
        r = requests.get(
            f"{self.base_url}/oauth-server/encode",
            params={"client_id": ZETTI_CLIENT_ID, "client_secret": ZETTI_CLIENT_SECRET},
            timeout=15,
        )
        r.raise_for_status()
        self._encode = r.json()["encode"]
        return self._encode

    def _request_token(self) -> dict:
        headers = {
            "Authorization": "Basic " + self._get_encode(),
            "Content-Type":  "application/x-www-form-urlencoded",
        }
        r = requests.post(
            self.oauth_url,
            headers=headers,
            data={"grant_type": "password", "username": ZETTI_USERNAME, "password": ZETTI_PASSWORD},
            timeout=30,
        )
        if r.status_code != 200:
            raise Exception(f"Error token: {r.status_code} — {r.text}")
        return r.json()

    def _ensure_token(self):
        now = datetime.now()
        if (
            not self._access_token
            or not self._token_expires_at
            or now >= self._token_expires_at - timedelta(minutes=5)
        ):
            data = self._request_token()
            self._access_token     = data["access_token"]
            self._token_expires_at = now + timedelta(seconds=data.get("expires_in", 43200))

    def _headers(self) -> dict:
        self._ensure_token()
        return {
            "Authorization": "Bearer " + self._access_token,
            "Content-Type":  "application/json",
            "Accept":        "application/json",
        }

    def get_tarjetas(self, node_id: str) -> dict:
        r = requests.post(
            f"{self.base_url}/v2/{node_id}/entities/search",
            headers=self._headers(),
            data=json.dumps({"idEntityType": 8}),
            timeout=30,
        )
        r.raise_for_status()
        data  = r.json()
        items = data if isinstance(data, list) else data.get("content", [])
        return {str(t.get("id")): t.get("name", "DESCONOCIDA") for t in items}

    def get_cupones(self, node_id: str, fecha_desde: str, fecha_hasta: str, page: int = 1) -> list:
        url    = f"{self.base_url}/v2/{node_id}/card-installments/search"
        params = {} if page == 1 else {"page": page}
        body   = {
            "emissionDateFrom": fecha_desde,
            "emissionDateTo":   fecha_hasta,
        }
        r = requests.post(
            url,
            headers=self._headers(),
            params=params,
            data=json.dumps(body),
            timeout=60,
        )
        if r.status_code != 200:
            raise Exception(f"Error cupones nodo {node_id} pág {page}: {r.status_code} — {r.text}")
        data = r.json()
        return data if isinstance(data, list) else data.get("content", [])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def fecha_iso(fecha_str: str, hora: str) -> str:
    """Convierte DD/MM/YYYY + hora a ISO con timezone Argentina."""
    d, m, y = fecha_str.split("/")
    return f"{y}-{m}-{d}T{hora}.000-03:00"


def fetch_all_cupones(client: ZettiClient, node_id: str, fecha_desde_iso: str, fecha_hasta_iso: str) -> list:
    todos = []
    page  = 1
    while True:
        items = client.get_cupones(node_id, fecha_desde_iso, fecha_hasta_iso, page=page)
        if not items:
            break
        todos.extend(items)
        print(f"  Pág {page}: {len(items)} cupones (acumulado: {len(todos)})")
        if len(items) < 50:
            break
        page += 1
    return todos


def normalizar_cupones(cupones: list, tarjetas_map: dict) -> list:
    rows = []
    for c in cupones:
        card    = c.get("card") or {}
        node    = c.get("creationNode") or {}
        status  = c.get("status") or {}
        card_id = str(card.get("id", "")) if card else ""
        rows.append({
            "cupon_id":        c.get("id"),
            "fecha":           (c.get("emissionDate") or "")[:10],
            "sucursal_id":     node.get("id"),
            "sucursal_nombre": node.get("name"),
            "tarjeta_id":      card_id,
            "tarjeta_nombre":  card.get("name") or tarjetas_map.get(card_id, "DESCONOCIDA"),
            "monto":           c.get("mainAmount", 0),
            "cuotas":          c.get("installments"),
            "nro_cupon":       c.get("couponNumber"),
            "tipo_operacion":  c.get("operationType") or (c.get("valueType") or {}).get("name"),
            "status_id":       status.get("id"),
            "estado":          status.get("name") or status.get("description"),
            "anulado":         c.get("cancellation"),
        })
    return rows


# ---------------------------------------------------------------------------
# Detectar fecha desde el archivo de Payway más reciente en 2_payway/
# ---------------------------------------------------------------------------
def detectar_fecha_desde_payway() -> str:
    """
    Lee el archivo payway_YYYYMMDD.xlsx más reciente de 2_payway/
    y devuelve la fecha en formato DD/MM/YYYY.
    Si no hay ninguno, devuelve None.
    """
    payway_dir = os.path.join(BASE_DIR, "2_payway")
    if not os.path.isdir(payway_dir):
        return None
    archivos = sorted(
        [f for f in os.listdir(payway_dir) if f.startswith("payway_") and f.endswith(".xlsx")],
        reverse=True
    )
    if not archivos:
        return None
    nombre = archivos[0]  # el más reciente
    try:
        slug = nombre.replace("payway_", "").replace(".xlsx", "")  # YYYYMMDD
        dt   = datetime.strptime(slug, "%Y%m%d")
        return dt.strftime("%d/%m/%Y")
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    # Intentar tomar la fecha del archivo de Payway más reciente
    fecha_auto = detectar_fecha_desde_payway()
    FECHA = fecha_auto if fecha_auto else "28/03/2026"  # fallback manual

    if fecha_auto:
        print(f"  Fecha detectada desde 2_payway/: {FECHA}")
    else:
        print(f"  ⚠️  No se encontró archivo de Payway — usando fecha hardcodeada: {FECHA}")

    fecha_desde_iso = fecha_iso(FECHA, "00:00:00")
    fecha_hasta_iso = fecha_iso(FECHA, "23:59:59")

    print("=" * 60)
    print(f"  Zetti — 02 Cupones {FECHA}")
    print("  Doblen Solutions x Farmacias del Pueblo")
    print("=" * 60)

    client = ZettiClient()

    print("\nAutenticando...")
    client._ensure_token()
    print("OK")

    print("\nCargando catálogo de tarjetas...")
    tarjetas_map = client.get_tarjetas(ZETTI_NODE_ID_RAIZ)
    print(f"OK — {len(tarjetas_map)} tarjetas")

    print(f"\nConsultando cupones del {FECHA}...")
    cupones = fetch_all_cupones(client, ZETTI_NODE_ID_RAIZ, fecha_desde_iso, fecha_hasta_iso)
    print(f"Total cupones traídos: {len(cupones)}")

    if not cupones:
        print(f"No se encontraron cupones para el {FECHA}.")
        return

    # DataFrame completo
    rows = normalizar_cupones(cupones, tarjetas_map)
    df   = pd.DataFrame(rows)
    df["monto"] = pd.to_numeric(df["monto"], errors="coerce").fillna(0)

    print(f"\nEstados encontrados: {df[['status_id','estado']].drop_duplicates().to_dict('records')}")
    print(f"Anulados:            {df['anulado'].value_counts().to_dict()}")

    # ---------------------------------------------------------------------------
    # Filtro: solo estado INGR (cupones ingresados, no anulados)
    # ---------------------------------------------------------------------------
    df_ingr = df[
        df["anulado"].isna() &
        (df["estado"].str.strip().str.upper() == "INGR")
    ].copy()

    print(f"\nFiltro INGR: {len(df)} → {len(df_ingr)} cupones")

    # Resumen por sucursal y tarjeta
    resumen = (
        df_ingr.groupby(["sucursal_nombre", "tarjeta_nombre"])
        .agg(
            cupones=("cupon_id", "count"),
            monto_total=("monto", "sum"),
        )
        .reset_index()
        .sort_values(["sucursal_nombre", "monto_total"], ascending=[True, False])
    )

    resumen["monto_total"] = resumen["monto_total"].round(2)

    print("\n" + "=" * 60)
    print(f"  RESUMEN — Estado INGR — {FECHA}")
    print(f"  {len(df_ingr)} cupones en {len(resumen)} combinaciones sucursal/tarjeta")
    print("=" * 60)
    print(resumen.to_string(index=False))

    # Exportar
    fecha_slug = FECHA.replace("/", "")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    archivo_detalle = os.path.join(OUTPUT_DIR, f"cupones_{fecha_slug}.csv")
    archivo_resumen = os.path.join(OUTPUT_DIR, f"resumen_cupones_{fecha_slug}.csv")

    df["monto"] = df["monto"].round(2)
    df.to_csv(archivo_detalle,    index=False, encoding="utf-8-sig", sep=";")
    resumen.to_csv(archivo_resumen, index=False, encoding="utf-8-sig", sep=";")

    print(f"\nArchivos generados en 3_cupones/:")
    print(f"  cupones_{fecha_slug}.csv         — {len(df)} cupones (todos los estados)")
    print(f"  resumen_cupones_{fecha_slug}.csv — {len(resumen)} filas (solo INGR)")
    print("=" * 60)


if __name__ == "__main__":
    main()
