"""
steps/03_zetti_cupones.py — Descarga cupones de Zetti para la fecha indicada
Doblen Solutions x Farmacias del Pueblo

Lógica idéntica al zetti_reporte_03mar.py original de Joaco.
Diferencia: recibe la fecha por parámetro (viene del paso 2) en lugar
de tenerla hardcodeada.

Variables de entorno requeridas:
    ZETTI_BASE_URL      (default: http://farmaciasdelpueblo.com:9080/api-rest)
    ZETTI_OAUTH_URL     (default: http://farmaciasdelpueblo.com:9080/oauth-server/oauth/token)
    ZETTI_NODE_ID       (default: 2379975)
    ZETTI_CLIENT_ID
    ZETTI_CLIENT_SECRET
    ZETTI_USERNAME
    ZETTI_PASSWORD

Retorna la ruta local al CSV de resumen (solo cupones INGR, sin anulados).
"""

import os
import logging
from datetime import datetime, timedelta, date

import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.WARNING)

ZETTI_BASE_URL      = os.getenv("ZETTI_BASE_URL",      "http://farmaciasdelpueblo.com:9080/api-rest")
ZETTI_OAUTH_URL     = os.getenv("ZETTI_OAUTH_URL",     "http://farmaciasdelpueblo.com:9080/oauth-server/oauth/token")
ZETTI_NODE_ID       = os.getenv("ZETTI_NODE_ID",       "2379975")
ZETTI_CLIENT_ID     = os.getenv("ZETTI_CLIENT_ID",     "doblen_solutions_api")
ZETTI_CLIENT_SECRET = os.getenv("ZETTI_CLIENT_SECRET", "")
ZETTI_USERNAME      = os.getenv("ZETTI_USERNAME",      "DOBLENAPI")
ZETTI_PASSWORD      = os.getenv("ZETTI_PASSWORD",      "")


# ---------------------------------------------------------------------------
# Cliente Zetti (idéntico al original)
# ---------------------------------------------------------------------------
class ZettiClient:

    def __init__(self):
        self.base_url          = ZETTI_BASE_URL.rstrip("/")
        self.oauth_url         = ZETTI_OAUTH_URL
        self.node_id           = ZETTI_NODE_ID
        self._access_token     = None
        self._refresh_token    = None
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
        self._encode = r.json()["encode"]
        return self._encode

    def _request_token(self) -> dict:
        headers = {
            "Authorization": "Basic " + self._get_encode(),
            "Content-Type":  "application/x-www-form-urlencoded",
        }
        body = {"grant_type": "password", "username": ZETTI_USERNAME, "password": ZETTI_PASSWORD}
        r = requests.post(self.oauth_url, headers=headers, data=body, timeout=30)
        if r.status_code != 200:
            raise Exception(f"Error token Zetti: {r.status_code} — {r.text}")
        return r.json()

    def _ensure_token(self):
        now = datetime.now()
        if (not self._access_token or not self._token_expires_at
                or now >= self._token_expires_at - timedelta(minutes=5)):
            data = self._request_token()
            self._access_token     = data["access_token"]
            self._refresh_token    = data.get("refresh_token")
            self._token_expires_at = now + timedelta(seconds=data.get("expires_in", 43200))

    def _headers(self) -> dict:
        self._ensure_token()
        return {"Authorization": "Bearer " + self._access_token, "Content-Type": "application/json"}

    def get_cupones(self, fecha_desde: str, fecha_hasta: str, page: int = 0) -> dict:
        url  = f"{self.base_url}/v2/{self.node_id}/card-installments/search"
        body = {
            "emissionDateFrom": fecha_desde,
            "emissionDateTo":   fecha_hasta,
            "page":             page,
            "perPage":          500,
        }
        r = requests.post(url, headers=self._headers(), json=body, timeout=60)
        if r.status_code != 200:
            raise Exception(f"Error cupones Zetti: {r.status_code} — {r.text}")
        return r.json()

    def get_tarjetas(self) -> dict:
        r = requests.post(
            f"{self.base_url}/v2/{self.node_id}/entities/search",
            headers=self._headers(),
            json={"idEntityType": 8},
            timeout=30,
        )
        data  = r.json()
        items = data if isinstance(data, list) else data.get("content", [])
        return {str(t.get("id")): t.get("name", "DESCONOCIDA") for t in items}


# ---------------------------------------------------------------------------
# Descarga y normalización
# ---------------------------------------------------------------------------
def _descargar_todos_los_cupones(client: ZettiClient, fecha: date) -> list:
    fecha_desde = f"{fecha.isoformat()}T00:00:00.000-03:00"
    fecha_hasta = f"{fecha.isoformat()}T23:59:59.000-03:00"

    todos = []
    page  = 0
    while True:
        data  = client.get_cupones(fecha_desde, fecha_hasta, page=page)
        items = data.get("content", data) if isinstance(data, dict) else data
        total = data.get("totalElements", len(items)) if isinstance(data, dict) else len(items)

        todos.extend(items)
        print(f"  Página {page}: {len(items)} cupones (acumulado: {len(todos)} / {total})")

        if len(items) < 500:
            break
        page += 1

    return todos


def _normalizar(cupones: list, tarjetas_map: dict) -> pd.DataFrame:
    rows = []
    for c in cupones:
        card   = c.get("card") or {}
        node   = c.get("creationNode") or {}
        status = c.get("status") or {}
        card_id = str(card.get("id", "")) if card else ""

        rows.append({
            "cupon_id":        c.get("id"),
            "fecha":           (c.get("emissionDate", "") or "")[:10],
            "sucursal_id":     node.get("id"),
            "sucursal_nombre": node.get("name"),
            "tarjeta_id":      card_id,
            "tarjeta_nombre":  card.get("name") or tarjetas_map.get(card_id, "DESCONOCIDA"),
            "monto":           c.get("mainAmount", 0),
            "estado":          status.get("description") or status.get("name"),
            "cupon_numero":    c.get("couponNumber"),
            "anulado":         c.get("cancellation"),
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["monto"] = pd.to_numeric(df["monto"], errors="coerce").fillna(0)
    return df


# ---------------------------------------------------------------------------
# Punto de entrada del pipeline
# ---------------------------------------------------------------------------
def run(fecha: date, output_path: str) -> tuple[str, pd.DataFrame]:
    """
    Descarga los cupones de Zetti para la fecha indicada y genera el CSV de resumen.

    fecha:       date a conciliar
    output_path: ruta local donde guardar el CSV (/tmp/YYYYMMDD_cupones_zetti.csv)

    Retorna (output_path, df_resumen)
    donde df_resumen contiene solo cupones INGR sin anulados,
    agrupados por sucursal y tarjeta.
    """
    print(f"\n{'='*60}")
    print(f"  PASO 3 — Descarga Zetti — {fecha.strftime('%d/%m/%Y')}")
    print(f"{'='*60}")

    client = ZettiClient()

    print("  Autenticando con Zetti...")
    client._ensure_token()
    print("  OK")

    print("  Cargando catálogo de tarjetas...")
    tarjetas_map = client.get_tarjetas()
    print(f"  OK — {len(tarjetas_map)} tarjetas")

    print(f"  Consultando cupones del {fecha.isoformat()}...")
    cupones = _descargar_todos_los_cupones(client, fecha)
    print(f"  Total cupones descargados: {len(cupones)}")

    if not cupones:
        raise ValueError(
            f"Zetti no devolvió cupones para el {fecha.isoformat()}. "
            f"Verificar que sea un día hábil con transacciones."
        )

    df_todos = _normalizar(cupones, tarjetas_map)
    print(f"  Estados encontrados: {df_todos['estado'].unique().tolist()}")

    # Filtro: solo INGR + sin anulados (igual que el original)
    df_ingr = df_todos[
        (df_todos["estado"] == "INGR") &
        (df_todos["anulado"].isna())
    ].copy()
    print(f"  Cupones INGR (válidos): {len(df_ingr)} de {len(df_todos)}")

    # Resumen agrupado por sucursal y tarjeta (lo que lee el paso 4)
    resumen = (
        df_ingr.groupby(["sucursal_nombre", "tarjeta_nombre"])
        .agg(
            cupones=("cupon_id", "count"),
            monto_total=("monto", "sum"),
        )
        .reset_index()
        .sort_values(["sucursal_nombre", "monto_total"], ascending=[True, False])
    )
    resumen.columns = ["sucursal_nombre", "tarjeta_nombre", "cupones", "monto_total"]

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    resumen.to_csv(output_path, index=False, encoding="utf-8-sig", sep=";")

    print(f"  ✅ CSV generado: {os.path.basename(output_path)} ({len(resumen)} filas)")
    print(f"  Sucursales Zetti: {sorted(resumen['sucursal_nombre'].unique())}")
    print(f"  Tarjetas Zetti:   {sorted(resumen['tarjeta_nombre'].unique())}")

    return output_path, resumen
