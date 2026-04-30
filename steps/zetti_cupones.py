"""
steps/zetti_cupones.py — Descarga cupones de Zetti para la fecha indicada
Doblen Solutions x Farmacias del Pueblo

Filtros aplicados:
  - Estado INGR / INGRESADO / AGRUP: se incluyen con monto positivo
  - Anulados (ANULADO / ANULADOR): se incluyen con monto NEGATIVO
  - UNIFICADO, MODIF y cualquier otro estado: se excluyen

Variables de entorno requeridas:
    ZETTI_BASE_URL, ZETTI_OAUTH_URL, ZETTI_NODE_ID
    ZETTI_CLIENT_ID, ZETTI_CLIENT_SECRET
    ZETTI_USERNAME, ZETTI_PASSWORD
"""

import os
import json
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

# Estados que se incluyen con monto positivo
ESTADOS_VALIDOS = {"INGR", "INGRESADO", "AGRUP"}

# Valores del campo anulado que se incluyen con monto negativo
ANULADOS_NEGATIVOS = {"ANULADO", "ANULADOR"}

# Tarjetas que se excluyen de la conciliación
TARJETAS_EXCLUIDAS = {
    "TARJETA MERCADO PAGO QR",
    "TARJETA DEL PUEBLO",
    "TARJETA MODO",
}


class ZettiClient:

    def __init__(self):
        self.base_url          = ZETTI_BASE_URL.rstrip("/")
        self.oauth_url         = ZETTI_OAUTH_URL
        self.node_id           = ZETTI_NODE_ID
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
            raise Exception(f"Error token Zetti: {r.status_code} — {r.text}")
        return r.json()

    def _ensure_token(self):
        now = datetime.now()
        if (not self._access_token or not self._token_expires_at
                or now >= self._token_expires_at - timedelta(minutes=5)):
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

    def get_tarjetas(self) -> dict:
        r = requests.post(
            f"{self.base_url}/v2/{self.node_id}/entities/search",
            headers=self._headers(),
            data=json.dumps({"idEntityType": 8}),
            timeout=30,
        )
        r.raise_for_status()
        data  = r.json()
        items = data if isinstance(data, list) else data.get("content", [])
        return {str(t.get("id")): t.get("name", "DESCONOCIDA") for t in items}

    def get_cupones(self, fecha_desde: str, fecha_hasta: str, page: int = 1) -> list:
        url    = f"{self.base_url}/v2/{self.node_id}/card-installments/search"
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
            raise Exception(f"Error cupones Zetti pág {page}: {r.status_code} — {r.text}")
        data = r.json()
        return data if isinstance(data, list) else data.get("content", [])


def _descargar_todos(client: ZettiClient, fecha: date) -> list:
    fecha_desde = f"{fecha.isoformat()}T00:00:00.000-03:00"
    fecha_hasta = f"{fecha.isoformat()}T23:59:59.000-03:00"

    todos = []
    page  = 1
    while True:
        items = client.get_cupones(fecha_desde, fecha_hasta, page=page)
        if not items:
            break
        todos.extend(items)
        print(f"  Página {page}: {len(items)} cupones (acumulado: {len(todos)})")
        if len(items) < 50:
            break
        page += 1
    return todos


def _normalizar(cupones: list, tarjetas_map: dict) -> pd.DataFrame:
    rows = []
    for c in cupones:
        card    = c.get("card") or {}
        node    = c.get("creationNode") or {}
        status  = c.get("status") or {}
        cancel  = c.get("cancellation")
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
            "estado":          status.get("name") or status.get("description"),
            "status_id":       status.get("id"),
            "anulado":         cancel,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["monto"] = pd.to_numeric(df["monto"], errors="coerce").fillna(0)
    return df


def run(fecha: date, output_todos: str, output_resumen: str) -> tuple[str, str, pd.DataFrame]:
    """
    Descarga cupones de Zetti para la fecha indicada.

    Lógica de filtrado:
      - Estado INGR/INGRESADO + sin anular → monto positivo (incluir)
      - Estado INGR/INGRESADO + anulado (ANULADO/ANULADOR) → monto negativo (incluir)
      - Cualquier otro estado → excluir
      - Tarjetas MERCADO PAGO QR, DEL PUEBLO, MODO → excluir siempre

    Genera dos archivos:
      - output_todos:   todos los cupones sin filtrar (auditoría)
      - output_resumen: cupones procesados agrupados por sucursal y tarjeta
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
    cupones = _descargar_todos(client, fecha)
    print(f"  Total cupones descargados: {len(cupones)}")

    if not cupones:
        raise ValueError(
            f"Zetti no devolvió cupones para el {fecha.isoformat()}. "
            f"Verificar que sea un día hábil con transacciones."
        )

    df_todos = _normalizar(cupones, tarjetas_map)
    print(f"  Estados encontrados:  {df_todos['estado'].unique().tolist()}")
    print(f"  Anulados encontrados: {df_todos['anulado'].dropna().unique().tolist()}")

    # Guardar CSV completo sin filtrar (auditoría)
    os.makedirs(os.path.dirname(output_todos), exist_ok=True)
    df_todos.to_csv(output_todos, index=False, encoding="utf-8-sig", sep=";")
    print(f"  ✅ CSV completo: {os.path.basename(output_todos)} ({len(df_todos)} cupones)")

    # -----------------------------------------------------------------------
    # Filtrado y ajuste de montos
    # -----------------------------------------------------------------------

    # 1. Cupones INGR válidos (sin anular) → monto positivo
    df_validos = df_todos[
        (df_todos["estado"].str.upper().isin(ESTADOS_VALIDOS)) &
        (df_todos["anulado"].isna())
    ].copy()

    # 2. Cupones INGR anulados → monto negativo
    df_anulados = df_todos[
        (df_todos["estado"].str.upper().isin(ESTADOS_VALIDOS)) &
        (df_todos["anulado"].str.upper().isin(ANULADOS_NEGATIVOS) if df_todos["anulado"].notna().any() else False)
    ].copy()
    df_anulados["monto"] = df_anulados["monto"] * -1

    # 3. Unir válidos + anulados con monto negativo
    df_procesados = pd.concat([df_validos, df_anulados], ignore_index=True)
    print(f"  Tarjetas únicas antes de excluir: {sorted(df_procesados['tarjeta_nombre'].dropna().unique())}")

    # 4. Excluir tarjetas que no concilian
    mask_excluidas = df_procesados["tarjeta_nombre"].str.upper().isin(
        {t.upper() for t in TARJETAS_EXCLUIDAS}
    )
    excluidas_count = mask_excluidas.sum()
    df_procesados = df_procesados[~mask_excluidas].copy()

    print(f"  Cupones INGR válidos:   {len(df_validos)}")
    print(f"  Cupones INGR anulados:  {len(df_anulados)} (monto negativo)")
    print(f"  Tarjetas excluidas:     {excluidas_count} filas ({', '.join(TARJETAS_EXCLUIDAS)})")
    print(f"  Total para conciliar:   {len(df_procesados)}")

    # -----------------------------------------------------------------------
    # Resumen agrupado por sucursal y tarjeta (lo que lee el paso 4)
    # -----------------------------------------------------------------------
    resumen = (
        df_procesados.groupby(["sucursal_nombre", "tarjeta_nombre"])
        .agg(
            cupones=("cupon_id", "count"),
            monto_total=("monto", "sum"),
        )
        .reset_index()
        .sort_values(["sucursal_nombre", "monto_total"], ascending=[True, False])
    )

    os.makedirs(os.path.dirname(output_resumen), exist_ok=True)
    resumen.to_csv(output_resumen, index=False, encoding="utf-8-sig", sep=";")
    print(f"  ✅ CSV resumen: {os.path.basename(output_resumen)} ({len(resumen)} filas)")
    print(f"  Sucursales Zetti: {sorted(resumen['sucursal_nombre'].unique())}")
    print(f"  Tarjetas Zetti:   {sorted(resumen['tarjeta_nombre'].unique())}")

    return output_todos, output_resumen, resumen
