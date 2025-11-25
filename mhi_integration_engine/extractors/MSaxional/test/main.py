import time
import json
import base64
import logging
import os
from typing import Any, Dict, Optional, List

import pandas as pd
import requests
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# Configuración de LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("geasa-extractor")


# =========================
# Configuración de ENDPOINTS
# =========================
URL_APUNTES_PAGES = "https://cursachnew.geasa.eu/service/rest/d202305_1/Wearemind/v0/api/ApuntesContablesGetPages"
URL_APUNTES = "https://cursachnew.geasa.eu/service/rest/d202305_1/Wearemind/v0/api/ApuntesContablesGet"
URL_EMPRESAS = "https://cursachnew.geasa.eu/service/rest/d202305_1/Wearemind/v0/api/EmpresasGet"


# =========================
# Sesión con reintentos
# =========================
def build_session() -> Session:
    """Crea una sesión Requests con política de reintentos exponenciales."""
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# =========================
# Utilidades HTTP
# =========================
def get_json(session: Session, url: str, headers: Dict[str, str], params: Dict[str, Any], timeout: int = 60) -> Optional[Any]:
    """Hace GET y devuelve JSON o None, con logging y control de errores."""
    try:
        logger.debug(f"GET {url} | params={params}")
        r = session.get(url, headers=headers, params=params, timeout=timeout)
        if r.status_code >= 400:
            logger.warning(f"HTTP {r.status_code} en {url} | params={params} | body={r.text[:500]}")
            return None
        try:
            return r.json()
        except json.JSONDecodeError as je:
            logger.error(f"JSON inválido en {url} | params={params} | error={je} | body={r.text[:500]}")
            return None
    except requests.exceptions.RequestException as re:
        logger.error(f"Error de red al llamar {url} | params={params} | error={re}")
        return None


# =========================
# Normalización de payloads
# =========================
def normalize_to_df(payload: Any) -> pd.DataFrame:
    """Intenta convertir la respuesta en DataFrame, flexible con distintos envoltorios."""
    if payload is None:
        return pd.DataFrame()
    if isinstance(payload, list):
        return pd.DataFrame(payload)
    if isinstance(payload, dict):
        data = payload.get("data") or payload.get("results") or payload.get("items") or []
        if isinstance(data, dict):
            return pd.DataFrame([data])
        return pd.DataFrame(data)
    return pd.DataFrame()


# =========================
# Flujo principal
# =========================
def main():
    t0 = time.perf_counter()

    # Credenciales
    credentials = "cs.wm:Wearemind$"
    base64_str = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
    headers = {
        "Authorization": f"Basic {base64_str}",
        "Accept": "application/json",
    }

    session = build_session()

    # 1) Leemos empresas
    params_empresas = { "date_updated": "2022-01-01"}
    empresas_payload = get_json(session, URL_EMPRESAS, headers, params_empresas)
    empresas_df = normalize_to_df(empresas_payload)
    print(empresas_df)
    if empresas_df.empty or "empcode" not in empresas_df.columns:
        logger.error("No se pudieron obtener empresas o falta la columna 'empcode'. Abortando.")
        return

    listado_empresas: List[str] = empresas_df["empcode"].dropna().astype(str).tolist()
    logger.info(f"Se obtuvieron {len(listado_empresas)} empresas.")

    total_registros_reportado = 0

    # 2) Iteramos por año empresa
    year_to_load = [2022,2023,2024]
    for year in year_to_load:
        chunks: List[pd.DataFrame] = []
        start_date= f"{year}-01-01"
        end_date=f"{year}-12-31"
        for emp in listado_empresas:
            base_params = {
                "empcode": emp, 
                "date_ini": start_date, 
                "date_fin":end_date}

            # Metadatos de paginación
            meta_payload = get_json(session, URL_APUNTES_PAGES, headers, base_params)
            if not isinstance(meta_payload, list) or not meta_payload:
                logger.warning(f"[{emp}] Meta de páginas no válida: {meta_payload}")
                continue

            meta0 = meta_payload[0] if isinstance(meta_payload[0], dict) else {}
            pages = int(meta0.get("pages", 0) or 0)
            registers = int(meta0.get("registers", 0) or 0)
            total_registros_reportado += registers

            logger.info(f"[{emp}] Páginas={pages} | Registros reportados={registers}")

            if pages <= 0:
                continue

            # Iteración paginada
            for page in range(1, pages + 1):
                paged_params = {**base_params, "page": page}
                payload = get_json(session, URL_APUNTES, headers, paged_params)
                df_page = normalize_to_df(payload)
                if df_page.empty:
                    continue
                chunks.append(df_page)

        
        # 3) Concat final
        if chunks:
            resultado = pd.concat(chunks, ignore_index=True, sort=False)
        else:
            resultado = pd.DataFrame()

        logger.info(f"Registros reportados por API: {total_registros_reportado}")
        logger.info(f"Registros procesados realmente: {len(resultado)}")

        # 4) Guardado en Parquet particionado por año de "fecha"
        out_dir = "/Users/joaquin.orono/Downloads/movimientos_parquet"
        os.makedirs(out_dir, exist_ok=True)

        if not resultado.empty and "fecha" in resultado.columns:
            logger.info("Iniciamos el proceso de guardado")
            resultado["_m_account"] = "cursach"
            resultado['_m_datasource'] = "axional"
            resultado["_m_extracted_ts"] = int(time.time_ns())
            
            out_path = os.path.join(out_dir, f"ApuntesContables_{start_date}_{end_date}_cursach.parquet")
            resultado.to_parquet(out_path, index=False)
            logger.info(f"Guardado {len(resultado)} registros en {out_path}")
        else:
            logger.warning("No se encontró la columna 'fecha' o el DataFrame está vacío")

    # 5) Tiempo total
    elapsed = time.perf_counter() - t0
    logger.info(f"Tiempo total: {elapsed:.2f} s ({elapsed/60:.2f} min)")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Fallo no controlado en ejecución: {e}", exc_info=True)
