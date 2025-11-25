from fastapi import APIRouter, HTTPException, Query, Body
from fastapi.responses import JSONResponse
from typing import Optional, Dict, Any, Literal, List
import requests
from requests.adapters import HTTPAdapter, Retry
from requests.auth import HTTPBasicAuth

router = APIRouter()

def fetch_endpoint(
    url: str,
    auth_type: Optional[Literal["basic", "bearer"]] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    retries: int = 5,
    backoff_factor: float = 2.0,
    status_forcelist: Optional[List[int]] = None,
) -> dict:
    if status_forcelist is None:
        status_forcelist = [429, 500, 502, 503, 504]

    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    req_headers = headers.copy() if headers else {}
    auth = None
    if auth_type == "basic" and user and password:
        auth = HTTPBasicAuth(user, password)
    elif auth_type == "bearer" and token:
        req_headers["Authorization"] = f"Bearer {token}"

    try:
        response = session.get(url, auth=auth, headers=req_headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        msg = f"Error al llamar al endpoint {url}: {type(e).__name__}: {e}"
        print(msg)
        raise HTTPException(status_code=500, detail=msg)


# -------- ENDPOINT --------
@router.post("/")
async def read_endpoint(
    url: str = Query(..., description="URL del endpoint a leer"),
    auth_type: Optional[Literal["basic", "bearer"]] = Query(None, description="Tipo de autenticación: basic o bearer"),
    user: Optional[str] = Query(None, description="Usuario para Basic Auth"),
    password: Optional[str] = Query(None, description="Password para Basic Auth"),
    token: Optional[str] = Query(None, description="Token para Bearer Auth"),
    headers: Optional[Dict[str, str]] = Body(None, description="Headers adicionales"),
    params: Optional[Dict[str, Any]] = Body(None, description="Parámetros de query string opcionales"),
):
    """
    Endpoint genérico que llama a cualquier endpoint externo y devuelve su respuesta en JSON.
    """
    data = fetch_endpoint(
        url=url,
        auth_type=auth_type,
        user=user,
        password=password,
        token=token,
        headers=headers,
        params=params
    )
    return JSONResponse(content={"data": data}, status_code=200)
