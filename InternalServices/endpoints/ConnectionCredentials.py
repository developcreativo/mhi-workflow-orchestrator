import os, json
from typing import Dict, Any
from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse
from google.cloud import secretmanager


class ConnectionSecret:
    def __init__(self, connection_id: str):
        self.project_id = str(os.environ.get('_M_PROJECT_ID'))
        self.secret_version = "latest"
        self.connection_id = connection_id
        self.secret_client = secretmanager.SecretManagerServiceClient()

    def get_secret(self) -> Dict[str, Any]:
        """
        Retrieve and parse a secret from Google Cloud Secret Manager.
        """
        try:
            name = f"projects/{self.project_id}/secrets/{self.connection_id}/versions/{self.secret_version}"
            response = self.secret_client.access_secret_version(request={"name": name})
            payload = response.payload.data.decode("UTF-8")
            return json.loads(payload)
        except Exception as e:
            msg = f"Error retrieving secret {self.connection_id}: {str(e)}"
            print(msg)
            raise HTTPException(status_code=404, detail=msg)


# ---------- ROUTER ----------
router = APIRouter()

@router.get("/")
async def get_connection_secret(
    account: str = Query(...),
    datasource_name: str = Query(...),
    connection_id: str = Query(...)
):
    """
    Retrieve a connection secret from Google Cloud Secret Manager.
    Validates that `account` and `datasource` are part of the connection_id.
    """
    # --- Validación de consistencia ---
    if account not in connection_id or datasource_name not in connection_id:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid connection_id: must belong to '{account}' and datasource='{datasource_name}'"
        )

    # --- Obtener secreto ---
    secret = ConnectionSecret(connection_id)
    content = secret.get_secret()
    return JSONResponse(content=content, status_code=200)
