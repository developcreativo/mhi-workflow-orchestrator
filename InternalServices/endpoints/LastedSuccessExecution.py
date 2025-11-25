import os, json
from typing import Dict, Any, Optional
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from google.cloud import storage

class LatestSuccessExecution:
    def __init__(self, account: str, datasource_name: str, connection_id: str, dataset_name: str, partition: Optional[str] = None):
        # Store parameters as instance variables
        self.account = account
        self.datasource_name = datasource_name.lower()
        self.connection_id = connection_id
        self.dataset_name = dataset_name
        self.partition = partition

        project_id = str(os.environ.get('_M_PROJECT_ID'))
        bucket_name = 'lastest_incremental_data'
        
        # Construcción del path dinámico
        if partition:
            self.gcs_path = f"{datasource_name}/{dataset_name}/{connection_id}/{account}_{partition}.json"
        else:
            self.gcs_path = f"{datasource_name}/{dataset_name}/{connection_id}/{account}.json"

        storage_client = storage.Client(project=project_id)
        self.bucket = storage_client.bucket(bucket_name)

    def get_value(self) -> Optional[Dict[str, Any]]:
        """
        Get the latest successful execution data for incremental loads.
        """
        try:
            blob = self.bucket.blob(self.gcs_path)
            if not blob.exists():
                return None

            content = blob.download_as_string()
            return json.loads(content)

        except Exception as e:
            print(f"Warning: Could not get latest execution data: {str(e)}")
            return None

    def set_value(self, incremental_field: str, incremental_value: Any) -> None:
        """
        Store the latest successful execution data for incremental loads.
        """
        try:
            blob = self.bucket.blob(self.gcs_path)

            # Create JSON data with all relevant information
            data = {
                "account": self.account,
                "datasource_name": self.datasource_name,
                "dataset_name": self.dataset_name,
                "connection_id": self.connection_id,
                "partition": self.partition,
                "incremental_field": incremental_field,
                "incremental_value": incremental_value,
            }
            
            json_string = json.dumps(data, ensure_ascii=False, indent=2)
            
            blob.upload_from_string(json_string, content_type="application/json")

        except Exception as e:
            msg = f"Error guardando el fichero con la última ejecución exitosa: {str(e)}"
            print(msg)
            raise Exception(msg)

# ---------- ROUTER ----------
router = APIRouter()

@router.get("/")
async def get_last_date(
    account: str = Query(...),
    datasource_name: str = Query(...),
    connection_id: str = Query(...),
    dataset_name: str = Query(...),
    partition: Optional[str] = Query(None)
):
    file = LatestSuccessExecution(account, datasource_name, connection_id, dataset_name, partition)
    content = file.get_value()
    if content is None:
        return JSONResponse(content={"error": "No data found"}, status_code=404)
    return JSONResponse(content=content, status_code=200)


@router.post("/")
async def set_last_date(
    account: str = Query(...),
    datasource_name: str = Query(...),
    connection_id: str = Query(...),
    dataset_name: str = Query(...),
    partition: Optional[str] = Query(None),
    incremental_field: str = Query(...),
    incremental_value: str = Query(...)
):
    file = LatestSuccessExecution(account, datasource_name, connection_id, dataset_name, partition)
    file.set_value(incremental_field, incremental_value)
    return JSONResponse(content={"message": "Lasted success date updated"}, status_code=200)
