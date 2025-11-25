import time, gcsfs, json, gzip, base64
import pandas as pd
from typing import Optional, List, Dict, Any, Union
from fastapi import APIRouter, HTTPException, Query, Body
from fastapi.responses import JSONResponse

router = APIRouter()

class IntegrationLayerSaver:
    def __init__(
        self,
        account: str,
        datasource_name: str,
        dataset_name: str,
        connection_id: str,
        flow_id: str,
        run_id: str,
        folder: Optional[str] = None,
        filename: Optional[str] = None,
    ):
        self.account = account
        self.datasource_name = datasource_name
        self.dataset_name = dataset_name
        self.connection_id = connection_id
        self.flow_id = flow_id
        self.run_id = run_id
        self.folder = f"/{folder}" if folder else ""
        self.filename = f"{filename}_{account}.parquet" if filename else f"{account}.parquet"
        self.data_source = f"int_{datasource_name}"
        self.bucket = "ocean_data"  # bucket fijo

    def save(self, data: List[Dict[str, Any]], type_mapping: Optional[Dict[str, Any]] = None) -> str:
        try:
            # Convertir JSON a DataFrame
            df_without_datatypes = pd.DataFrame(data)
            
            # Aplicar tipos solo si se proporciona type_mapping
            if type_mapping:
                df = self._forceDataTypes(df_without_datatypes, type_mapping)
            else:
                df = df_without_datatypes
                print("No se proporcionó type_mapping, pandas/pyarrow decidirá los tipos de datos")

            # Añadir metadatos
            df["_m_account"] = self.account
            df["_m_datasource"] = self.datasource_name
            df["_m_connection_id"] = self.connection_id
            df["_m_flow_id"] = self.flow_id
            df["_m_run_id"] = self.run_id
            df["_m_extracted_ts"] = int(time.time())
            
            # Ruta destino en GCS
            destination = f"gs://{self.bucket}/{self.data_source}/{self.dataset_name}/{self.connection_id}{self.folder}/{self.filename}"

            # Guardar como Parquet en GCS usando gcsfs
            fs = gcsfs.GCSFileSystem()
            with fs.open(destination, 'wb') as f:
                df.to_parquet(f, compression='gzip', engine='pyarrow', index=False)
            
            msg = f"SUCCESS: Almacenados {len(df.index)} registros en {destination}"
            print(msg)
            return msg

        except Exception as e:
            msg = f"ERROR: Falló al guardar datos en {self.dataset_name} para account={self.account}, datasource={self.datasource_name}. Error: {str(e)}"
            print(msg)
            raise HTTPException(status_code=500, detail=msg)
    
    def _decompress_data(self, compressed_data: str) -> List[Dict[str, Any]]:
        """
        Descomprime datos que vienen en formato base64 + gzip
        
        Args:
            compressed_data (str): Datos comprimidos en base64
            
        Returns:
            List[Dict[str, Any]]: Datos descomprimidos
        """
        try:
            # Decodificar base64
            compressed_bytes = base64.b64decode(compressed_data)
            
            # Descomprimir gzip
            decompressed_bytes = gzip.decompress(compressed_bytes)
            
            # Convertir a JSON
            json_str = decompressed_bytes.decode('utf-8')
            data = json.loads(json_str)
            
            print(f"Datos descomprimidos exitosamente: {len(data)} registros")
            return data
            
        except Exception as e:
            raise HTTPException(
                status_code=400, 
                detail=f"Error al descomprimir datos: {str(e)}"
            )
    
    def _forceDataTypes(self, df: pd.DataFrame, type_mapping: Dict[str, Any]) -> pd.DataFrame:
        """
        Aplica tipos de datos al DataFrame según el mapeo proporcionado
        
        Args:
            df (pandas.DataFrame): DataFrame a convertir
            type_mapping (dict): Mapeo de tipos de datos por columna
            
        Returns:
            pandas.DataFrame: DataFrame con tipos convertidos
        """
        # Crear un diccionario de conversiones solo para columnas que existen en el DataFrame
        conversions = {col: dtype for col, dtype in type_mapping.items() if col in df.columns}
        
        # Si no hay conversiones, devolver el DataFrame original
        if not conversions:
            print("No hay conversiones de tipos a aplicar")
            return df
        
        # Aplicar conversiones de tipos
        print("Iniciamos el proceso de conversión de tipos")
        for col, dtype in conversions.items():
            print(f"Procesando campo {col} a tipo: {dtype}")
            try:
                # Manejar conversiones de tipos especiales
                if dtype.startswith('datetime'):
                    # Para campos de fecha, dejar como string para evitar problemas
                    df[col] = df[col].astype('string')
                    print(f"Campo {col} convertido a string (datetime)")
                elif dtype == 'float64':
                    # Para campos numéricos, convertir
                    try:
                        df[col] = df[col].astype(float).fillna(0.0)
                        print(f"Campo {col} convertido a float64")
                    except:
                        # Si falla, intentar con to_numeric para ser más flexible
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
                        print(f"Campo {col} convertido a float64 (con coerción)")
                elif dtype == 'int64':
                    # Para enteros, convertir a entero pero manejar posibles NaN
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    # Llenar NaN con 0 para poder convertir a int64
                    df[col] = df[col].fillna(0).astype('int64')
                    print(f"Campo {col} convertido a int64")
                elif dtype == 'string':
                    df[col] = df[col].astype('string')
                    print(f"Campo {col} convertido a string")
                elif dtype == 'boolean':
                    df[col] = df[col].convert_dtypes(convert_boolean=True)
                    print(f"Campo {col} convertido a boolean")
                else:
                    # Conversión general de tipos
                    df[col] = df[col].astype(dtype)
                    print(f"Campo {col} convertido a {dtype}")
            except Exception as e:
                print(f"ERROR: No se pudo convertir columna {col} a {dtype}: {str(e)}")
                # Continuar con la siguiente columna sin fallar
        
        print("Proceso de conversión de tipos completado")
        return df


# -------- ENDPOINT --------
@router.post("/")
async def save_integration_layer(
    account: str = Query(..., description="Cuenta origen"),
    datasource_name: str = Query(..., description="Fuente de datos"),
    dataset_name: str = Query(..., description="Nombre del dataset"),
    connection_id: str = Query(..., description="ID de la conexión"),
    flow_id: str = Query(..., description="ID del flujo"),
    run_id: str = Query(..., description="ID de la ejecución"),
    folder: Optional[str] = Query(None, description="Carpeta opcional"),
    filename: Optional[str] = Query(None, description="Nombre de archivo opcional"),
    compressed: bool = Query(False, description="Indica si los datos vienen comprimidos"),
    payload: Dict[str, Any] = Body(..., description="Payload con datos y mapeo de tipos")
):
    """
    Guarda un dataset en la capa de integración (Integration Layer) como fichero Parquet en GCS.
    
    Payload sin comprimir:
    {
        "data": [...],  // Array de objetos con los datos (REQUERIDO)
        "type_mapping": {  // Mapeo de tipos de datos (OPCIONAL)
            "MotivChaPrice": "string",
            "ExtraAmount": "float64",
            "Price": "float64",
            ...
        }
    }
    
    Payload comprimido (compressed=true):
    {
        "compressed_data": "H4sIAAAAAAAA...",  // Datos comprimidos en base64
        "type_mapping": {  // Mapeo de tipos de datos (OPCIONAL)
            "MotivChaPrice": "string",
            "ExtraAmount": "float64",
            ...
        }
    }
    
    Si no se proporciona type_mapping, pandas/pyarrow decidirá automáticamente los tipos de datos.
    """
    try:
        # Crear saver
        saver = IntegrationLayerSaver(
            account, datasource_name, dataset_name, 
            connection_id, flow_id, run_id, folder, filename
        )
        
        # Extraer type_mapping (siempre opcional)
        type_mapping = payload.get("type_mapping")
        if type_mapping is not None and not isinstance(type_mapping, dict):
            raise HTTPException(status_code=400, detail="El campo 'type_mapping' debe ser un objeto")
        
        # Procesar según si viene comprimido o no
        if compressed:
            # Datos comprimidos
            compressed_data = payload.get("compressed_data")
            if not compressed_data:
                raise HTTPException(status_code=400, detail="El campo 'compressed_data' es requerido cuando compressed=true")
            
            if not isinstance(compressed_data, str):
                raise HTTPException(status_code=400, detail="El campo 'compressed_data' debe ser un string en base64")
            
            # Descomprimir datos
            data = saver._decompress_data(compressed_data)
            
        else:
            # Datos sin comprimir (comportamiento original)
            data = payload.get("data")
            if not data:
                raise HTTPException(status_code=400, detail="El campo 'data' es requerido cuando compressed=false")
            
            if not isinstance(data, list):
                raise HTTPException(status_code=400, detail="El campo 'data' debe ser una lista")
        
        # Procesar y guardar
        result_msg = saver.save(data, type_mapping)
        
        return JSONResponse(content={"message": result_msg}, status_code=200)
        
    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"ERROR: Error inesperado al procesar la solicitud: {str(e)}"
        print(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)