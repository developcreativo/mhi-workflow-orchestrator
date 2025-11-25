import json, os, time, io,logging
from typing import Dict, Any, Optional
import pandas as pd
from google.cloud import storage, secretmanager, pubsub_v1 as pubsub

# Set up logging
#logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class Controller:
    """
    Controller class with common utility functions for data extractors.
    """
    def __init__(self, 
                 account:str, 
                 connection_id:str, 
                 extractor_name:str,
                 flow_id:str = None,
                 run_id:str = None,
                 task_id:str = None
                 ):
        """Initialize the controller with GCP project configuration."""
        # Constantes
        try:
            self._extractor_name = str(extractor_name).lower()
            self._m_account = account
            self.connection_id = connection_id
            self.flow_id=flow_id
            self.run_id=run_id
            self.task_id=task_id
            self.__GCP_PROJECT_ID = str(os.environ.get('_M_PROJECT_ID'))
            self.project_id = os.environ.get("_M_PROJECT_ID")
            self.region = os.environ.get("_M_FUNCTION_EXECUTION_REGION", "europe-southwest1")
            self.__STORAGE_INTEGRATION_BUCKET =  str(os.environ.get('_M_BUCKET_DATA'))
            self.__STORAGE_LASTEST_INCREMENTAL_BUCKET =  str(os.environ.get('_M_BUCKET_LASTEST_EXECUTION'))
            self.__SECRET_VERSION = 'latest'
            self.__RETURN = {
                0: 'fail',
                1: 'success',
                2: 'warning'
            }
            # Initialize GCP clients
            self.storage_client = storage.Client(project=self.project_id)
            self.secret_client = secretmanager.SecretManagerServiceClient()
            self.publisher = pubsub.PublisherClient()
        except Exception as e:
            msg='El proceso de instanciación de controller ha fallado'
            logger.error(msg)
            raise msg

    def getSecret(self, secret_name: str) -> Dict[str, Any]:
        """
        Retrieve a secret from Google Cloud Secret Manager.

        Args:
            secret_name (str): The name of the secret.

        Returns:
            Dict[str, Any]: The secret payload as a dictionary.
        """
        try:
            secret_path = f"projects/{self.project_id}/secrets/{secret_name}/versions/{self.__SECRET_VERSION}"
            response = self.secret_client.access_secret_version(request={"name": secret_path})
            payload_str = response.payload.data.decode("UTF-8")
            return json.loads(payload_str)
        except Exception as e:
            logger.error(f"Error getting secret {secret_name}: {e}")
            raise

    def getLatestSuccessExecution(self, dataset: str,  **kwargs) -> Optional[Dict[str, Any]]:
        """
        Retrieves the state of the last successful execution from a JSON file in Google Cloud Storage.
        
        Args:
            control_key (str): The key or name of the control file (e.g., 'events_last_read').
            **kwargs: Can include 'event_id' for event-specific control files.

        Returns:
            Optional[Dict[str, Any]]: The parsed JSON data or None if not found.
        """
        try:
            # Crear la ruta del archivo, usando el dataset como subcarpeta
            if kwargs.get('event_id'):
                file_path = f"{self._extractor_name}/{dataset}/event_{kwargs['event_id']}_{self.connection_id}.json"
            else:
                file_path = f"{self._extractor_name}/{dataset}/{dataset}_{self.connection_id}.json"
                
            bucket = self.storage_client.bucket(self.__STORAGE_LASTEST_INCREMENTAL_BUCKET)
            blob = bucket.blob(file_path)
            
            if blob.exists():
                json_string = blob.download_as_text()
                return json.loads(json_string)
            else:
                logger.info(f"Archivo de control no encontrado: gs://{self.__STORAGE_LASTEST_INCREMENTAL_BUCKET}/{file_path}")
                return None
        except Exception as e:
            logger.error(f"Error obteniendo el archivo de control para {dataset}: {e}")
            return None

    def postLatestSuccessExecution(self, dataset: str, data: Dict[str, Any], **kwargs):
        """
        Guarda el estado de la última ejecución exitosa en un archivo JSON en Google Cloud Storage,
        usando una estructura de carpetas por `control_key`.

        Args:
            control_key (str): The key or name of the control file (e.g., 'events_last_read').
            data (Dict[str, Any]): The data to save.
            **kwargs: Can include 'event_id' for event-specific control files.
        """
        try:
            # Crea la ruta del archivo, usando el control_key como subcarpeta
            if kwargs.get('event_id'):
                file_path = f"{self._extractor_name}/{dataset}/event_{kwargs['event_id']}_{self.connection_id}.json"
            else:
                file_path = f"{self._extractor_name}/{dataset}/{dataset}_{self.connection_id}.json"

            bucket = self.storage_client.bucket(self.__STORAGE_LASTEST_INCREMENTAL_BUCKET)
            blob = bucket.blob(file_path)

            blob.upload_from_string(
                data=json.dumps(data),
                content_type='application/json'
            )

            logger.info(f"Estado de control guardado en gs://{self.__STORAGE_LASTEST_INCREMENTAL_BUCKET}/{file_path}")
            return True
        
        except Exception as e:
            logger.error(f"Error guardando el archivo de control para {dataset}: {e}")
            return False
    
    
    def saveStorage(self, dataset: str, filename: str, df: pd.DataFrame):
        """
        Saves a pandas DataFrame to a parquet file in Google Cloud Storage.
        
        Args:
            dataset (str): The name of the dataset (e.g., 'events').
            filename (str): The name of the file (e.g., 'events_20230101.parquet').
            df (pd.DataFrame): The DataFrame to save.
        """
        try:
            full_path = f"int_{self._extractor_name}/{dataset}/{filename}"
            
            # Use a buffer to avoid writing to disk
            pq_buffer = io.BytesIO()
            
            # Normalize column names before saving
            df.columns = [str(col).lower().replace(' ', '_').replace('.', '_') for col in df.columns]

            # Adjust data types to be compatible with Parquet
            df = self._adjust_data_types(df, dataset)
            df['_m_extracted_ts'] = int(time.time_ns())
            df['_m_account'] = self._m_account
            df['_m_connection_id'] = self.connection_id
            df['_m_datasource'] = self._extractor_name
            df['_m_flow_id'] = self.flow_id
            df['_m_run_id'] = self.run_id
            df['_m_task_id'] = self.task_id
            
            df.to_parquet(pq_buffer, engine='pyarrow', index=False)
            
            # Rewind the buffer to the beginning
            pq_buffer.seek(0)
            
            bucket = self.storage_client.bucket(self.__STORAGE_INTEGRATION_BUCKET)
            blob = bucket.blob(full_path)
            blob.upload_from_file(pq_buffer, content_type='application/octet-stream')
            
            logger.info(f"Archivo guardado exitosamente en {full_path} con {len(df)} filas")
            return True
        except Exception as e:
            logger.error(f"Error guardando el archivo {full_path}: {e}")
            return False
    
    
    def putNotification(
            self, 
            ptype, 
            subject,
            body,
            to_email=None,
            from_email=None, 
            cc=None,
            bcc=None,
            template=None):
        # se queda en este fichero como auxiliar del repositorio del extractor
        future = ''
        topic_id   = 'notifications'
        
        data = json.dumps({
            "_m_account": self._m_account,
            "type":ptype,
            "data":{
                "subject":subject,
                "text":body
            }
        }).encode('utf-8')
        try:
            future = self._putMessageInBus(topic_id=topic_id, data=data)
            msg = f"El método putNotificationFin ha finalizado en {self.__RETURN[1]}"
            logger.info(msg)
        except Exception as e:
            msg=f'El método putNotification ha fallado: {str(e)}'
            logger.error(msg)
            raise Exception(msg)
    
    def _putMessageInBus(self, topic_id, data):

        try:
            topic_path = self.publisher.topic_path(self.__GCP_PROJECT_ID, topic_id)
            future = self.publisher.publish(topic=topic_path, data=data)
            logger.info(f"Se ha publicado un mensaje en el topic {topic_id} con el código: {future.result()}")
            return(self.__RETURN[1])
        except Exception as e:
            msg=f'El método _putMessageInBus ha fallado: {str(e)}'
            logger.error(msg)
            raise Exception(msg)

    def _adjust_data_types(self, df: pd.DataFrame, dataset: str) -> pd.DataFrame:
        """
        Adjusts data types of a DataFrame based on a predefined mapping file.
        
        Args:
            df (pd.DataFrame): The DataFrame to adjust.
            dataset (str): The name of the dataset to find the mapping for.
            
        Returns:
            pd.DataFrame: The DataFrame with adjusted data types.
        """
        script_dir = os.path.dirname(os.path.abspath(__file__))
        mapping_path = os.path.join(script_dir, 'data-type-mapping.json')  # <-- aquí el cambio

        if not os.path.exists(mapping_path):
            logger.warning(f"No se encontró el archivo de mapeo de tipos de datos en {mapping_path}")
            return df

        try:
            with open(mapping_path, 'r') as f:
                type_mapping = json.load(f).get(dataset, {})
                
            for col, dtype in type_mapping.items():
                if col in df.columns:
                    try:
                        if dtype.startswith('datetime'):
                            df[col] = df[col].astype('string')
                        elif dtype == 'float64':
                            try:
                                df[col] = df[col].astype(float).fillna(0.0)
                            except:
                                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
                        elif dtype == 'int64':
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                            df[col] = df[col].fillna(0).astype('int64')
                        elif dtype == 'string':
                            df[col] = df[col].astype('string')
                        elif dtype == 'boolean':
                            df[col] = df[col].convert_dtypes(convert_boolean=True)
                        else:
                            df[col] = df[col].astype(dtype)
                    except Exception as e:
                        logger.warning(f"Error al convertir columna {col} a {dtype}: {str(e)}")
            
        except Exception as e:
            logger.error(f"Error leyendo o procesando el archivo de mapeo de tipos de datos: {e}")
            
        return df