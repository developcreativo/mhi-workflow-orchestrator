import json, base64, os, time
from typing import Dict, Any, Optional
import pandas as pd
from google.cloud import storage, secretmanager, pubsub_v1 as pubsub

class Controller:
    """
    Controller class with common utility functions for data extractors.
    """
    def __init__(self, account, extractor_name):
        """Initialize the controller with GCP project configuration."""
        # Constantes
        
        self.__GCP_PROJECT_ID = str(os.environ.get('_M_PROJECT_ID'))
        self.__SECRET_VERSION = 'latest'
        self.__STORAGE_INTEGRATION_BUCKET =   str(os.environ.get('_M_BUCKET_DATA'))
        self.__STORAGE_LASTEST_INCREMENTAL_BUCKET =  str(os.environ.get('_M_BUCKET_LASTEST_EXECUTION'))
        self.__RETURN = {
        0:'fail',
        1:'success',
        2:'warning'
        }
        self._extractor_name = str(extractor_name).lower()
        self._m_account = account
        self.project_id = os.environ.get("_M_PROJECT_ID")
        self.region = os.environ.get("_M_FUNCTION_EXECUTION_REGION", "europe-southwest1")
        
        # Initialize GCP clients
        self.storage_client = storage.Client(project=self.project_id)
        self.secret_client = secretmanager.SecretManagerServiceClient()
        self.publisher = pubsub.PublisherClient()
        
    def getSecret(self, secret_name: str) -> Dict[str, Any]:
        """
        Retrieve a secret from Google Cloud Secret Manager.
        
        Args:
            secret_name: Name of the secret to retrieve
            
        Returns:
            Dictionary containing the secret data
        """
        try:
            # Build the resource name
            name = f"projects/{self.project_id}/secrets/{secret_name}/versions/{self.__SECRET_VERSION}"
            
            # Access the secret
            response = self.secret_client.access_secret_version(request={"name": name})
            
            # Decode the secret
            payload = response.payload.data.decode("UTF-8")
            
            # Parse JSON content
            return json.loads(payload)
            
        except Exception as e:
            raise Exception(f"Error retrieving secret {secret_name}: {str(e)}")
    
    def saveStorage(self, dataset_name: str, 
                     file_name: str, data_frame: pd.DataFrame) -> str:
        """
        Save a DataFrame to Google Cloud Storage in parquet format.
        
        Args:
            extractor_name: Name of the extractor/connector
            dataset_name: Name of the dataset
            file_name: Name of the file
            data_frame: Pandas DataFrame containing the data
            
        Returns:
            GCS URI for the stored file
        """
        try:
            data_frame = self._forceDataTypes(data_frame, dataset_name)
            data_frame['_m_account'] = self._m_account
            data_frame['_m_datasource'] = self._extractor_name
            data_frame['_m_extracted_ts'] = int(time.time_ns())
            data_source = f'int_{self._extractor_name}'
            gcs_path = f'gs://{self.__STORAGE_INTEGRATION_BUCKET}/{data_source}/{dataset_name}/{file_name}'

            data_frame.to_parquet(gcs_path, compression='gzip', engine='pyarrow', index=False)
            print(f"Fichero correctamente almacenado en {gcs_path} con {len(data_frame)} filas")
            return gcs_path
            
        except Exception as e:
            msg = f"Error saving data to storage: {str(e)}"
            print(msg)
            raise Exception(msg)
    
    def getLatestSuccessExecution(self, dataset_name: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest successful execution data for incremental loads.
        
        Args:
            account: Client account identifier
            extractor_name: Name of the extractor/connector
            dataset_name: Name of the dataset
            
        Returns:
            Dictionary with the latest execution data, or None if not found
        """
        try:
            # Define the GCS path
            gcs_path = f"{self._extractor_name}/{dataset_name}/{self._m_account}.json"
            
            # Get the bucket
            bucket = self.storage_client.bucket(self.__STORAGE_LASTEST_INCREMENTAL_BUCKET)
            
            # Get the blob
            blob = bucket.blob(gcs_path)
            
            # Check if blob exists
            if not blob.exists():
                return None
            
            # Download and parse the JSON content
            content = blob.download_as_string()
            return json.loads(content)
            
        except Exception as e:
            # Log the error but don't fail - this is recoverable
            print(f"Warning: Could not get latest execution data: {str(e)}")
            return None
    
    def postLatestSuccessExecution(self, dataset_name: str, data: Dict[str, Any]) -> None:
        """
        Store the latest successful execution data for incremental loads.
        
        Args:
            account: Client account identifier
            extractor_name: Name of the extractor/connector
            dataset_name: Name of the dataset
            data: Dictionary with the execution data
        """
        try:
            # Define the GCS path
            gcs_path = f"{self._extractor_name}/{dataset_name}/{self._m_account}.json"
            
            # Get the bucket
            bucket = self.storage_client.bucket(self.__STORAGE_LASTEST_INCREMENTAL_BUCKET)
            
            # Create a temporary JSON file
            temp_file = "/tmp/temp_data.json"
            with open(temp_file, "w") as f:
                json.dump(data, f)
            
            # Upload to GCS
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(temp_file)
            
            # Clean up
            os.remove(temp_file)
            
        except Exception as e:
            msg = f"Error guardando el fichero con la última ejecución exitosa:{str(e)} "
            print(msg)
            raise Exception(msg)
    
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
        #print("Inicio del metodo de notificaciones")
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
            # future = self._putMessageInBus(topic_id=topic_id, data=data)
            msg = f"El método putNotificationFin ha finalizado en {self.__RETURN[1]}"
            print(msg)
        except Exception as e:
            msg=f'El método putNotification ha fallado: {str(e)}'
            print(msg)
            raise Exception(msg)
    
    def _putMessageInBus(self, topic_id, data):

        try:
            topic_path = self.publisher.topic_path(self.__GCP_PROJECT_ID, topic_id)
            future = self.publisher.publish(topic=topic_path, data=data)
            print(f"Se ha publicado un mensaje en el topic {topic_id} con el código: {future.result()}")
            return(self.__RETURN[1])
        except Exception as e:
            msg=f'El método _putMessageInBus ha fallado: {str(e)}'
            print(msg)
            raise Exception(msg)

    def _forceDataTypes(self, df: pd.DataFrame, dataset_name: str):
        """
        Aplica tipos de datos al DataFrame según el dataset
        
        Args:
            df (pandas.DataFrame): DataFrame a convertir
            dataset_name (str): Nombre del dataset
            
        Returns:
            pandas.DataFrame: DataFrame con tipos convertidos
        """
        try:
            __mapping_file_name = "data-type-mapping.json"
            # Obtener mapeo de tipos para este dataset
            current_dir = os.path.dirname(os.path.abspath(__file__))
            file_path = os.path.join(current_dir, __mapping_file_name)
            
            with open(file_path, 'r') as json_file:
                mapping_file = json.load(json_file)
            
            if dataset_name in mapping_file:
                ds_mapping = mapping_file[dataset_name]
            else:
                print(f"No hay mapeo de tipos de datos para el dataset {dataset_name}")
                return df
                
        except Exception as e:
            print(f"Error al buscar los mapeos: {str(e)}")
            return df  
        # Crear un diccionario de conversiones solo para columnas que existen en el DataFrame
        conversions = {col: dtype for col, dtype in ds_mapping.items() if col in df.columns}
        
        # Si no hay conversiones, devolver el DataFrame original
        if not conversions:
            return df
        
        # Aplicar conversiones de tipos
        print("Iniciamos el proceso de conversión de tipos")
        for col, dtype in conversions.items():
            print(f"Procesando columana {col} a tipo: {dtype}")
            try:
                # Manejar conversiones de tipos especiales
                if dtype.startswith('datetime'):
                    # Para campos de fecha, dejar como string para evitar problemas
                    df[col] = df[col].astype('string')
                elif dtype == 'float64':
                    # Para campos numéricos, convertir
                    try:
                        df[col] = df[col].astype(float).fillna(0.0)
                    except:
                        # Si falla, intentar con to_numeric para ser más flexible
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
                elif dtype == 'int64':
                    # Para enteros, convertir a entero pero manejar posibles NaN
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    # Llenar NaN con 0 para poder convertir a int64
                    df[col] = df[col].fillna(0).astype('int64')
                elif dtype == 'string':
                    df[col] = df[col].astype('string')
                elif dtype == 'boolean':
                    df[col] = df[col].convert_dtypes(convert_boolean=True)
                else:
                    # Conversión general de tipos
                    df[col] = df[col].astype(dtype)
            except Exception as e:
                print(f"Error al convertir columna {col} a {dtype}: {str(e)}")
        
        return df
