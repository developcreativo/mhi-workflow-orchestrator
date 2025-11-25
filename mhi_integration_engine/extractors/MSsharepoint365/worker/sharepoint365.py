"""
CONECTOR SHAREPOINT 365

Lógica general:
--------------
- Este conector lee una pestaña de excel y lo guarda en la carpeta de ocean_data/int_sharepoint con el siguiente formato:
- <nombre_fichero>_<nombre_pestaña>_<_m_account>.parquet
- Lee de la celda A1 hasta que no encuentra datos, lo transforma en una tabla de pyarrow
- Siempre es full load y reemplaza los datos con lo ultimo qu encuentra

La conexion: (Secret en Secret Manager) 
---------------------------------------
El secret manager contendrá un json con:
{
"client_id":"fdlmgjfdg543n5k43guslfdddss--d",
"client_secret":"fjowerj23045u293405u2-34rjfsdñlfw",
"base_url": https://mindanalytics.sharepoint.com/MIND_INTERNAL_DATA
}

La Carga: (PubSub en scheduler) 
---------------------------------------
El mensaje a pubsub contendrá un json con:
{
    "_m_account":"mind",
    "connection_id":"mind_sharepoint_MIND_INTERNAL_DATA",
    "datasets":
        "presupuesto2024":{
                            "path": <ruta_a_fichero>,
                            "tabs":["pestaña1","pestaña2"]
                            },
        "presupuesto2025":{
                            "path": <ruta_a_fichero>,
                            "tabs":["pestaña1","pestaña2"]
                            }
}
"""

import json, logging, requests, os, base64
from requests.adapters import HTTPAdapter, Retry
from typing import Dict, List, Any
import concurrent.futures
import pandas as pd
from utils.controller import Controller

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Extraer:
    def __init__(self, _m_account, start_date=None, end_date=None, companyIds=None):
        """
        Initialize the Sharepoint365 Extractor
        
        Args:
            _m_account (str): Client account identifier
        """
        logger.info("Iniciando la clase Extraer")
        self._m_account = _m_account
        
        # Load config.json utilizando una ruta relativa correcta
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(os.path.dirname(script_dir), 'config.json')
        
        with open(config_path, 'r') as config_file:
            self.config = json.load(config_file)
        
        # Get extractor name from config
        self.extractor_name = self.config.get("extractor_name")
        
        # Initialize controller
        self.controller = Controller(account=self._m_account, extractor_name=self.extractor_name)
        
        # Get credentials from Secret Manager
        secret_name = f"{_m_account}_{self.extractor_name}"
        self.credentials = self.controller.getSecret(secret_name)
    
        # Set API config and parameters
        self.api_config = self.config.get("api_config", {})
        self.api_version = self.api_config.get("version")

        
        # Set base URL 
        self.base_url = self.credentials.get("base_url", "")
        
        # Set default parameters
        self.default_params = self.config.get("default_params", {})
        self.page_size = self.default_params.get("page_size", 20000)
        self.max_retries = self.default_params.get("max_retries", 3)
        self.timeout = self.default_params.get("timeout", 300)
        
        # Preparamos la seguridad
        scope_url = self.config.get("scope_url")
        grant_type = self.config.get("grant_type")
        content_type =self.config.get("content_type") 
        client_id = self.credentials.get("client_id", "")
        client_secret = self.credentials.get("client_secret", "")
        
        self.header_token = {
            "content_type": content_type
        }
        self.token_url = self.config.get("token_url")
        self.token_data = {
            'client_id': client_id,
            'scope': scope_url,
            'grant_type': grant_type,
            'client_secret': client_secret
        }
        token = self.getToken()
        # Esta header esta instanciada para interactuar con microsoft graph y office 365
        self.header = {
            "Authorization": token,
            "Content-type": "application/json"
        }   

        logger.info("Finalizada la inicialiación de la clase Sharepoint365")

    def _getToken(self):
        # This functions returns the token that is needed to make queries to Microsft Graph:
        response = requests.get(self.token_url, self.header_token, data=self.token_data)
        if response.status_code != 200:
            print("Couldn't obtain token from microsoft graph API authentication when executing office365.py script")
        data = json.loads(response.content)
        token = data["token_type"] + " " + data["access_token"]
        return token
    
    def callEndPoint(self, url: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Realiza una llamada a un endpoint de la API de Business Central
        
        Args:
            url: URL del endpoint
            params: Parámetros adicionales para la llamada
            
        Returns:
            Respuesta del endpoint en formato JSON
        """
        try:
            retry_strategy = Retry(
                total=self.max_retries,  # Maximum number of retries
                backoff_factor=2, # Time to wait between retries
                status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
            )

            # Create an HTTP adapter with the retry strategy and mount it to session
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session = requests.Session()
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            response = session.get(url=url, headers=self.headers, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Error al llamar al endpoint {url}: {e}")
            
    
    def Iniciar(self, datasets: List[str] = None) -> bool:
        """
        Inicia la extracción de datos
        
        Args:
            datasets_to_process: Lista de nombres de datasets a procesar. Si es None, se procesan todos.
        
        Returns:
            True si la extracción fue exitosa, False en caso contrario
        """
        logger.info("Iniciando extracción de datos")
        
        # Validar que haya empresas
        if not self.companies:
            logger.error("No hay empresas para procesar")
            return False
        
        # Si no se especifican datasets, procesar todos
        if datasets is None:
            logger.error(f"No hay datasets a procesar")
            return False
        logger.info(f"Se procesarán los siguientes datasets: {[d for d in datasets]}")
        
        # Procesar cada dataset en paralelo
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(self.processDataSet, datasets))
    
            for i, result in enumerate(results):
                try:
                    if result:
                        logger.info(f"Dataset {datasets[i]} procesado correctamente con los siguientes resultados")
                    else:
                        raise Exception
                except Exception as e:
                    logger.error(f"Error al procesar dataset {datasets[i]}: {e}")
                    return False
        return True
    
    def processDataSet(self, dataset: str) -> bool:
        """
        Procesa un dataset específico
        
        Args:
            dataset_name: Nombre del dataset a procesar
            
        Returns:
            True si el procesamiento fue exitoso, False en caso contrario
        """
        logger.info(f"Procesando dataset {dataset}")
        
        # Obtener configuración del dataset
        dataset_config = dict(self.config['datasets'][dataset])
        
        if not dataset_config:
            logger.error(f"No se encontró configuración para el dataset {dataset}")
            return False

        # Determinar el tipo de carga
        is_paginated = dataset_config.get('pages_endpoint', False) 
        
        if not is_paginated:
            return self.processIncrementalDataset(dataset, dataset_config)
        if is_paginated:
            return self.processIncrementalPaginatedDataset(dataset, dataset_config)
        else:
            logger.error(f"Tipo de carga desconocido")
            return False


    def processIncrementalDataset(self, dataset, dataset_config: Dict[str, Any]) -> bool:
        """
        Procesa un dataset con carga incremental
        
        Args:
            dataset_config: Configuración del dataset
            
        Returns:
            True si el procesamiento fue exitoso, False en caso contrario
        """
        dataset_name = dataset
        endpoint = dataset_config['endpoint']
        requires_empcode = dataset_config['requires_empcode']
        date_updated = None
        
        logger.info(f"Procesando carga incremental para {dataset_name}")
        
        date_updated_aux = self.controller.getLatestSuccessExecution(dataset_name)
        if not date_updated_aux is None:
            date_updated = date_updated_aux['incremental_value']
           
        start_date = date_updated if date_updated else self.start_date
        url = f'{self.base_url}/{endpoint}'

        if requires_empcode:
             # Construir los params
            for companyId in self.companies:
                logger.info(f"Procesando empresa {companyId} para {dataset_name}")
                params={
                    "date_updated": start_date,
                    "empcode":companyId
                }    
                response_data = self.callEndPoint(url, params=params)
                df = pd.DataFrame(response_data)
                if not df.empty:
                    # Guardamos el valor y actualizamos en parquet la tabla
                    filename = f"{dataset_name}_{companyId}_{start_date}_{self.end_date}_{self._m_account}.parquet"
                    self.controller.saveStorage(dataset_name, filename, df)
                    logger.info(f"Se guardaron {len(df)} registros para el dataset {dataset_name}")
            incremental_data = {
                "incremental_field": "date_updated",
                "incremental_value": self.end_date
            }
            self.controller.postLatestSuccessExecution(dataset_name=dataset_name, data=incremental_data)           
        else:
            params={
                "date_updated":start_date
            }
            response_data = self.callEndPoint(url, params=params)
            df = pd.DataFrame(response_data)
            if not df.empty:
                # Guardamos el valor y actualizamos en parquet la tabla
                filename = f"{dataset_name}_{start_date}_{self.end_date}_{self._m_account}.parquet"
                self.controller.saveStorage(dataset_name, filename, df)
                logger.info(f"Se guardaron {len(df)} registros para el dataset {dataset_name}")
            incremental_data = {
                        "incremental_field": "date_updated",
                        "incremental_value": self.end_date
                    }
            self.controller.postLatestSuccessExecution(dataset_name=dataset_name, data=incremental_data)
                    
        if not response_data:
            logger.warning(f"No hay datos incrementales para el dataset {dataset_name}")
        return True


    def processIncrementalPaginatedDataset(self, dataset, dataset_config: Dict[str, Any]) -> bool:
        """
        Procesa un dataset con carga incremental paginando con un end point de paginacion
        Args:
            dataset: Dataset a cargar
            dataset_config: Configuración del dataset
            
        Returns:
            True si el procesamiento fue exitoso, False en caso contrario
        """
        logger.info(f"Procesando carga incremental para {dataset}")
        
        dataset_name = dataset
        endpoint = dataset_config['endpoint']
        pages_endpoint = dataset_config['pages_endpoint']
       
        requires_empcode = dataset_config['requires_empcode']
        date_updated = None
        date_updated_aux = self.controller.getLatestSuccessExecution(dataset_name)
        if not date_updated_aux is None:
            date_updated = date_updated_aux['incremental_value']
           
        start_date = date_updated if date_updated else self.start_date
        
        url = f'{self.base_url}/{endpoint}'

        if requires_empcode:
            # Construir los params
            for companyId in self.companies:
                # Obtenemos las paginas a iterar
                params_pages={
                    "date_updated": start_date,
                    "empcode":companyId
                }
                url_pages = f'{self.base_url}/{pages_endpoint}'
                try:
                    response_pages = requests.get(url=url_pages, headers=self.headers, params=params_pages)
                except BaseException as e:
                    logger.error(f"No se encontraron las paginas para la companyId {companyId} del datset {dataset_name}")
                    raise e
                paginas = json.loads(response_pages.content.decode("utf-8"))
                paginas=paginas[0]["pages"]
                for page in range(1, paginas+1):
                    params={
                        "date_updated":start_date,
                        "empcode":companyId,
                        "page":page
                    }
                    response_data = self.callEndPoint(url, params=params)
                    df = pd.DataFrame(response_data)
                    if not df.empty:
                        # Guardamos el valor y actualizamos en parquet la tabla
                        filename = f"{dataset_name}_{companyId}_{start_date}_{self.end_date}_page{page}_{self._m_account}.parquet"
                        self.controller.saveStorage(dataset_name, filename, df)
                        logger.info(f"Se guardaron {len(df)} registros para la companyId {companyId} del dataset {dataset_name}")
            incremental_data = {
                        "incremental_field": "date_updated",
                        "incremental_value": self.end_date
                    }
            self.controller.postLatestSuccessExecution(dataset_name=dataset_name, data=incremental_data)         
        else:
            # Obtenemos las paginas a iterar
            params_pages={
                "date_updated": start_date,
            }
            url_pages = f'{self.base_url}/{pages_endpoint}'
            try:
                response_pages = requests.get(url=url_pages, headers=self.headers, params=params_pages)
            except BaseException as e:
                logger.error(f"No se encontraron las paginas para el datset {dataset_name}")
                raise e
            paginas = json.loads(response_pages.content.decode("utf-8"))
            paginas=paginas[0]["pages"]
            for page in range(1, paginas+1):
                params={
                    "date_updated":start_date,
                    "page":page
                }
                response_data = self.callEndPoint(url, params=params)
                df = pd.DataFrame(response_data)
                if not df.empty:
                    # Guardamos el valor y actualizamos en parquet la tabla
                    filename = f"{dataset_name}_{start_date}_{self.end_date}_page{page}_{self._m_account}.parquet"
                    self.controller.saveStorage(dataset_name, filename, df)
                    logger.info(f"Se guardaron {len(df)} registros para el dataset {dataset_name}")
            incremental_data = {
                "incremental_field": "date_updated",
                "incremental_value": self.end_date
            }
            self.controller.postLatestSuccessExecution(dataset_name=dataset_name, data=incremental_data)

        if not response_data:
            logger.warning(f"No hay datos incrementales para el dataset {dataset_name}")
        return True