"""
EXPLICACION LÓGICA CONECTOR
Lógica general:
--------------
El conector de Business Central sigue el patron estandar de Mind:
- Se leen los datos de la fuente ya sea en incremental o total
- Se almacenan los datos en parquet en google cloud storage
- Es un conector multi-tenant donde cada cliente tiene sus credenciales y se acceden a sus datos
- Toda los metodos generales se almacenan en utils/controller.py 
- La condiguracion del conector estará almacenada en config.json

Lógica particular:
-----------------
Existen 4 tipos de cargas
    - Carga de Empresas (Companies) 
        --> Siempre es tipo Full Load
        --> Se lee en el constructor
        --> Siempore actualizamos el fichero parquet con las empresas
        --> Operamos con un dict en memoria para hacer llamadas a los otros datasets
        --> Se guarda un solo parquet con el resultado en formato companies_<_m_account>.parquet
    - Carga tipo:FullLoad para cada Empresa
        --> Se lee el endopoint 
        --> Se guarda los datos en un parquet temporal
        --> Si el resultado está paginado con el campo @odata.etag entonces se llama al siguiente endpoint 
        --> El resultado del siguiente endpoint se almacena en el parquet temporal
        --> Se guarda un solo parquet con el resultado en formato <dataset>_<_m_account>.parquet
    - Incremental para cada Empresa
        --> El campo incremental siempre es lastModifiedDateTime
        --> Se lee cuando fue la ultima carga, si no hay se basa en el start_date
        --> Siempre se va a intentar leer desde la ultima carga hasta ahora, asi para todas las Empresas
        --> El last load date es unico para todas las empresas ya que por integridad de datos siempre leeremos todas las empresas por debado del tiempo
        --> Si el resultado está paginado guardamos en parquet la pagina
        --> El parquet tendrá el siguiente formato <dataset>_<companyid>_<lastModifiedDateTimeMIN><lastModifiedDateTimeMAX>_<_m_account>.parquet
    - Incremental con Cabecera y Lineas para cada Empresa 
        --> El campo incremental siempre es lastModifiedDateTime
        --> Se lee cuando fue la ultima carga, si no hay se basa en el start_date
        --> Siempre se va a intentar leer desde la ultima carga hasta ahora, asi para todas las Empresas
        --> El last load date es unico para todas las empresas ya que por integridad de datos siempre leeremos todas las empresas por debado del tiempo
        --> Para cada linea de la cabecera se van leer las lineas que la componen
        --> Se van a guardar las lineas en el mismo registro de forma que siempre estarán asociados 
        --> Si el resultado está paginado guardamos un parquet para cada pagina
        --> El parquet tendrá el siguiente formato <dataset>_<companyid>_<lastModifiedDateTimeMIN><lastModifiedDateTimeMAX>_<_m_account>.parquet
Métodos que componen el modulo:
------------------------------
Clase Extraer
    __init__: contructor de la instancia
    getToken: Obtiene el token para interacturar con Business Central 
    getCompanies: Obtiene las companias, las almacena en GCS y las deja en un dic
    callEndPoint: Hace la llamada al endpoint y devuelve el resultado en json
    Iniciar: Valida la informacion sea correcta para iniciar la ejecución en paralelo de todos los datasets
    processDataSet: Procesa cada uno de los datasets 
        - Evalua la configuracion
        - Ejecuta carga en funcion del tipo


"""

import json, logging, requests, os, time
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple, Optional
import concurrent.futures
import msal
import pandas as pd
from utils.controller import Controller

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Extraer:
    def __init__(self, _m_account, start_date=None, end_date=None, companyIds=None):
        """
        Initialize the Business Central extractor
        
        Args:
            _m_account (str): Client account identifier
            start_date (str, optional): Start date for data extraction
            end_date (str, optional): End date for data extraction
            companies (list, optional): List of company IDs to filter data
        """
        self._m_account = _m_account
        # Initialize controller
        self.controller = Controller()
        
        # Load config.json utilizando una ruta relativa correcta
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(os.path.dirname(script_dir), 'config.json')
        
        with open(config_path, 'r') as config_file:
            self.config = json.load(config_file)
        
        # Get extractor name from config
        self.extractor_name = self.config.get("extractor_name")
        
        # Get credentials from Secret Manager
        secret_name = f"{_m_account}_{self.extractor_name}"
        self.credentials = self.controller.getSecret(secret_name)
               
        # Set extraction parameters
        self.start_date = start_date or self.credentials.get("start_date") or self.config.get("default_start_date")
        self.end_date = end_date or datetime.now().strftime('%Y-%m-%d')
        
        # Set API config and parameters
        self.api_config = self.config.get("api_config", {})
        self.api_version = self.api_config.get("version")
        
        # Set base URL with tenant_id and environment
        self.base_url = self.api_config.get("base_url", "").format(
            tenant_id=self.credentials.get("tenant_id"),
            environment=self.credentials.get("environment")
        )
        
        # Set default parameters
        self.default_params = self.config.get("default_params", {})
        self.page_size = self.default_params.get("page_size", 20000)
        self.max_retries = self.default_params.get("max_retries", 3)
        self.timeout = self.default_params.get("timeout", 300)
        

        # Get access token
        token_result = self.getToken(
            self.credentials.get("tenant_id"),
            self.credentials.get("client_id"),
            self.credentials.get("client_secret")
        )
        self.access_token = token_result.get('access_token')
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        # Set companies information 
        if companyIds is None or companyIds == [] :
            self.companies = self.getCompanyIds()
        else:
            self.companies = companyIds
        # Para Fergus, eliminamos TEST FERGUS si existe
        test_company_id = "b06baaa1-7d4a-ef11-a316-000d3a672b9e"
        if test_company_id in self.companies:
            self.companies.remove(test_company_id)
        

        
    def getToken(self, tenant, client_id, client_secret):
        """
        Get access token for Business Central API using MSAL
        
        Args:
            tenant (str): Tenant ID
            client_id (str): Client ID
            client_secret (str): Client secret
            
        Returns:
            dict: Token response containing access_token
        """
        # Get authority and scope from config
        auth_config = self.api_config.get("auth", {})
        authority_url_template = auth_config.get("authority_url", "https://login.microsoftonline.com/{tenant_id}")
        authority = authority_url_template.replace("{tenant_id}", tenant)
        
        scope = auth_config.get("scope", ["https://api.businesscentral.dynamics.com/.default"])

        app = msal.ConfidentialClientApplication(client_id, authority=authority, client_credential=client_secret)
        
        try:
            accessToken = app.acquire_token_for_client(scopes=scope)
            if accessToken.get('access_token'):
                logger.info('New access token retrieved....')
            else:
                error_msg = f"Error acquiring authorization token: {accessToken.get('error_description', 'Unknown error')}"
                logger.error(error_msg)
                raise Exception(error_msg)
        except Exception as err:
            logger.error(f"Exception during token acquisition: {str(err)}")
            raise
        
        return accessToken
    
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
            
            response = session.get(url=url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error al llamar al endpoint {url}: {e}")
            # Si el token expiró, intentar renovarlo
            if response.status_code == 401:
                logger.info("Renovando token")
                self.access_token = self.getToken(
                    self.credentials.get("tenant_id"),
                    self.credentials.get("client_id"),
                    self.credentials.get("client_secret")
                )
                return self.callEndPoint(url, params)
            raise
    
    def getCompanyIds(self) -> Dict[str, Dict[str, Any]]:
        """
        Obtiene las empresas de Business Central, las almacena en GCS y las deja en un diccionario
        
        Returns:
            Diccionario con las empresas
        """
        dataset = 'companies'
        file_name = dataset+"_"+ self._m_account +'.parquet'
        response = self.callEndPoint(f"{self.base_url}/{dataset}")
        result = response.get('value', [])
        df = pd.DataFrame(result)
        self.controller.saveStorage(self._m_account, self.extractor_name, dataset, file_name, df)
        return df['id'].to_list()
    
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
        is_incremental = dataset_config.get('incremental', False)
        is_parent_child = dataset_config.get('parentchild', False) 

        if not is_incremental:
            return self.processFullLoadDataset(dataset, dataset_config)
        if is_incremental and not is_parent_child:
            return self.processIncrementalDataset(dataset, dataset_config)
        if is_incremental and is_parent_child:
            return self.processIncrementalParentChild(dataset, dataset_config)
        else:
            logger.error(f"Tipo de carga desconocido")
            return False

    def processFullLoadDataset(self,  dataset, dataset_config: Dict[str, Any]) -> bool:
        """
        Procesa un dataset con carga completa
        
        Args:
            dataset_config: Configuración del dataset
            
        Returns:
            True si el procesamiento fue exitoso, False en caso contrario
        """
        dataset_name = dataset
        endpoint = dataset_config['endpoint']
        
        logger.info(f"Procesando carga completa para {dataset_name}")
        
        # Procesar cada empresa
        for companyId in self.companies:
            logger.info(f"Procesando empresa {companyId} para {dataset_name}")
            # Construir URL
            url = self.base_url+'/'f"{str(endpoint).format(companyId=companyId)}"

            def doFullLoadSteps(url, page):
                # Realizar primera llamada
                response_data = self.callEndPoint(url)
                company_data = response_data['value']
                df = pd.DataFrame(company_data)

                if not df.empty:
                    df['companyId'] = companyId
                    filename = f"{dataset_name}_{companyId}_{page}_{self._m_account}.parquet"
                    self.controller.saveStorage(self._m_account, self.extractor_name, dataset_name, filename, df)
                    logger.info(f"Se guardaron {len(df)} registros para {dataset_name} en empresa {companyId}")
                else:
                    logger.warning(f"No hay datos para {dataset_name} en empresa {companyId}")
                
                return response_data
            
            # Realizar primera llamada
            page_count=1
            response_data = doFullLoadSteps(url,page_count)
            if not response_data or 'value' not in response_data or not response_data['value']:
                logger.warning(f"No hay datos para {dataset_name} en empresa {companyId}")
            else:
                # Verificar si hay más páginas
                while '@odata.nextLink' in response_data:
                    logger.info(f"Procesando página {page_count + 1} para {dataset_name} en empresa {companyId}")
                    # Llamar a la siguiente página
                    response_data = doFullLoadSteps(response_data['@odata.nextLink'],page_count+1)

                    if not response_data or 'value' not in response_data or not response_data['value']:
                        break
                    page_count += 1
        
        return True
        
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
        
        logger.info(f"Procesando carga incremental para {dataset_name}")
        
        # Procesar cada empresa
        for companyId in self.companies:
            logger.info(f"Procesando empresa {companyId} para {dataset_name}")
            
            # Determinar fecha de inicio
            last_load_date = None
            last_load_date_aux = self.controller.getLastestSucccessExecution(self.extractor_name, dataset_name, self._m_account, companyId)
            if not last_load_date_aux is None:
                last_load_date = last_load_date_aux['incremental_value']
                print(f"last_load_date: {last_load_date}")
            
            start_date = last_load_date if last_load_date else self.start_date
            logger.info(f"Última carga: {start_date}")
            # Convertir a formato ISO 8601 para OData
            start_date_formatted = datetime.fromisoformat(str(start_date)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            end_date_formatted = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            
            # Construir URL con filtro de fecha
            filter_query = f"$filter=lastModifiedDateTime gt {start_date_formatted} and lastModifiedDateTime le {end_date_formatted}"
            url = self.base_url+'/'f"{str(endpoint).format(companyId=companyId)}?{filter_query}"
            
            def doIncrementalSteps(url):
                 # Inicializar variables para seguimiento de min/max
                min_date = None
                max_date = None

                response_data = self.callEndPoint(url)
                company_data = response_data['value']
                df = pd.DataFrame(company_data)
                if not df.empty:
                    df['companyId'] = companyId
                    min_date = datetime.fromisoformat(min(df['lastModifiedDateTime']).replace('Z', ''))
                    max_date = datetime.fromisoformat(max(df['lastModifiedDateTime']).replace('Z', ''))
                    
                    # Guardamos el valor y actualizamos en parquet la tabla 
                    min_date_str = min_date.strftime("%Y%m%d%H%M%S")
                    max_date_str = max_date.strftime("%Y%m%d%H%M%S")
                        
                    filename = f"{dataset_name}_{companyId}_{min_date_str}_{max_date_str}_{self._m_account}.parquet"
                    self.controller.saveStorage(self._m_account, self.extractor_name, dataset_name, filename, df)
                    logger.info(f"Se guardaron {len(df)} registros para {dataset_name} en empresa {companyId}")
                # Actualizar fecha de última carga
                self.controller.postLastestSucccessExecution(self._m_account, self.extractor_name, dataset_name, end_date_formatted, companyId)
                
                return response_data

            # Realizar primera llamada
            response_data = doIncrementalSteps(url)
            if not response_data or 'value' not in response_data or not response_data['value']:
                logger.warning(f"No hay datos incrementales para {dataset_name} en empresa {companyId}")
            else:    
                # Verificar si hay más páginas
                page_count = 1
                while '@odata.nextLink' in response_data:
                    logger.info(f"Procesando página {page_count + 1} para {dataset_name} en empresa {companyId}")
                    # Llamar a la siguiente página
                    response_data = doIncrementalSteps(response_data['@odata.nextLink'])
                    
                    if not response_data or 'value' not in response_data or not response_data['value']:
                        break
                    page_count += 1
        return True
    
    def processIncrementalParentChild(self, dataset, dataset_config: Dict[str, Any]) -> bool:
        """
        Procesa un dataset con carga incremental que incluye cabeceras y líneas
        
        Args:
            dataset_config: Configuración del dataset
            
        Returns:
            True si el procesamiento fue exitoso, False en caso contrario
        """
        
        dataset_name = dataset
        logger.info(f"Procesando carga incremental con cabeceras y líneas para {dataset_name}")
        
        parent_endpoint = dataset_config['endpoint']
        child_endpoint = dataset_config['childendpoint']
        parent_id_field = dataset_config.get('parentfield', 'id')
        child_field = dataset_config.get('childfield', 'child')
        
        for companyId in self.companies:
            logger.info(f"Procesando empresa {companyId} para {dataset_name}")
            
            # Determinar fecha de inicio
            last_load_date = None
            last_load_date_aux = self.controller.getLastestSucccessExecution(self.extractor_name, dataset_name, self._m_account, companyId)
            if not last_load_date_aux is None:
                last_load_date = last_load_date_aux['incremental_value']
                print(f"last_load_date: {last_load_date}")
            
            start_date = last_load_date if last_load_date else self.start_date
            logger.info(f"Última carga: {start_date}")
            # Convertir a formato ISO 8601 para OData
            start_date_formatted = datetime.fromisoformat(str(start_date)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            end_date_formatted = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            
            # Construir URL con filtro de fecha
            filter_query = f"$filter=lastModifiedDateTime gt {start_date_formatted} and lastModifiedDateTime le {end_date_formatted}"
            url = self.base_url+'/'f"{str(parent_endpoint).format(companyId=companyId)}?{filter_query}"
            
            def doIncrementalParentChildSteps(url):
                 # Inicializar variables para seguimiento de min/max
                min_date = None
                max_date = None

                response_data = self.callEndPoint(url)
                company_data = response_data['value']
                df = pd.DataFrame(company_data)
                # añadir un campo con el nombre del endpoint hijo
                company_data_with_lines=[]
                def process_parent(parent):
                    child_url = self.base_url + '/' + str(child_endpoint).format(companyId=companyId, parentId=parent[parent_id_field])
                    child_response_data = self.callEndPoint(child_url)
                    child_data = child_response_data['value']
                    new_child_data = []
                    for linea in child_data:
                        linea['_odata_etag'] = linea.pop('@odata.etag')
                        new_child_data.append(linea)
                    
                    parent_copy = parent.copy()  # Evitar modificar el original
                    parent_copy[child_field] = new_child_data
                    return parent_copy
                 # Usar ThreadPoolExecutor para paralelizar las llamadas API
                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                    logger.info(f"Procesando {len(company_data)} registros en paralelo")
                    company_data_with_lines = list(executor.map(process_parent, company_data))
                
                # Crear DataFrame una sola vez
                df = pd.DataFrame(company_data_with_lines)
                
                if not df.empty:
                    df['companyId'] = companyId
                    min_date = datetime.fromisoformat(min(df['lastModifiedDateTime']).replace('Z', ''))
                    max_date = datetime.fromisoformat(max(df['lastModifiedDateTime']).replace('Z', ''))
                    
                    # Guardamos el valor y actualizamos en parquet la tabla 
                    min_date_str = min_date.strftime("%Y%m%d%H%M%S")
                    max_date_str = max_date.strftime("%Y%m%d%H%M%S")
                        
                    filename = f"{dataset_name}_{companyId}_{min_date_str}_{max_date_str}_{self._m_account}.parquet"
                    self.controller.saveStorage(self._m_account, self.extractor_name, dataset_name, filename, df)
                    logger.info(f"Se guardaron {len(df)} registros para {dataset_name} en empresa {companyId}")
                # Actualizar fecha de última carga
                self.controller.postLastestSucccessExecution(self._m_account, self.extractor_name, dataset_name, end_date_formatted, companyId)
                
                return response_data

            # Realizar primera llamada
            response_data = doIncrementalParentChildSteps(url)
            if not response_data or 'value' not in response_data or not response_data['value']:
                logger.warning(f"No hay datos incrementales para {dataset_name} en empresa {companyId}")
            else:    
                # Verificar si hay más páginas
                page_count = 1
                while '@odata.nextLink' in response_data:
                    logger.info(f"Procesando página {page_count + 1} para {dataset_name} en empresa {companyId}")
                    # Llamar a la siguiente página
                    response_data = doIncrementalParentChildSteps(response_data['@odata.nextLink'])
                    
                    if not response_data or 'value' not in response_data or not response_data['value']:
                        break
                    page_count += 1
        return True