"""
CONECTOR AXIONAL - VERSIÓN CORREGIDA
=====================================

Lógica general:
--------------
- Entidad principal: Empresa
- Primero se obtienen todos los empresas y se almacenan como empresas/empresas_<_m_account>.parquet
- Todos los endpoints son incrementales
- Si el dataset tiene la configuracion requires_empcode = true, se evalua para cada empresa
- Se almacenan en los datos en parquet y en los casos incrementales se actualiza la ultima incrementalidad

Lógica incremental:
------------------
- Todas los datasets tienen el campo date_updated por lo que todos los datasets se consideran incrementales
- El dataset ApuntesContables fue modificado y no tiene date_updated, en su lugar tiene date_ini y date_fin
"""

import json, logging, requests, os, base64
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime,timedelta
from typing import Dict, List, Any
import concurrent.futures
import pandas as pd
from utils.controller import Controller

# Set up logging
#logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class Extraer:
    def __init__(self, _m_account, start_date=None, end_date=None, companyIds=None):
        """
        Initialize the Axional Extractor
        
        Args:
            _m_account (str): Client account identifier
            start_date (str, optional): Start date for data extraction
            end_date (str, optional): End date for data extraction
            companyIds (list, optional): List of company IDs to filter data
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
    
        # Set extraction parameters
        self.start_date = start_date or self.credentials.get("start_date") or self.config.get("default_start_date")
        self.end_date = end_date or (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
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
        
        user = self.credentials.get("user", "")
        password = self.credentials.get("password", "")
        credentials = f'{user}:{password}'
        b = base64.b64encode(bytes(credentials,'utf-8'))
        base64_str = b.decode('utf-8')

        self.headers = {
            "Authorization": f"Basic {base64_str}",
            "Accept": "application/json"
        }
        
        # Set companies information 
        if companyIds is None or companyIds == [] :
            self.companies = self.getCompanyIds()
        else:
            self.companies = companyIds
        logger.info("Finalizada la inicialiación de la clase Axional")

    
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
            
    
    def getCompanyIds(self) -> Dict[str, Dict[str, Any]]:
        """
        Obtiene las empresas de Axional, las almacena en GCS y las deja en un diccionario
        
        Returns:
            Diccionario con las empresas
        """
        dataset = 'Empresas'
        datasets  = self.config.get("datasets", {})
        endpoint  = datasets[dataset]["endpoint"]
       
        params = {"date_updated":self.start_date}
        try:
            response = self.callEndPoint(f"{self.base_url}/{endpoint}", params=params)
        except Exception as e:
            logger.error(f"Error en extraer las compañías: {e}")
        df = pd.DataFrame(response)

        file_name = dataset+"_"+ self._m_account +'.parquet'
        self.controller.saveStorage(
            dataset_name = dataset,
             file_name= file_name,
             data_frame= df)
        empcode_list = [item["empcode"] for item in response if "empcode" in item]
        return empcode_list
        

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
        
        if dataset== 'ApuntesContables':
            return self.processApuntesContablesDataset(dataset, dataset_config)
        if not is_paginated:
            return self.processIncrementalDataset(dataset, dataset_config)
        if is_paginated:
            return self.processIncrementalPaginatedDataset(dataset, dataset_config)
        else:
            logger.error(f"Tipo de carga desconocido")
            return False

    def processApuntesContablesDataset(self, dataset, dataset_config) -> bool:
        try:
            logger.info(f"Procesando carga incremental para {dataset}")
            today = datetime.today()
            #limit_date = datetime(today.year, 10, 31)
            limit_date = datetime(today.year, 8, 31)
            years_to_load = []
            if today < limit_date:
                #years_to_load.append(today.year-3)
                #years_to_load.append(today.year-2)
                years_to_load.append(today.year-1)
                years_to_load.append(today.year)
            else:
                years_to_load.append(today.year)

            for year_to_load in years_to_load:
                logger.info(f"---------------------------------------------")
                logger.info(f"------  Procesando Año {year_to_load} -------")
                logger.info(f"---------------------------------------------")
                chunks: List[pd.DataFrame] = []
                start_date = f"{year_to_load}-01-01"
                end_date = f"{year_to_load}-12-31"
                annual_records_axional=0
                for company in self.companies:
                    company_records_mind=0
                    logger.info(f"Procesando Empresa: {company}")
                    base_params = {"empcode": company, "date_ini": start_date, "date_fin": end_date}
                    pages_endpoint = dataset_config['pages_endpoint']
                    url_pages = f'{self.base_url}/{pages_endpoint}'
                    try:
                        response_pages = requests.get(url=url_pages, headers=self.headers, params=base_params)
                    except BaseException as e:
                        logger.error(f"No se encontraron las paginas para la company {company} del datset {dataset}")
                        raise e
                    pagination = json.loads(response_pages.content.decode("utf-8"))
                    pages=pagination[0]["pages"]+1
                    registers=pagination[0]["registers"]
                    annual_records_axional += registers
                    logger.info(f"  Empresa {company} con {pages} páginas y {registers} registros")
                    if registers == 0:
                        logger.warning(f"       Empresa con 0 registros no se procesa")
                    else:
                        for page in range(0, pages):
                            logger.info(f"      Procesando Página {page}/{pages}")
                            paged_params = {**base_params, "page": page}
                            endpoint = dataset_config['endpoint']
                            url = f'{self.base_url}/{endpoint}'
                            payload = self.callEndPoint(url,paged_params)
                            df_page = pd.DataFrame(payload)
                            # Metemos un warning si hay discrepancias:
                            company_records_mind += len(df_page)
                            logger.info(f"      Obtenidos {len(df_page)} registros de la página {page}/{pages}")
                            if df_page.empty:
                                continue
                            else:
                                chunks.append(df_page)
                        if company_records_mind != registers:
                            logger.warning(f"       Atencion {company_records_mind} y los registros del endpoint({registers}) son diferentes")
                        
                if chunks:
                    resultado = pd.concat(chunks, ignore_index=True, sort=False)
                else:
                    resultado = pd.DataFrame()  
                
                filename = f"{dataset}_{start_date}_{end_date}_{self._m_account}.parquet"
                self.controller.saveStorage(dataset, filename, resultado)
                logger.info(f"Se guardaron {len(resultado)}/{annual_records_axional} registros para el año {year_to_load} del dataset {dataset}")
            return True
        except Exception as e:
            logger.warning(f"Error extrayendo el dataset {dataset}")


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
        df = pd.DataFrame()
        response_data ={}
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
                
                try:
                    response_data = self.callEndPoint(url, params=params)
                    df = pd.DataFrame(response_data)
                except Exception as e:
                    logger.warning(f"La consulta a la url {url} no devolvio datos")
                    
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
            df = pd.DataFrame()
            try:
                response_data = self.callEndPoint(url, params=params)
                df = pd.DataFrame(response_data)
            except Exception as e:
                logger.warning(f"La consulta a la url {url} no devolvio datos")

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
        df = pd.DataFrame()
        response_data ={}
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
                    try:
                        response_data = self.callEndPoint(url, params=params)
                        df = pd.DataFrame(response_data)
                    except Exception as e:
                        logger.warning(f"la consulta a la url {url} no devolvió datos")
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
    
    def _generate_filename(self, dataset_name: str, params: Dict[str, Any]) -> str:
        """
        Genera el nombre del archivo basado en los parámetros
        
        Args:
            dataset_name: Nombre del dataset
            params: Parámetros para generar el nombre
            
        Returns:
            Nombre del archivo generado
        """
        parts = [dataset_name]
        
        if params.get("company_id"):
            parts.append(params["company_id"])
        
        if params.get("start_date"):
            parts.append(params["start_date"])
        
        if params.get("end_date"):
            parts.append(params["end_date"])
        
        if params.get("page"):
            parts.append(f"page{params['page']}")
        
        parts.append(self.account)
        
        return f"{'_'.join(parts)}.parquet"