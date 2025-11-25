import json,logging,os,requests,threading,faulthandler, time
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter, Retry
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from utils.controller import Controller

# Habilita volcado de fallos nativos
faulthandler.enable()

# Set up logging
#logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class Extraer:
    def __init__(self, 
                _m_account: str,
                connection_id: str,
                extractor_name:str,
                flow_id: str = None,
                run_id: str = None,
                task_id: str = None,
                start_date: str = None,
                end_date: str = None,
                events: str = []):
        """
        Inicializa el extractor de Forvenues
        """
        try:
            self._m_account = _m_account
            self.connection_id = connection_id
            self.extractor_name = extractor_name
            self.flow_id=flow_id
            self.run_id=run_id
            self.task_id=task_id
            self.events = events
            # Cargar configuración
            self._load_config()

            # Inicializar controller y credenciales
            self.controller = Controller(account=_m_account, 
                                        connection_id=connection_id, 
                                        extractor_name=extractor_name,
                                        flow_id=self.flow_id,
                                        run_id=self.run_id,
                                        task_id=self.task_id)
            self._load_credentials(connection_id)

            # Paso 0: Inicialización
            self._initialize_dates(start_date,end_date)

            # Configurar API (thread-safe)
            self._setup_api()

            # Lock para escrituras (parquet/IO)
            self._write_lock = threading.Lock()
        except Exception as e:
            msg='El proceso de instanciación de fourvenues ha fallado'
            logger.error(msg)
            raise msg
    # ---------------------------
    # Setup & helpers
    # ---------------------------
    def _load_config(self):
        """Carga la configuración desde config.json"""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(os.path.dirname(script_dir), "config.json")
        with open(config_path, "r") as config_file:
            self.config = json.load(config_file)

        self.extractor_name = self.config.get("extractor_name")
        self.default_params = self.config.get("default_params", {})

        # Parámetros de control de carga
        self.page_size = self.default_params.get("page_size", 100)
        self.max_retries = self.default_params.get("max_retries", 3)
        self.timeout = self.default_params.get("timeout", 300)
        self.max_workers = self.default_params.get("max_workers", 5)
        self.max_event_workers = self.default_params.get("max_event_workers", 10)
        self.data_field = self.default_params.get("data_field", "data")
        

    def _load_credentials(self, connection_id:str):
        """Carga las credenciales desde Secret Manager"""
        secret_json = self.controller.getSecret(connection_id)
        self.credentials = json.loads(secret_json) if isinstance(secret_json, str) else secret_json

    def _initialize_dates(self,start_date:str, end_date:str):
        """
        Exclusivo para incrementales: para incluir HOY se consulta hasta MAÑANA porque siempre concatena las hora a las 00:00:00
        En cargas incrementales siempre se ha de cargar ayer y hoy
        """
        self.current_date = datetime.now()
        self.extraction_date = self.current_date.strftime("%Y-%m-%d") 
        self.today = (self.current_date + timedelta(days=-1)).strftime("%Y-%m-%d")   # inclusivo
        self.end_exclusive = (self.current_date + timedelta(days=1)).strftime("%Y-%m-%d")  # exclusivo
        self.timestamp = self.current_date.strftime("%Y%m%d%H%M%S")

        # Leer última lectura de eventos
        print("------------------------")
        if start_date is None:
            print("Start Date is none")
            last_events_read = self.controller.getLatestSuccessExecution("events_last_read")
            print(f"last_events_read:{last_events_read}")
            if last_events_read and last_events_read.get("next_start_date"):
                self.events_start_date = last_events_read.get("next_start_date")
            else:
                self.events_start_date = self.credentials.get("start_date") or self.config.get("default_start_date")
        else:
            self.events_start_date=start_date
            
        if not end_date is None:
            self.events_end_date=end_date
        else:
            self.events_end_date = (self.current_date + timedelta(days=365)).strftime("%Y-%m-%d")

        logger.info(
            f"Fechas - Events Start: {self.events_start_date}, "
            f"Events End: {self.events_end_date}, Incremental: [*, {self.end_exclusive})"
        )

    def _setup_api(self):
        """Configura parámetros y fábrica de sesiones por hilo"""
        self.api_config = self.config.get("api_config", {})
        self.base_url = self.api_config.get("base_url")
        self.api_version = self.api_config.get("version")

        self.headers = {
            "x-api-key": self.credentials.get("api_key"),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # Retry policy
        self._retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
        )

        # Thread-local para no compartir Session entre hilos
        self._thread_local = threading.local()

    def _get_session(self) -> requests.Session:
        """Devuelve una Session por hilo (no compartida)"""
        sess = getattr(self._thread_local, "session", None)
        if sess is None:
            sess = requests.Session()
            adapter = HTTPAdapter(max_retries=self._retry_strategy)
            sess.mount("http://", adapter)
            sess.mount("https://", adapter)
            self._thread_local.session = sess
        return sess

    def _next_day(self, d: str) -> str:
        return (datetime.strptime(d, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    
    def _format_endpoint(self, template: str, **kwargs) -> str:
        """Formatea endpoint tolerante a placeholders faltantes"""
        class _Safe(dict):
            def __missing__(self, k):
                return ""
        return template.format_map(_Safe(**kwargs))

    def call_endpoint(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Realiza una llamada GET a un endpoint de la API de Forvenues"""
        url = f"{self.base_url}{endpoint}"
        try:
            session = self._get_session()
            response = session.get(url=url, headers=self.headers, params=params, timeout=self.timeout)
            logger.info(f"API Call: {response.url} | Params: {str(params)} | Status: {response.status_code}" )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error calling endpoint {url}: {e}")
            raise

    def _fetch_paginated(
        self,
        endpoint: str,
        base_params: Dict[str, Any],
        event_id: Optional[str],
        paginated: bool,
    ) -> List[Dict[str, Any]]:
        """
        Soporta paginación por offset. (Si tu API usa token, ver alternativa más abajo.)
        """
        results: List[Dict[str, Any]] = []
        limit = self.page_size

        if paginated:
            original_endpoint = endpoint  # ¡No pisar!
            offset = 0

            while True:
                # 1) formateá SIEMPRE desde el endpoint original
                ctx = {
                    "event_id": event_id or "",
                    "page_size": limit,
                    "offset": offset,
                }
                endpoint = self._format_endpoint(original_endpoint, **ctx)

                try:
                    response = self.call_endpoint(endpoint, params=base_params)
                    data = response.get(self.data_field) or []

                    # 3) siempre acumulá lo recibido
                    results.extend(data)

                    # 4) condición de corte: última página si trae menos que el límite
                    if len(data) < limit:
                        break

                    # 5) avanzá el offset
                    offset += limit

                except Exception as e:
                    logger.warning(f"Fallo consultando página offset={offset}: {e}")
                    break

        else:
            # Sin paginación
            ctx = {"event_id": event_id or ""}
            endpoint = self._format_endpoint(endpoint, **ctx)
            try:
                response = self.call_endpoint(endpoint, params=base_params or {})
                data = response.get(self.data_field) or []
                results.extend(data)
            except Exception as e:
                logger.error(f"Formato endpoint falló. endpoint={endpoint} err={e}")
                raise

        return results

    # ---------------------------
    # Procesamiento datasets por tipo
    # ---------------------------
    def _safe_save(self, dataset: str, file_name: str, df: pd.DataFrame):
        """Escritura serializada para evitar crashes en librerías nativas (pyarrow/fastparquet)."""
        with self._write_lock:
            self.controller.saveStorage(dataset, file_name, df)

    def _process_event_dataset_type3(self, dataset: str, event_id: str) -> bool:
        """Tipo 3: relacionado con evento + NO incremental (con o sin paginación por config)"""
        print(f"Procesando dataset {dataset} para el evento {event_id}")
        dataset_config = self.config.get("datasets", {}).get(dataset)
        if not dataset_config:
            return False

        try:
            endpoint = dataset_config.get("endpoint")
            paginated = dataset_config.get("paginated") or False
            base_params: Dict[str, Any] = {}
            
            data = self._fetch_paginated(endpoint, base_params, event_id, paginated)
            if data:
                df = pd.DataFrame(data)
                df["event_id"] = event_id
                timestamp = int(time.time_ns())
                file_name = f"{dataset}_{event_id}_{timestamp}_{self.connection_id}.parquet"
                self._safe_save(dataset, file_name, df)
                #logger.info(f"Guardados {len(df)} registros de {dataset} para evento {event_id} (tipo 3)")
            else:
                logger.info(f"No hay datos de {dataset} para evento {event_id}")
            return True

        except Exception as e:
            logger.error(f"Error procesando dataset tipo 3 {dataset}: {e}")
            return False

    def _process_event_dataset_type4(self, dataset: str, event_id: str) -> bool:
        """
        Tipo 4: relacionado con evento + incremental (end-exclusivo).
        Rango: [start_date, end_exclusive)
        Control: actualiza last_loaded_date=hoy y next_start_date=mañana
        """
        print(f"Procesando dataset {dataset} para el evento {event_id}")
        dataset_config = self.config.get("datasets", {}).get(dataset)
        if not dataset_config:
            return False

        try:
            control_data = self.controller.getLatestSuccessExecution(dataset, event_id=event_id)
            start_date = control_data.get("next_start_date") if control_data else self.events_start_date
            end_exclusive = self.end_exclusive
            today_inclusive = self.today

            # Ventana vacía (si por algún motivo start > hoy)
            if datetime.strptime(start_date, "%Y-%m-%d") > datetime.strptime(today_inclusive, "%Y-%m-%d"):
                logger.info(f"[{dataset}|event:{event_id}] Ventana vacía {start_date}..{end_exclusive} (end-excl); skip")
                self.controller.postLatestSuccessExecution(
                    dataset,
                    {
                        "last_execution": self.extraction_date,
                        "last_loaded_date": today_inclusive,
                        "next_start_date": self._next_day(today_inclusive),
                    },
                    event_id=event_id,
                )
                return True

            endpoint = dataset_config.get("endpoint")
            incremental_field_from = dataset_config.get("incremental_field_from")
            incremental_field_to = dataset_config.get("incremental_field_to")
            base_params: Dict[str, Any] = {}
            if incremental_field_from and incremental_field_to:
                base_params[incremental_field_from] = start_date
                base_params[incremental_field_to] = end_exclusive

            paginated = dataset_config.get("paginated")
            
            data = self._fetch_paginated(endpoint, base_params, event_id, paginated)

            if data:
                df = pd.DataFrame(data)
                timestamp = int(time.time_ns())
                file_name = f"{dataset}_{event_id}_{start_date}_{end_exclusive}_{timestamp}_{self.connection_id}.parquet"
                self._safe_save(dataset, file_name, df)
                #logger.info(f"Guardados {len(df)} registros de {dataset} para evento {event_id} (tipo 4)")
            else:
                logger.info(f"No hay datos de {dataset} para evento {event_id} en [{start_date}, {end_exclusive})")

            self.controller.postLatestSuccessExecution(
                dataset,
                {
                    "last_execution": self.extraction_date,
                    "last_loaded_date": today_inclusive,
                    "next_start_date": self._next_day(today_inclusive),
                },
                event_id=event_id,
            )
            return True

        except Exception as e:
            logger.error(f"Error procesando dataset tipo 4 {dataset}: {e}")
            return False

    # ---------------------------
    # Orquestación
    # ---------------------------
    def _process_single_event_all_datasets(self, event: Dict[str, Any], datasets: List[str]) -> Dict[str, bool]:
        """Procesa todos los datasets de un evento de forma PARALELA"""
        event_id = event.get("_id")
        event_results: Dict[str, bool] = {}
        logger.info(f"Procesando evento {event_id} con {len(datasets)} datasets en paralelo")
        
        # Procesar datasets en paralelo para este evento
        with ThreadPoolExecutor(max_workers=min(len(datasets), 4)) as executor:
            futures = {}
            for dataset in datasets:
                dataset_key = f"{dataset}_{event_id}"
                dataset_config = self.config.get("datasets", {}).get(dataset, {})
                is_incremental = dataset_config.get("incremental", False)
                
                if is_incremental:
                    future = executor.submit(self._process_event_dataset_type4, dataset, event_id)
                else:
                    future = executor.submit(self._process_event_dataset_type3, dataset, event_id)
                
                futures[dataset_key] = future
            
            # Recoger resultados
            for dataset_key, future in futures.items():
                try:
                    success = future.result(timeout=300)  # 5 minutos por dataset
                    event_results[dataset_key] = success
                    if not success:
                        logger.warning(f"Falló dataset {dataset_key}")
                    else:
                        logger.info(f"Completado dataset {dataset_key}")
                except Exception as e:
                    logger.error(f"Excepción procesando dataset {dataset_key}: {e}")
                    event_results[dataset_key] = False
        
        return event_results
    
    def _process_type1_datasets(self, datasets: List[str]) -> Dict[str, bool]:
        """
        Tipo 1: no relacionado con evento + no incremental
        """
        results: Dict[str, bool] = {}
        for dataset in datasets:
            logger.info(f"Procesando dataset tipo 1: {dataset}")
            dataset_config = self.config.get("datasets", {}).get(dataset)
            if not dataset_config:
                results[dataset] = False
                continue
            try:
                endpoint = dataset_config.get("endpoint")
                paginated = dataset_config.get("paginated") or False 

                base_params: Dict[str, Any] = {}
                if dataset == "events":
                    timestamp = int(time.time_ns())
                    file_name = f"{dataset}_{timestamp}_{self.connection_id}.parquet"
                    base_params = {"start": self.events_start_date, "end": self.events_end_date}
                else:
                    file_name = f"{dataset}_{self.connection_id}.parquet"
                    
                data = self._fetch_paginated(endpoint, base_params, None, paginated)
                if data:
                    df = pd.DataFrame(data)
                    self._safe_save(dataset, file_name, df)
                    results[dataset] = True
                    logger.info(f"Guardados {len(df)} registros de {dataset} tipo 1")
                else:
                    logger.info(f"No hay datos de {dataset} tipo 1")
                    results[dataset] = True
            except Exception as e:
                logger.error(f"Error procesando dataset tipo 1 {dataset}: {e}")
                results[dataset] = False
        return results

    # ---------------------------
    # Eventos
    # ---------------------------
    def _get_events_to_process(self) -> List[Dict[str, Any]]:
        """Obtiene y evalúa los eventos a procesar."""
        try:
            url = "/events"
            params = {"start": self.events_start_date, "end": self.events_end_date}
            response_data = self.call_endpoint(url, params)
            all_events = response_data.get("data", [])
            logger.info(f"Obtenidos {len(all_events)} eventos desde la API")
            
            return all_events or []
        except Exception as e:
            logger.error(f"Error obteniendo eventos: {e}")
            return []

    def _get_all_parents_datasets(self) -> List[str]:
        """Detecta datasets que dependen de eventos (tienen {event_id} en su endpoint)"""
        parents_datasets: List[str] = []
        datasets_config = self.config.get("datasets", {})
        for dataset_name, dataset_config in datasets_config.items():
            if dataset_name == "events":
                continue
            endpoint = dataset_config.get("endpoint", "")
            if "{event_id}" in endpoint:
                parents_datasets.append(dataset_name)
        logger.info(f"Datasets parents detectados automáticamente: {parents_datasets}")
        return parents_datasets

    def _classify_datasets(self, requested_datasets: List[str]) -> Dict[str, List[str]]:
        """
        Si se solicita "events", automáticamente incluye TODOS los datasets satélite
        """
        datasets_config = self.config.get("datasets", {})

        if "events" in requested_datasets:
            logger.info("Dataset 'events' solicitado - incluyendo automáticamente todos los datasets parents")
            all_parents_datasets = self._get_all_parents_datasets()
            datasets_to_process = list(set(requested_datasets + all_parents_datasets))
            logger.info(f"Datasets expandidos automáticamente: {datasets_to_process}")
        else:
            datasets_to_process = requested_datasets

        classification = {"type1": [], "type3": [], "type4": []}
        for dataset in datasets_to_process:
            dataset_config = datasets_config.get(dataset, {})
            endpoint = dataset_config.get("endpoint", "")
            is_incremental = dataset_config.get("incremental", False)
            has_event_id = "{event_id}" in endpoint

            if not has_event_id and not is_incremental:
                classification["type1"].append(dataset)
#            elif not has_event_id and is_incremental:
#                classification["type2"].append(dataset)
            elif has_event_id and not is_incremental:
                classification["type3"].append(dataset)
            elif has_event_id and is_incremental:
                classification["type4"].append(dataset)

        logger.info(f"Clasificación final de datasets: {classification}")
        print(classification)
        return classification
    

    def Iniciar(self, requested_datasets: List[str]) -> bool:
        """
        Orquesta la extracción de datos
        """
        try:
            logger.info(f"Iniciando extracción para los datasets: {requested_datasets}")

            # Clasificar
            classification = self._classify_datasets(requested_datasets)

            events_to_process: List[Dict[str, Any]] = []
            event_dependent_datasets = classification["type3"] + classification["type4"]
            if event_dependent_datasets:
                   events_to_process = self._get_events_to_process()
            
            overall_results: Dict[str, bool] = {}

            # Pool general (type1)
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {}
                if classification["type1"]:
                    futures["type1"] = executor.submit(
                        self._process_type1_datasets, classification["type1"]
                    )
                for key, fut in futures.items():
                    logger.info(f'Evaluando resultado de {key}')
                    result = fut.result()
                    overall_results.update(result)

            # Pool de eventos
            if event_dependent_datasets and events_to_process:
                with ThreadPoolExecutor(max_workers=self.max_event_workers) as ev_exec:
                    ev_futures = [
                        ev_exec.submit(
                            self._process_single_event_all_datasets, event, event_dependent_datasets
                        )
                        for event in events_to_process
                    ]
                    for f in as_completed(ev_futures):
                        result = f.result()
                        overall_results.update(result)

            # Verificar resultados
            failed_datasets = [d for d, ok in overall_results.items() if not ok]
            if failed_datasets:
                logger.error(f"Datasets fallidos: {failed_datasets}")
                return False

            # Actualizar control de eventos solo si hubo dependientes
            if event_dependent_datasets:
                self.controller.postLatestSuccessExecution(
                    "events_last_read",
                    {
                        "last_execution": self.extraction_date,
                        "last_loaded_date": self.today,
                        "next_start_date": self._next_day(self.today),
                    },
                )

            logger.info("Extracción completada exitosamente")
            return True

        except Exception as e:
            logger.error(f"Error en la extracción: {e}")
            return False
