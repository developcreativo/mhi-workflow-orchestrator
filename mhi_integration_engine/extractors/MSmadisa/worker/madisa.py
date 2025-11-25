import json, os, datetime, logging,sys
from typing import List, Dict, Any, Optional
import pandas as pd
from sqlalchemy import create_engine
from utils.controller import Controller

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class Extraer:
    """
    Madisa data extractor class for SQL Server databases using pyairbyte.
    """
    def __init__(self, 
                 _m_account: str, 
                 start_date: Optional[str] = None, 
                 end_date: Optional[str] = None,
                 companies: Optional[List[str]] = None):
        """
        Initialize the Madisa extractor.
        
        Args:
            account: Client account identifier
            datasets: List of datasets to extract
            start_date: Optional start date for incremental loads (format: YYYY-MM-DD)
            end_date: Optional end date for incremental loads (format: YYYY-MM-DD)
            companies: Optional list of company IDs to filter data
        """
        self.account = _m_account
        
        # Inicializamos las fechas si recibimos como parametros start_date o end_date
        self.exec_datetime = datetime.datetime.now()
        
        # Convertir start_date y end_date a objetos date si se proporcionan
        self.start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
        ayer = (datetime.datetime.now() - datetime.timedelta(days=1)).date()
        self.end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else ayer
        
        # Instanciamos companies si los recibimos como parametro
        self.companies = companies if companies else []
        
        
        # Initialize controller for common utility functions
        self.controller = Controller(self.account, 'MADISA')
        
        # Load config and credentials
        self._load_config()
        self._load_credentials()
        
        self._init_execution()
        
    def _load_config(self):
        """Load the connector configuration from config.json."""
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(os.path.dirname(script_dir), 'config.json')
        
            with open(config_path, 'r') as config_file:
                self.config = json.load(config_file)
        
            # Validate essential config elements
            if "extractor_name" not in self.config:
                raise ValueError("Missing 'extractor_name' in config.json")
                
            if "datasets" not in self.config:
                raise ValueError("Missing 'datasets' section in config.json")
                
            # Set default parameters
            self.extractor_name = self.config["extractor_name"]
            
            # Si start_date no fue proporcionado en init, usar el valor por defecto y convertirlo a date
            if not self.start_date:
                default_start_date_str = self.config.get("default_start_date", "2022-01-01")
                self.start_date = datetime.datetime.strptime(default_start_date_str, "%Y-%m-%d").date()
                
        except Exception as e:
            msg = f"Error loading configuration: {str(e)}"
            logger.error(msg)
            raise Exception(msg)
    
    def _load_credentials(self):
        """Load credentials from Secret Manager."""
        try:
            # Get credentials using the controller utility
            secret_name = f"{self.account}_{self.extractor_name}"
            self.credentials = self.controller.getSecret(secret_name)
            
            # Check if credentials contain required fields
            required_fields = ["hostname", "port", "dbname", "schema", "user", "password"]
            for field in required_fields:
                if field not in self.credentials:
                    raise ValueError(f"Missing required credential field: {field}")
                
            # Override default_start_date if specified in credentials
            if "start_date" in self.credentials:
                default_start_date_cred = self.credentials["start_date"]
                # Convertir a objeto date para la comparación y asignación
                if not self.start_date or self.start_date == datetime.datetime.strptime(self.config.get("default_start_date", "2022-01-01"), "%Y-%m-%d").date():
                    self.start_date = datetime.datetime.strptime(default_start_date_cred, "%Y-%m-%d").date()
                    
        except Exception as e:
            raise Exception(f"Error loading credentials: {str(e)}")
    
    def _init_execution(self):
        """Initialize MSSQL Connection."""
        try:
            hostname    =self.credentials['hostname']
            port        =self.credentials['port']
            dbname      =self.credentials['dbname']
            user        =self.credentials['user']
            password    =self.credentials['password']
            
            conn_string =f"mssql+pymssql://{user}:{password}@{hostname}:{port}/{dbname}"
            self.engine = create_engine(conn_string, echo=False)
            
        except Exception as e:
            msg= f"Error inicializando la conexión: {str(e)}"
            logger.error(msg)
            raise Exception(msg)
    
    def Iniciar(self, datasets: List[str]) -> List[Dict[str, Any]]:
        """
        Start the extraction process for all requested datasets.
        
        Returns:
            List of dictionaries with extraction results for each dataset
        """
        results = []
        
        for dataset in datasets:
            try:
                # Poner control de que el dataset sea un dataset valido
                if dataset in self.config["datasets"]:
                    result = self._extract_dataset(dataset)
                    results.append({
                                "dataset": dataset,
                                "status": "success",
                                "rows_processed": result.get("rows_processed", 0)
                            })
            except Exception as e:
                    results.append({
                        "dataset": dataset,
                        "status": "error",
                        "error": str(e)
                    })
                    
                    # Send notification about dataset extraction failure
                    error_message = {
                        "status": "error",
                        "message": f"Failed to extract dataset {dataset}: {str(e)}",
                        "account": self.account,
                        "extractor": self.extractor_name
                    }
                    raise Exception(f"Failed to extract dataset {dataset}: {str(e)}")
        return results
    
    def _extract_dataset(self, dataset_name: str) -> Dict[str, Any]:
        """
        Extract a single dataset.
        
        Args:
            dataset_name: Name of the dataset to extract
            
        Returns:
            Dictionary with extraction results
        """

        try:
            
            logger.info(f"Iniciando el proceso de extracción de {dataset_name}")
            dataset_config = self.config["datasets"][dataset_name]
            
            # Handle incremental datasets
            if dataset_config.get("incremental", False):
                return self._extract_incremental_dataset(dataset_name, dataset_config)
            else:
                return self._extract_full_dataset(dataset_name, dataset_config)
                
        except Exception as e:
            raise Exception(f"Error en el proceso de extracción de {dataset_name}: {str(e)}")
    
    def _extract_full_dataset(self, dataset_name: str, dataset_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract a full dataset (non-incremental).
        
        Args:
            dataset_name: Name of the dataset
            dataset_config: Configuration for the dataset
            
        Returns:
            Dictionary with extraction results
        """
        try:
            # Get the table name from config
            table_name = dataset_config.get("table", dataset_name)
            schema     = self.credentials['schema']
            # Build SQL query
            query = f"SELECT * FROM {schema}.{table_name}"
    
            # Convert to DataFrame
            df = pd.read_sql(query, con=self.engine)
            # Save data to GCS
            file_path = f"{dataset_name}_{self.account}.parquet"
            self.controller.saveStorage(
                dataset_name,
                file_path,
                df
            )
            logger.info(f"Fin del proceso de extracción de {dataset_name}")
            
            return {
                "rows_processed": len(df)
            }
            
        except Exception as e:
            raise Exception(f"Error in full extraction for {dataset_name}: {str(e)}")
    
    def _extract_incremental_dataset(self, dataset_name: str, dataset_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract an incremental dataset. Esta versión recarga el mes actual y, si es el primer día del mes, también el último día del mes anterior.
        
        Args:
            dataset_name: Name of the dataset
            dataset_config: Configuration for the dataset
            
        Returns:
            Dictionary with extraction results
        """
        
        try:
            logger.info(f"Iniciando el proceso de extracción INCREMENTAL de {dataset_name}")

            incremental_field = dataset_config.get("incremental_field", "NumAccountDate")
            
            # Obtener el primer día del mes actual
            today_date = datetime.date.today()
            first_day_of_month = datetime.date(today_date.year, today_date.month, 1)
            
            # Calcular el último día del mes actual para la fecha de finalización
            if today_date.month == 12:
                last_day_of_month = datetime.date(today_date.year + 1, 1, 1) - datetime.timedelta(days=1)
            else:
                last_day_of_month = datetime.date(today_date.year, today_date.month + 1, 1) - datetime.timedelta(days=1)

            # Formatear las fechas para la consulta SQL
            start_date_num = int(first_day_of_month.strftime("%Y%m%d"))
            end_date_num = int(last_day_of_month.strftime("%Y%m%d"))
            
            logger.info(f"Ejecutando consulta a {dataset_name} para el período de recarga: {start_date_num} a {end_date_num}")
            
            table_name = dataset_config.get("table", dataset_name)
            schema = self.credentials['schema']
            
            # Consulta principal del mes actual
            query = f"SELECT * FROM {schema}.{table_name} WHERE {incremental_field} BETWEEN {start_date_num} AND {end_date_num}"
            
            # Ejecutamos la consulta
            df = pd.read_sql(query, con=self.engine)
            
            rows_processed = len(df)
            logger.info(f"Se encontraron {rows_processed} filas para {dataset_name} en el período de recarga.")
            
            # Si es el primer día del mes, recargar también el último día del mes anterior
            err='datos del ultimo dia del mes anterior actual'
            if today_date == first_day_of_month:
                if today_date.month == 1:
                    last_day_prev_month = datetime.date(today_date.year - 1, 12, 31)
                else:
                    last_day_prev_month = datetime.date(today_date.year, today_date.month, 1) - datetime.timedelta(days=1)
                prev_date_num = int(last_day_prev_month.strftime("%Y%m%d"))
                logger.info(f"Incluyendo también el último día del mes anterior: {prev_date_num}")
                query_prev = f"SELECT * FROM {schema}.{table_name} WHERE {incremental_field} = {prev_date_num}"
                df_prev = pd.read_sql(query_prev, con=self.engine)
                rows_processed += len(df_prev)
                if not df_prev.empty:
                    parameters_prev = f"last_day_prev_month_{prev_date_num}"
                    file_path_prev = f"{dataset_name}_{parameters_prev}_{self.account}.parquet"
                    self.controller.saveStorage(
                        dataset_name,
                        file_path_prev,
                        df_prev
                    )
            
            # Guardar datos del mes actual
            if not df.empty:
                parameters = f"from_{start_date_num}_to_{end_date_num}"
                file_path = f"{dataset_name}_{parameters}_{self.account}.parquet"
                self.controller.saveStorage(
                    dataset_name,
                    file_path,
                    df
                )
            
            # Actualizar estado de la última ejecución
            latest_exec_data = {
                "account": self.account,
                "data_source": self.extractor_name,
                "data_set": dataset_name,
                "incremental_strategy": "reload_current_month",
                "incremental_field": incremental_field,
                "incremental_value": end_date_num
            }
            self.controller.postLatestSuccessExecution(
                dataset_name,
                latest_exec_data
            )
            
            logger.info(f"Fin del proceso de extracción de {dataset_name}")
            return {
                "rows_processed": rows_processed
            }
            
        except Exception as e:
            msg = f"Error in incremental extraction for {dataset_name}: {str(e)}"
            logger.error(msg)
            raise Exception(msg)