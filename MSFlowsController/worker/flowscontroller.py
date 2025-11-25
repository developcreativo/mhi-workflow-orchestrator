import json
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List
from google.cloud import pubsub_v1
from google.cloud import storage
import os
from tasks import execute_single_task

logger = logging.getLogger(__name__)


class TaskStatus:
    PENDING = "pending"
    EXECUTING = "executing"
    COMPLETED = "completed" 
    FAILED = "failed"

class FlowController:
    """
    Controlador principal para la ejecución de workflows.
    Maneja el ciclo de vida completo de los flujos y sus tareas.
    """
    
    def __init__(self):
        self.publisher = pubsub_v1.PublisherClient()
        self.storage_client = storage.Client()
        
        # Configuración de buckets
        self.flows_bucket_name = os.getenv('FLOWS_BUCKET')
        self.runs_bucket_name = os.getenv('RUNS_BUCKET')
        self.project_id = os.getenv('_M_PROJECT_ID') or os.getenv('GCP_PROJECT') or os.getenv('GOOGLE_CLOUD_PROJECT')
        
        # Configuración de topic de comunicación
        self.flows_topic = f"projects/{self.project_id}/topics/flows-controller"
        
    def start_flow(self, message_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Inicia la ejecución de un flujo.
        
        Args:
            message_data: {"flow_id": "finance-intelligence-fergus", "account": "fergus"}
        """
        logger.info(f"Iniciando flujo MESSAGE_DATA: {message_data}")
        
        # Validar datos de entrada
        flow_id = message_data.get('flow_id')
        account = message_data.get('account')
        
        if not flow_id or not account:
            raise ValueError("flow_id y account son requeridos")
        
        logger.info(f"flow_id: {flow_id}, account: {account}")

        # Cargar definición del flujo
        flow_definition = self._load_flow_definition(account, flow_id)
        logger.info(f"Definición del flujo cargada: {flow_definition}")

        # Generar run_id único
        run_id = f"{flow_id}_{account}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        
        # Crear estado inicial de ejecución
        execution_status = self._create_initial_execution_status(flow_definition, run_id)
        logger.info(f"Estado inicial de ejecución creado: {execution_status}")

        # Guardar estado inicial
        self._save_execution_status_running(run_id, execution_status)
        logger.info(f"Estado inicial de ejecución guardado en running: {run_id}")

        # Enqueue initial tasks via Celery
        logger.info(f"Enqueando tareas iniciales via Celery: {run_id}")
        self._enqueue_initial_tasks(flow_definition, execution_status, run_id)
        logger.info(f"Tareas iniciales encoladas via Celery: {run_id}")

        logger.info(f"Flujo iniciado: {flow_id}, account: {account}, run_id: {run_id}")
        
        return {"run_id": run_id, "status": "started"}

    
    def _save_execution_status_running(self, run_id: str, execution_status: Dict[str, Any]):
        """
        Guarda el estado de ejecución en el bucket de running.
        
        args:
            run_id: str
            execution_status: Dict[str, Any]
        returns:
            None
        """
        try:
            bucket = self.storage_client.bucket(self.runs_bucket_name)
            blob_name = f"running/{run_id}.json"
            blob = bucket.blob(blob_name)
            
            # Añadir timestamp de última actualización
            execution_status["last_updated"] = datetime.now(timezone.utc)
            
            content = json.dumps(execution_status, indent=2, default=str)
            blob.upload_from_string(content, content_type='application/json')
            
            logger.debug(f"Estado de ejecución guardado en running: {run_id}")
            
        except Exception as e:
            logger.error(f"Error guardando estado de ejecución {run_id} en running: {str(e)}")
            raise
    

    def _load_flow_definition(self, account: str, flow_id: str) -> Dict[str, Any]:
        """
        Carga la definición de un flujo desde Cloud Storage.
        
        args:
            account: str
            flow_id: str
        returns:
            Dict[str, Any]
        raises:
            FileNotFoundError: Si la definición del flujo no se encuentra en el bucket de flows.
            Exception: Si hay un error al cargar la definición del flujo.
        """
        bucket = self.storage_client.bucket(self.flows_bucket_name)
        # Convert hyphens to underscores to match actual file naming convention
        file_name = flow_id + '.json'
        blob_path = f"graphs/{account}/{file_name}"
        blob = bucket.blob(blob_path)
        
        logger.info(f"Buscando archivo en bucket: {self.flows_bucket_name}, ruta: {blob_path}")
        
        if not blob.exists():
            raise FileNotFoundError(f"Flow definition not found: {blob_path}")
        
        content = blob.download_as_text()
        return json.loads(content)

    
    def _get_tasks_iterable(self, flow_definition: Dict[str, Any]) -> List[tuple]:
        """
        Obtiene un iterable de tareas desde la definición del flujo.
        Maneja tanto formato 'tasks' (dict/list) como 'steps'.
        """
        defined_tasks = flow_definition.get("tasks")
        if isinstance(defined_tasks, dict):
            return [(name, cfg) for name, cfg in defined_tasks.items()]
        elif isinstance(defined_tasks, list):
            return [(t.get("id"), t) for t in defined_tasks]
        else:
            # Fallback a "steps" si no hay "tasks"
            steps = flow_definition.get("steps", [])
            return [(s.get("id"), s) for s in steps]

    def _create_initial_execution_status(self, flow_definition: Dict[str, Any], run_id: str) -> Dict[str, Any]:
        """
        Crea el estado inicial de ejecución.
        """
        logger.info(f"Creando estado inicial de ejecución: {flow_definition} {run_id}")
        execution_status = {
            "flow_id": flow_definition["flow_id"],
            "account": flow_definition["account"],
            "run_id": run_id,
            "status": "executing",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "tasks": {}
        }
        
        # Inicializar estado de todas las tareas
        for task_name, task_def in self._get_tasks_iterable(flow_definition):
            if not task_name:
                continue
            execution_status["tasks"][task_name] = {
                "status": TaskStatus.PENDING,
                "start": None,
                "end": None,
                "messages": []
            }
        
        return execution_status

    
    def _enqueue_initial_tasks(self, flow_definition: Dict[str, Any], execution_status: Dict[str, Any], run_id: str):
        """
        Enqueues initial tasks (no dependencies) via Celery.
        """
        logger.info(f"Enqueando tareas iniciales via Celery: {run_id}")
        
        for task_name, task_def in self._get_tasks_iterable(flow_definition):
            if not task_name:
                continue
            # Admite ambos nombres de clave de dependencias
            deps = task_def.get("depends_on") or task_def.get("dependencies") or []
            if not deps:
                self._execute_task(task_name, task_def, execution_status, run_id)

    
    def _execute_task(self, task_name: str, task_definition: Dict[str, Any], execution_status: Dict[str, Any], run_id: str):
        """
        Ejecuta una tarea específica.
        """
        try:
            logger.info(f"Ejecutando tarea: {task_name}")
            
            # Actualizar estado a ejecutando
            execution_status["tasks"][task_name]["status"] = TaskStatus.EXECUTING
            execution_status["tasks"][task_name]["start"] = datetime.now(timezone.utc)
            
            # Guardar estado actualizado en running
            self._save_execution_status_running(run_id, execution_status)
            
            # Preparar y enviar mensaje
            self._publish_task_message(task_name, task_definition, execution_status, run_id)
            
        except Exception as e:
            logger.error(f"Error ejecutando tarea {task_name}: {str(e)}")
            self._mark_task_as_failed(task_name, execution_status, run_id, str(e))
            raise

    def _publish_task_message(self, task_name: str, task_definition: Dict[str, Any], execution_status: Dict[str, Any], run_id: str):
        """
        Publica el mensaje de la tarea al topic correspondiente.
        """
        # Obtener config del step (puede estar en 'config' o 'params' para compatibilidad)
        step_config = task_definition.get("config", task_definition.get("params", {}))
        
        # Preparar mensaje con formato específico para el nodo de tarea
        # Envolver config en un objeto 'config' para que los extractores puedan leerlo correctamente
        task_message = {
            "flow_id": execution_status["flow_id"],
            "account": execution_status["account"],
            "run_id": run_id,
            "task_id": task_name,
            "config": step_config  # Usar 'config' en lugar de 'params'
        }
        
        # Enviar mensaje al topic específico del tipo de tarea
        topic_name = self._get_task_execution_topic(task_definition["type"])
        topic_path = f"projects/{self.project_id}/topics/{topic_name}"
        
        message_data = json.dumps(task_message).encode('utf-8')
        future = self.publisher.publish(topic_path, message_data)
        
        logger.info(f"Mensaje enviado para tarea {task_name}: {future.result()}")

    def _mark_task_as_failed(self, task_name: str, execution_status: Dict[str, Any], run_id: str, error_message: str):
        """
        Marca una tarea como fallida y guarda el estado.
        """
        execution_status["tasks"][task_name]["status"] = TaskStatus.FAILED
        execution_status["tasks"][task_name]["end"] = datetime.now(timezone.utc)
        execution_status["tasks"][task_name]["messages"] = [error_message]
        
        self._save_execution_status_running(run_id, execution_status)
    
    def _get_task_execution_topic(self, task_type: str) -> str:
        """
        Obtiene el nombre del topic para ejecutar un tipo de tarea.
        """
        if task_type == "notifications":
            return task_type
        elif task_type.startswith("extractor"):
            return task_type
        else:
            return f"{task_type}"
    
    def _load_execution_status_running(self, run_id: str) -> Dict[str, Any]:
        """
        Carga el estado de ejecución desde el bucket de running.
        
        args:
            run_id: str
        returns:
            Dict[str, Any]
        raises:
            FileNotFoundError: Si el estado de ejecución no se encuentra en el bucket de running.
            Exception: Si hay un error al cargar el estado de ejecución.
        """
        logger.info(f"Cargando estado de ejecución desde el bucket de running ESTE: {run_id}")
        bucket = self.storage_client.bucket(self.runs_bucket_name)
        blob_path = f"running/{run_id}.json"
        blob = bucket.blob(blob_path)
        
        if not blob.exists():
            raise FileNotFoundError(f"Execution status not found: {blob_path}")
        
        content = blob.download_as_text()
        return json.loads(content)