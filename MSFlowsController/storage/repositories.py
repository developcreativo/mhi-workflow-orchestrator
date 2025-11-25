"""
Repositorios para manejo de datos en Cloud Storage.
"""
import json
import logging
from google.cloud import storage
from core.config import config

logger = logging.getLogger(__name__)


class StorageRepository:
    """Repositorio base para operaciones de Cloud Storage."""
    
    def __init__(self):
        self.storage_client = storage.Client()
    
    def _get_bucket(self, bucket_name: str):
        """Obtiene un bucket de Cloud Storage."""
        return self.storage_client.bucket(bucket_name)


class FlowDefinitionRepository(StorageRepository):
    """Repositorio para definiciones de flujos."""
    
    def get_flow_definition(self, account: str, flow_id: str) -> dict:
        """
        Lee la definición del flujo desde Cloud Storage
        
        Args:
            account: Cuenta/organización
            flow_id: ID del flujo
            
        Returns:
            dict: Definición del flujo o None si no existe
        """
        try:
            bucket = self._get_bucket(config.FLOWS_BUCKET)
            
            # Estructura organizada por cuenta
            blob_name = f"graphs/{account}/{flow_id}.json"
            blob = bucket.blob(blob_name)
            
            if blob.exists():
                content = blob.download_as_text()
                return json.loads(content)
            else:
                logger.warning(f"Definición de flujo {flow_id} para cuenta {account} no encontrada en GCS")
                return None
                
        except Exception as e:
            logger.error(f"Error leyendo definición de flujo {flow_id} para cuenta {account}: {str(e)}")
            return None


class FlowRunStateRepository(StorageRepository):
    """Repositorio para estados de ejecución de flujos."""
    
    def get_flow_run_state(self, flow_id: str, run_id: str) -> dict:
        """
        Lee el estado de ejecución del flujo desde Cloud Storage
        
        Args:
            flow_id: ID del flujo
            run_id: ID de la ejecución
            
        Returns:
            dict: Estado del flujo o None si no existe
        """
        try:
            # Leer desde el bucket de runs configurado
            bucket = self._get_bucket(config.RUNS_BUCKET)
            
            # PRIORIDAD 1: Buscar estado de ejecución en formato dinámico
            blob_name = f"flow-runs/{flow_id}/{run_id}.json"
            blob = bucket.blob(blob_name)
            
            if blob.exists():
                content = blob.download_as_text()
                logger.info(f"Estado encontrado en formato dinámico: {blob_name}")
                state = json.loads(content)
                # Normalizar el estado para que sea compatible con CallbackHandler
                return self._normalize_dynamic_state(state)
            
            # PRIORIDAD 2: Buscar estado de ejecución en formato clásico (running/)
            blob_name = f"running/{run_id}.json"
            blob = bucket.blob(blob_name)
            
            if blob.exists():
                content = blob.download_as_text()
                logger.info(f"Estado encontrado en formato clásico: {blob_name}")
                state = json.loads(content)
                # Normalizar el estado para que sea compatible con CallbackHandler
                return self._normalize_classic_state(state)
            
            logger.info(f"Estado de ejecución {run_id} no encontrado en ninguna ubicación")
            return None
                
        except Exception as e:
            logger.error(f"Error leyendo estado de flujo {flow_id}/{run_id}: {str(e)}")
            return None
    
    def _normalize_dynamic_state(self, state: dict) -> dict:
        """
        Normaliza el estado del formato dinámico para que sea compatible con CallbackHandler
        """
        # El estado dinámico ya tiene la estructura correcta
        return state
    
    def _normalize_classic_state(self, state: dict) -> dict:
        """
        Normaliza el estado del formato clásico para que sea compatible con CallbackHandler
        """
        # Convertir formato clásico a formato dinámico
        normalized = {
            "flow_id": state.get("flow_id"),
            "run_id": state.get("run_id"),
            "account": state.get("account"),
            "status": state.get("status"),
            "flow_config": {
                "steps": []
            },
            "started_at": state.get("created_at"),
            "last_updated": state.get("last_updated")
        }
        
        # OBTENER LA DEFINICIÓN COMPLETA DEL FLUJO desde graphs/
        flow_definition = self._get_flow_definition_for_classic_state(state)
        
        # Convertir tasks a steps usando la definición completa
        tasks = state.get("tasks", {})
        for task_id, task_info in tasks.items():
            # Buscar la definición completa del paso
            step_definition = self._find_step_definition(task_id, flow_definition)
            
            step = {
                "id": task_id,
                "name": task_id,
                "type": step_definition.get("type", "unknown"),
                "status": task_info.get("status", "pending"),
                "started_at": task_info.get("start"),
                "ended_at": task_info.get("end"),
                "config": step_definition.get("config", {}),
                "depends_on": step_definition.get("depends_on", [])
            }
            normalized["flow_config"]["steps"].append(step)
        
        logger.info(f"Estado clásico normalizado: {len(normalized['flow_config']['steps'])} pasos")
        return normalized
    
    def _get_flow_definition_for_classic_state(self, state: dict) -> dict:
        """
        Obtiene la definición completa del flujo desde graphs/ para estados clásicos
        """
        try:
            flow_id = state.get("flow_id")
            account = state.get("account")
            
            if not flow_id or not account:
                logger.warning("No se puede obtener definición: faltan flow_id o account")
                return {}
            
            # Usar FlowDefinitionRepository para obtener la definición
            definition_repo = FlowDefinitionRepository()
            flow_definition = definition_repo.get_flow_definition(account, flow_id)
            
            if flow_definition:
                logger.info(f"Definición del flujo obtenida: {flow_id} para cuenta {account}")
                return flow_definition
            else:
                logger.warning(f"No se encontró definición del flujo {flow_id} para cuenta {account}")
                return {}
                
        except Exception as e:
            logger.error(f"Error obteniendo definición del flujo: {str(e)}")
            return {}
    
    def _find_step_definition(self, task_id: str, flow_definition: dict) -> dict:
        """
        Encuentra la definición completa de un paso específico
        """
        steps = flow_definition.get("steps", [])
        for step in steps:
            if step.get("id") == task_id:
                return step
        
        # Si no se encuentra, retornar estructura básica
        logger.warning(f"No se encontró definición para el paso {task_id}")
        return {
            "id": task_id,
            "type": "unknown",
            "config": {},
            "depends_on": []
        }
    
    def save_flow_run_state(self, flow_id: str, run_id: str, state: dict):
        """
        Guarda el estado de ejecución del flujo en Cloud Storage
        
        Args:
            flow_id: ID del flujo
            run_id: ID de la ejecución
            state: Estado a guardar
        """
        try:
            bucket = self._get_bucket(config.RUNS_BUCKET)
            
            # Guardar estado de ejecución
            blob_name = f"flow-runs/{flow_id}/{run_id}.json"
            blob = bucket.blob(blob_name)
            
            from datetime import datetime, timezone
            state['last_updated'] = datetime.now(timezone.utc).isoformat()
            blob.upload_from_string(json.dumps(state, indent=2))
            
            logger.info(f"Estado de flujo {flow_id}/{run_id} guardado en GCS")
            
        except Exception as e:
            logger.error(f"Error guardando estado de flujo {flow_id}/{run_id}: {str(e)}")
