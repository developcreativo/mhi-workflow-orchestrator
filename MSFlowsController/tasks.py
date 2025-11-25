from time import timezone
from celery import shared_task
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _get_flow_controller():
    # Importación perezosa para evitar dependencias en tiempo de importación durante los tests
    from worker.flowscontroller import FlowController
    return FlowController


@shared_task(name="handle_task_completion")
def handle_task_completion(message_data: dict) -> None:
    """
    Maneja la finalización de tareas individuales
    
    Args:
        message_data: Datos del mensaje de completado
    """
    try:
        logger.info(f"Procesando completado de tarea: {message_data}")
        
        # Lógica para manejar completado de tareas
        # Esto puede incluir actualizar el estado del flujo,
        # enviar notificaciones, etc.
        
        logger.info("Tarea de completado procesada exitosamente")
        
    except Exception as e:
        logger.error(f"Error procesando completado de tarea: {str(e)}")

@shared_task(name="publish_to_pubsub")
def publish_to_pubsub(project_id: str, topic_name: str, payload: dict) -> str:
    """
    Publica un mensaje a un topic de Pub/Sub
    
    Args:
        project_id: ID del proyecto de GCP
        topic_name: Nombre del topic
        payload: Datos a publicar
    """
    try:
        from google.cloud import pubsub_v1
        import json
        
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_name)
        
        # Convertir payload a JSON
        message_data = json.dumps(payload).encode("utf-8")
        
        # Publicar mensaje
        future = publisher.publish(topic=topic_path, data=message_data)
        message_id = future.result()
        
        logger.info(f"Mensaje publicado exitosamente. Message ID: {message_id}")
        return message_id
        
    except Exception as e:
        logger.error(f"Error publicando mensaje a Pub/Sub: {str(e)}")
        raise

@shared_task(name="execute_single_task")
def execute_single_task(task_name: str, task_definition: dict, run_id: str) -> str:
    """
    Tarea Celery para ejecutar una única tarea: actualiza el estado, publica el mensaje a Pub/Sub
    (usando Celery para desacoplar), y gestiona errores.

    Args:
        task_name (str): Nombre de la tarea a ejecutar.
        task_definition (dict): Definición de la tarea, por ejemplo {"type": "...", "params": {...}}
        run_id (str): ID de la ejecución del flujo.

    Returns:
        str: ID del mensaje publicado en Pub/Sub.
    """
    try:
        fc = _get_flow_controller()
        
        # Carga el estado de ejecución
        logger.info(f"Cargando estado de ejecución para run_id: {run_id}")
        execution_status = fc._load_execution_status_running(run_id)
        
        # Actualiza el estado de la tarea a "executing"
        logger.info(f"Actualizando estado de la tarea a executing: {task_name}")
        execution_status["tasks"][task_name]["status"] = "executing"
        execution_status["tasks"][task_name]["start"] = datetime.now(timezone.utc)
        logger.info(f"Guardando estado de ejecución actualizado para run_id: {run_id}")
        fc._save_execution_status_running(run_id, execution_status)
        
        # Prepara el mensaje para el worker correspondiente
        logger.info(f"Preparando mensaje para el worker correspondiente: {task_name}")
        # Obtener config del step (puede estar en 'config' o 'params' para compatibilidad)
        step_config = task_definition.get("config", task_definition.get("params", {}))
        task_message = {
            "flow_id": execution_status["flow_id"],
            "account": execution_status["account"],
            "run_id": run_id,
            "task_id": task_name,
            "config": step_config  # Usar 'config' en lugar de 'params'
        }
        
        # Determina el tópico según el tipo de tarea
        logger.info(f"Determinando el tópico según el tipo de tarea: {task_definition["type"]}")
        topic_name = fc._get_task_execution_topic(task_definition["type"])
        project_id = fc.project_id
        
        # Encola la publicación a Pub/Sub (espera síncrona por el ID del mensaje)
        logger.info(f"Encolando la publicación a Pub/Sub para el worker correspondiente: {task_name}")
        msg_id = publish_to_pubsub.delay(project_id, topic_name, task_message).get()
        
        return msg_id
        
    except Exception as e:
        # Si falla, marca la tarea como "failed" y guarda el error
        logger.info(f"Marcando la tarea como failed: {task_name}")
        fc._load_execution_status_running(run_id)  # Recarga por si hubo cambios
        execution_status = fc._load_execution_status_running(run_id)
        execution_status["tasks"][task_name]["status"] = "failed"
        execution_status["tasks"][task_name]["end"] = datetime.now(timezone.utc)
        execution_status["tasks"][task_name]["messages"] = [str(e)]
        fc._save_execution_status_running(run_id, execution_status)
        logger.info(f"Guardando estado de ejecución actualizado para run_id: {run_id}")
        logger.error(f"Error ejecutando tarea: {str(e)}")
        raise