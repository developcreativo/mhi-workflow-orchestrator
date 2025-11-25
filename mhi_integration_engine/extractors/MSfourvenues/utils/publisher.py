import json
import os
from typing import Any, Dict
from google.cloud import pubsub_v1
import logging

logger = logging.getLogger(__name__)


def publish_message(project_id: str, topic_id: str, payload: Dict[str, Any]) -> str:
    """
    Publica un mensaje JSON en Pub/Sub y devuelve el message_id.
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    data = json.dumps(payload).encode('utf-8')
    future = publisher.publish(topic_path, data)
    message_id = future.result()
    logger.info(f"Published message to {topic_id}: {message_id}")
    return message_id


def send_to_flows_controller(flow_id: str, account: str, run_id: str, task_id: str, status: str, message: str = None) -> str:
    """
    Envía el resultado de la ejecución al topic ms-flows-controller.
    
    Args:
        flow_id: ID del flujo
        account: Cuenta del usuario
        run_id: ID de la ejecución
        task_id: ID de la tarea
        status: Estado de la tarea ("completed" o "failed")
        message: Mensaje de error (opcional, solo para status="failed")
        
    Returns:
        str: Message ID del mensaje publicado
    """
    try:
        # Obtener project_id desde variables de entorno
        project_id = os.getenv('_M_PROJECT_ID') or os.getenv('GCP_PROJECT') or os.getenv('GOOGLE_CLOUD_PROJECT')
        
        if not project_id:
            raise ValueError("No se pudo obtener el project_id desde las variables de entorno")
        
        # Preparar mensaje según el formato esperado por FlowController
        response_data = {
            "flow_id": flow_id,
            "account": account,
            "run_id": run_id,
            "task_id": task_id,
            "status": status,
            "step": "ms-extractor-fourvenues", 
            "result": {
                "status": status,
                "message": message
            }
        }
        
        # Agregar mensaje de error si es un fallo
        if status == "failed" and message:
            response_data["message"] = [
                "MSExtractorFourvenues execution failed",
                message,
                f"Task: {task_id}",
                f"Account: {account}"
            ]
        
        # Enviar al topic ms-flows-controller
        topic_name = "ms-flows-controller"
        message_id = publish_message(project_id, topic_name, response_data)
        
        logger.info(f"[MSExtractorFourvenues] Respuesta enviada a flows-controller: {message_id}")
        logger.info(f"[MSExtractorFourvenues] Task {task_id} status {status} reported to ms-flows-controller")
        
        return message_id
        
    except Exception as e:
        logger.error(f"[MSExtractorFourvenues] Error enviando respuesta a ms-flows-controller: {e}")
        raise






