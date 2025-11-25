"""
Utilidades para manejo de mensajes.
"""
import json
import base64
import logging

logger = logging.getLogger(__name__)


def decode_message_data(cloud_event) -> dict:
    """
    Decodifica los datos del mensaje desde el evento de Cloud Function.
    Maneja tanto mensajes simples como mensajes con estructura {'data': 'base64_content'}.
    
    Args:
        cloud_event: Evento de Cloud Function
        
    Returns:
        dict: Datos del mensaje decodificados
    """
    message_data = base64.b64decode(cloud_event.data['message']['data']).decode('utf-8')
    message_json = json.loads(message_data)
    
    # Si el mensaje tiene estructura {'data': 'base64_content'}, extraer el contenido real
    if 'data' in message_json and len(message_json) == 1:
        logger.info("Detectado mensaje con estructura {'data': 'base64_content'}, decodificando contenido interno")
        inner_data = base64.b64decode(message_json['data']).decode('utf-8')
        message_json = json.loads(inner_data)
        logger.info(f"Contenido interno decodificado: {message_json}")
    
    return message_json


def extract_flow_identifiers(message_json: dict) -> tuple:
    """
    Extrae los identificadores del flujo desde el mensaje.
    
    Args:
        message_json: Datos del mensaje
        
    Returns:
        tuple: (flow_id, account, task_id, run_id)
    """
    flow_id = message_json.get('flow_id')
    account = message_json.get('account')
    task_id = message_json.get('task_id')
    run_id = message_json.get('run_id')
    
    return flow_id, account, task_id, run_id


def has_dynamic_definition(message_json: dict) -> bool:
    """
    Determina si el mensaje contiene una definición dinámica de flujo.
    
    Args:
        message_json: Datos del mensaje
        
    Returns:
        bool: True si tiene definición dinámica
    """
    return bool(
        message_json.get("flow_config")
        or message_json.get("steps")
        or message_json.get("tasks")
    )
