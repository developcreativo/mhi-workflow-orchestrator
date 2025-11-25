"""
Utilidades para manejo de mensajes en DataTransformationWorker.
"""
import json
import base64
import logging
from typing import Dict, Any, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


def decode_message_data(cloud_event) -> Dict[str, Any]:
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


def extract_transformation_identifiers(message_json: Dict[str, Any]) -> Tuple[str, str, str, str, str]:
    """
    Extrae los identificadores de transformación desde el mensaje.
    
    Args:
        message_json: Datos del mensaje
        
    Returns:
        tuple: (m_account, job_id, flow_id, run_id, task_id)
    """
    # Manejar tanto '_m_account' como 'account' para compatibilidad
    m_account = message_json.get('_m_account') or message_json.get('account')
    job_id = message_json.get('job_id')
    flow_id = message_json.get('flow_id')
    run_id = message_json.get('run_id')
    task_id = message_json.get('task_id')
    
    return m_account, job_id, flow_id, run_id, task_id


def validate_required_fields(message_json: Dict[str, Any]) -> None:
    """
    Valida que los campos obligatorios estén presentes en el mensaje.
    
    Args:
        message_json: Datos del mensaje
        
    Raises:
        ValueError: Si faltan campos obligatorios
    """
    print(f'message_json: {message_json}')
    # Validar que al menos uno de los campos de cuenta esté presente
    if not (message_json.get('_m_account') or message_json.get('account')):
        raise ValueError("Ni '_m_account' ni 'account' llegan como parámetro o tienen valores vacíos")
    
    # Validar campos obligatorios
    required_fields = ['job_id', 'flow_id', 'run_id']
    
    for field in required_fields:
        if not message_json.get(field):
            raise ValueError(f"{field} no llega como parámetro o tiene un valor vacío")


def create_error_notification_data(m_account: str, flow_id: str, run_id: str, task_id: str, error_message: str) -> Dict[str, Any]:
    """
    Crea la estructura de datos para notificación de error.
    
    Args:
        m_account: Identificador de la cuenta
        flow_id: ID del flujo
        run_id: ID de la ejecución
        task_id: ID de la tarea
        error_message: Mensaje de error
        
    Returns:
        dict: Estructura de datos para notificación
    """
    contenido = f"Se ha producido un error en la transformación de datos:\n\n{error_message}\n\n"
    contenido += "Visita el Google Cloud Logging para más información: https://console.cloud.google.com/logs"
    
    return {
        "_m_account": m_account,
        "flow_id": flow_id,
        "run_id": run_id,
        "task_id": task_id,
        "type": "S",
        "data": {
            "subject": f"Error en la transformación de datos para la cuenta: {m_account}",
            "text": contenido
        }
    }


def create_success_notification_data(m_account: str, job_id: str, flow_id: str, run_id: str, task_id: str, result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Crea la estructura de datos para notificación de éxito.
    
    Args:
        m_account: Identificador de la cuenta
        job_id: ID del trabajo
        flow_id: ID del flujo
        run_id: ID de la ejecución
        task_id: ID de la tarea
        result: Resultado de la transformación
        
    Returns:
        dict: Estructura de datos para notificación
    """
    contenido = f"La transformación de datos se ha completado exitosamente:\n\n"
    contenido += f"Job ID: {job_id}\n"
    contenido += f"Resultado: {json.dumps(result, indent=2)}\n\n"
    contenido += "Visita el Google Cloud Logging para más información: https://console.cloud.google.com/logs"
    
    return {
        "_m_account": m_account,
        "flow_id": flow_id,
        "run_id": run_id,
        "task_id": task_id,
        "type": "S",
        "data": {
            "subject": f"Transformación de datos completada para la cuenta: {m_account}",
            "text": contenido
        }
    }


def create_flow_notification_data(
    flow_id: str,
    run_id: str,
    task_id: str,
    account: str,
    step: str,
    status: str,
    result: Dict[str, Any] = None,
    error: str = None
) -> Dict[str, Any]:
    """
    Crea la estructura de datos para notificación de flujo al FlowController.
    
    Args:
        flow_id: ID del flujo
        run_id: ID de la ejecución
        task_id: ID de la tarea
        account: Cuenta
        step: Paso del proceso
        status: Estado del proceso
        result: Resultado (opcional)
        error: Error (opcional)
        
    Returns:
        dict: Estructura de datos para notificación
    """
    notification_data = {
        'flow_id': flow_id,
        'run_id': run_id,
        'task_id': task_id,
        'account': account,
        'step': step,
        'status': status,
        'timestamp': datetime.now().isoformat()
    }
    
    if result is not None:
        notification_data['result'] = result
    
    if error is not None:
        notification_data['error'] = error
    
    return notification_data
