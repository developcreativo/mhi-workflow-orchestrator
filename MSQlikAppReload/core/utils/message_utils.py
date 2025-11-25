import base64
import json
from typing import Dict, Any, Tuple
from datetime import datetime


def decode_message_data(cloud_event) -> Dict[str, Any]:
    """Decodifica el mensaje Pub/Sub de un CloudEvent a dict."""
    if cloud_event and getattr(cloud_event, 'data', None):
        event = cloud_event.data
        if 'message' in event and 'data' in event['message']:
            decoded = base64.b64decode(event['message']['data']).decode('utf-8')
            return json.loads(decoded)
    raise ValueError("No data in Cloud Event")


def extract_identifiers(message_json: Dict[str, Any]) -> Tuple[str, str, str, str]:
    """Extrae _m_account, flow_id, run_id, task_id del mensaje (si existen)."""
    m_account = message_json.get('_m_account') or message_json.get('account')
    flow_id = message_json.get('flow_id')
    run_id = message_json.get('run_id')
    task_id = message_json.get('task_id')
    return m_account, flow_id, run_id, task_id


def create_flow_notification_data(
    *,
    flow_id: str,
    run_id: str,
    task_id: str,
    account: str,
    step: str,
    status: str,
    result: Dict[str, Any] | None = None,
    error: str | None = None,
) -> Dict[str, Any]:
    """Crea payload para notificar al FlowController del avance/resultado de un paso."""
    payload: Dict[str, Any] = {
        "flow_id": flow_id,
        "run_id": run_id,
        "task_id": task_id,
        "account": account,
        "step": step,
        "status": status,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    if result is not None:
        payload["result"] = result
    if error is not None:
        payload["error"] = error
    return payload


def create_error_notification_data(
    *, m_account: str, flow_id: str | None, run_id: str | None, task_id: str | None, error_message: str
) -> Dict[str, Any]:
    return {
        "_m_account": m_account,
        "type": "S",
        "data": {
            "subject": f"Error en Qlik Automation para la cuenta: {m_account}",
            "text": f"Se ha producido un error en la ejecución de Qlik Automation.\n\n{error_message}",
        },
        "meta": {
            "flow_id": flow_id,
            "run_id": run_id,
            "task_id": task_id,
        },
    }


def create_success_notification_data(
    *, m_account: str, flow_id: str | None, run_id: str | None, task_id: str | None, job_id: str, result: Dict[str, Any]
) -> Dict[str, Any]:
    return {
        "_m_account": m_account,
        "type": "S",
        "data": {
            "subject": f"Qlik Automation completada para la cuenta: {m_account}",
            "text": f"La ejecución se completó correctamente. job_id: {job_id}",
        },
        "meta": {
            "flow_id": flow_id,
            "run_id": run_id,
            "task_id": task_id,
            "job_id": job_id,
            "result": result,
        },
    }


