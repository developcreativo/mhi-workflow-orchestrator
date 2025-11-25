"""
Handler para el polling de estado de ejecuciones de Qlik.
"""
import logging
from typing import Dict, Any
from core.config import config
from core.utils.message_utils import (
    decode_message_data, 
    extract_identifiers, 
    create_error_notification_data,
    create_success_notification_data,
    create_flow_notification_data
)
from utils.publisher import publish_message

logger = logging.getLogger(__name__)


class QlikPollingHandler:
    """Handler para el polling de estado de ejecuciones de Qlik."""
    
    def handle_polling_request(self, cloud_event) -> Dict[str, Any]:
        """
        Maneja una solicitud de polling de estado de ejecución de Qlik.
        
        Args:
            cloud_event: Evento de Cloud Function
            
        Returns:
            dict: Resultado del polling
        """
        try:
            # Decodificar mensaje
            message_json = decode_message_data(cloud_event)
            
            # Extraer identificadores
            _m_account, flow_id, run_id, task_id = extract_identifiers(message_json)
            
            if not run_id:
                raise ValueError("run_id es requerido para polling de Qlik")

            
            # Configuración de polling
            max_attempts = message_json.get('max_attempts', 60)  # Default: 60 intentos
            poll_interval = message_json.get('poll_interval', 30)  # Default: 30 segundos
            
            # Procesar polling
            from worker.datavisualization import CargaAutomatizacion
            carga = CargaAutomatizacion(_m_account=_m_account)
            result = carga.poll_run_status(run_id, max_attempts, poll_interval)
            
            
            # Determinar el estado final
            if result["status"] == "success":
                # Notificar al FlowController sobre el éxito
                try:
                    flow_notification = create_flow_notification_data(
                        flow_id=flow_id,
                        run_id=run_id,
                        task_id=task_id,
                        account=_m_account,
                        step=config.TRIGGER_TOPIC,
                        status='completed',
                        result={
                            "run_id": run_id,
                            "qlik_status": result.get("qlik_status"),
                            "polling_result": result,
                            "attempts": result.get("attempts", 0)
                        }
                    )
                    message_id = publish_message(
                        project_id=config.PROJECT_ID, 
                        topic_id='ms-flows-controller', 
                        payload=flow_notification
                    )
                    logger.info(f"Notificación de éxito enviada al FlowController: {message_id} MSQLIKAUTOMATIONEXECUTION")
                except Exception as e:
                    logger.warning(f"No se pudo enviar notificación al FlowController: {e}")
                
            else:  # status == "error"
                # Notificar al FlowController sobre el error
                try:
                    flow_notification = create_flow_notification_data(
                        flow_id=flow_id,
                        run_id=run_id,
                        task_id=task_id,
                        account=_m_account,
                        step=config.TRIGGER_TOPIC,
                        status='error',
                        error=result.get("error", "Error desconocido en polling Qlik")
                    )
                    message_id = publish_message(
                        project_id=config.PROJECT_ID, 
                        topic_id='ms-flows-controller', 
                        payload=flow_notification
                    )
                    logger.info(f"Notificación de error enviada al FlowController: {message_id}")
                except Exception as notify_error:
                    logger.error(f"No se pudo enviar notificación de error al FlowController: {notify_error}")
                
                # Enviar notificación de error por email
                try:
                    error_data = create_error_notification_data(_m_account, flow_id, run_id, task_id, result.get("error", "Error desconocido"))
                    logger.info(f"Notificación de error por email enviada: {message_id}")
                except Exception as notify_error:
                    logger.error(f"No se pudo enviar notificación de error por email: {notify_error}")
            
            return {
                "status": result["status"],
                "message": f"Polling Qlik completado para run_id: {run_id}",
                "_m_account": _m_account,
                "run_id": run_id,
                "flow_id": flow_id,
                "task_id": task_id,
                "qlik_status": result.get("qlik_status"),
                "result": result
            }
            
        except Exception as e:
            logger.error(f"Error procesando solicitud de polling Qlik: {str(e)}", exc_info=True)
            
            # Extraer identificadores para notificación de error
            try:
                message_json = decode_message_data(cloud_event)
                _m_account = message_json.get('_m_account', 'unknown')
                run_id = message_json.get('run_id', 'unknown')
                flow_id = message_json.get('flow_id', 'unknown')
                task_id = message_json.get('task_id', 'unknown')
            except:
                _m_account = run_id = flow_id = task_id = 'unknown'
            
            # Notificar al FlowController sobre el error
            try:
                flow_notification = create_flow_notification_data(
                    flow_id=flow_id,
                    run_id=run_id,
                    task_id=task_id,
                    account=_m_account,
                    step=config.TRIGGER_TOPIC,
                    status='error',
                    error=str(e)
                )
                message_id = publish_message(
                    project_id=config.PROJECT_ID, 
                    topic_id='ms-flows-controller', 
                    payload=flow_notification
                )
                logger.info(f"Notificación de error enviada al FlowController: {message_id}")
            except Exception as notify_error:
                logger.error(f"No se pudo enviar notificación de error al FlowController: {notify_error}")
            
            return {
                "status": "error",
                "message": f"Error procesando solicitud de polling Qlik: {str(e)}",
                "_m_account": _m_account,
                "run_id": run_id,
                "flow_id": flow_id,
                "task_id": task_id
            }
