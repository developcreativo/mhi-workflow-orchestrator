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
            logger.info(f"Evento de polling recarga Qlik recibido MSQLIKAPPRELOAD: {cloud_event.data}")
            
            # Decodificar mensaje
            message_json = decode_message_data(cloud_event)
            logger.info(f"Procesando mensaje de polling recarga Qlik MSQLIKAPPRELOAD: {message_json}")
            
            # Extraer identificadores
            _m_account, flow_id, run_id, task_id = extract_identifiers(message_json)
            
            # Extraer parámetros específicos para recarga
            reload_id = message_json.get('reload_id')
            app_id = message_json.get('app_id')
            reload_token = message_json.get('reload_token')
            
            if not reload_id:
                raise ValueError("reload_id es requerido para polling de recarga Qlik")
            if not app_id:
                raise ValueError("app_id es requerido para polling de recarga Qlik")
            if not reload_token:
                raise ValueError("reload_token es requerido para polling de recarga Qlik")

            logger.info(f"Identificadores polling recarga Qlik MSQLIKAPPRELOAD: {_m_account}, {reload_id}, {app_id}, {flow_id}, {task_id}")
            
            # Configuración de polling
            max_attempts = message_json.get('max_attempts', 60)  # Default: 60 intentos
            poll_interval = message_json.get('poll_interval', 30)  # Default: 30 segundos
            
            # Preparar header de autenticación
            header = {
                'Accept': 'application/json', 
                'Content-Type': 'application/json', 
                'Authorization': f'Bearer {reload_token}'
            }
            
            # Procesar polling
            from worker.appreload import RecargaApp
            recarga = RecargaApp(_m_account=_m_account)
            result = recarga.poll_reload_status(reload_id, header, app_id, max_attempts, poll_interval)
            
            logger.info(f"Polling recarga Qlik completado para cuenta: {_m_account}, reload_id: {reload_id} MSQLIKAPPRELOAD")
            
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
                            "reload_id": reload_id,
                            "app_id": app_id,
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
                    logger.info(f"Notificación de éxito enviada al FlowController: {message_id} MSQLIKAPPRELOAD")
                except Exception as e:
                    logger.warning(f"No se pudo enviar notificación al FlowController: {e}")
                
                # # Enviar notificación de éxito por email
                # try:
                #     success_data = create_success_notification_data(_m_account, run_id, flow_id, run_id, task_id, result)
                #     message_id = publish_message(
                #         project_id=config.PROJECT_ID, 
                #         topic_id='notifications', 
                #         payload=success_data
                #     )
                #     logger.info(f"Notificación de éxito por email enviada: {message_id} MSQLIKAUTOMATIONEXECUTION")
                # except Exception as e:
                #     logger.warning(f"No se pudo enviar notificación de éxito por email: {e}")
                    
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
                        error=result.get("error", "Error desconocido en polling recarga Qlik")
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
                    # message_id = publish_message(
                    #     project_id=config.PROJECT_ID, 
                    #     topic_id='notifications', 
                    #     payload=error_data
                    # )
                    logger.info(f"Notificación de error por email enviada: {message_id}")
                except Exception as notify_error:
                    logger.error(f"No se pudo enviar notificación de error por email: {notify_error}")
            
            return {
                "status": result["status"],
                "message": f"Polling recarga Qlik completado para reload_id: {reload_id}",
                "_m_account": _m_account,
                "reload_id": reload_id,
                "app_id": app_id,
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
