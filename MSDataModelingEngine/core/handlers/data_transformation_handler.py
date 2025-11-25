"""
Handler para el procesamiento de transformación de datos.
"""
import logging
from typing import Dict, Any
from core.config import config
from core.utils.message_utils import (
    decode_message_data, 
    validate_required_fields,
    create_flow_notification_data
)
from utils.publisher import publish_message

logger = logging.getLogger(__name__)


class DataTransformationHandler:
    """Handler para el procesamiento de transformación de datos."""
    
    def handle_data_transformation_request(self, cloud_event) -> Dict[str, Any]:
        """
        Maneja una solicitud de transformación de datos.
        
        Args:
            cloud_event: Evento de Cloud Function
            
        Returns:
            dict: Resultado del procesamiento
        """
        try:
            # Decodificar mensaje
            message_json = decode_message_data(cloud_event)
            
            # Validar campos obligatorios
            try:
                validate_required_fields(message_json)
            except ValueError as validation_error:
                return {
                    "status": "error",
                    "message": f"Error de validación: {validation_error}",
                    "error_type": "validation_error"
                }
            # Manejar tanto '_m_account' como 'account' para compatibilidad
            m_account = message_json.get('_m_account') or message_json.get('account', 'unknown')
            
            # Buscar job_id en diferentes ubicaciones del mensaje
            job_id = message_json.get('job_id') or message_json.get('params', {}).get('job_id', 'unknown')
            
            # Buscar parámetros de flujo en diferentes ubicaciones
            flow_id = message_json.get('flow_id', 'unknown')
            run_id = message_json.get('run_id', 'unknown')
            task_id = message_json.get('task_id', 'unknown')
            
            # Si no se encontraron los parámetros principales, buscar en params
            if flow_id == 'unknown':
                flow_id = message_json.get('params', {}).get('flow_id', 'unknown')
            if run_id == 'unknown':
                run_id = message_json.get('params', {}).get('run_id', 'unknown')
            if task_id == 'unknown':
                task_id = message_json.get('params', {}).get('task_id', 'unknown')
          
             # # Procesar transformación de datos
            from worker.datatransformation import CargaAutomatizacion
            carga = CargaAutomatizacion(_m_account=m_account)
            result = carga.Iniciar(job_id, flow_id, run_id, task_id)

            return {
                "status": "completed",
                "message": "Transformación de datos completada exitosamente",
                "m_account": m_account,
                "job_id": job_id,
                "flow_id": flow_id,
                "run_id": run_id,
                "task_id": task_id,
                "result": result
            }
                
        except Exception as e:
            logger.error(f"Error procesando solicitud de transformación: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "message": f"Error procesando solicitud de transformación: {str(e)}",
                "error_type": "processing_error"
            }

    def handle_polling_request(self, cloud_event) -> Dict[str, Any]:
        """
        Maneja una solicitud de polling de estado de ejecución.
        
        Args:
            cloud_event: Evento de Cloud Function
            
        Returns:
            dict: Resultado del polling
        """
        try:
            logger.info(f"Evento de polling recibido MSDATAMODELING: {cloud_event.data}")
            
            # Decodificar mensaje
            message_json = decode_message_data(cloud_event)
            logger.info(f"Procesando mensaje de polling MSDATAMODELING: {message_json}")
            
            # Validar campos obligatorios
            try:
                validate_required_fields(message_json)
            except ValueError as validation_error:
                logger.error(f"Error de validación en polling MSDATAMODELING: {validation_error}")
                
                # Extraer identificadores disponibles para notificación de error
                m_account = message_json.get('_m_account', 'unknown')
                run_id = message_json.get('run_id', 'unknown')
                flow_id = message_json.get('flow_id', 'unknown')
                task_id = message_json.get('task_id', 'unknown')
                
                # Notificar al FlowController sobre el error de validación
                try:
                    flow_notification = create_flow_notification_data(
                        flow_id=flow_id,
                        run_id=run_id,
                        task_id=task_id,
                        account=m_account,
                        step=config.TRIGGER_TOPIC,
                        status='error',
                        error=str(validation_error)
                    )
                    message_id = publish_message(
                        project_id=config.PROJECT_ID, 
                        topic_id=config.MS_FLOWS_CONTROLLER_TOPIC, 
                        payload=flow_notification
                    )
                    logger.info(f"Notificación de error de validación en polling enviada al FlowController: {message_id}")
                except Exception as notify_error:
                    logger.error(f"No se pudo enviar notificación de error de validación en polling: {notify_error}")
                
                # Retornar error inmediatamente para evitar loop infinito
                return {
                    "status": "error",
                    "message": f"Error de validación en polling: {validation_error}",
                    "error_type": "validation_error"
                }
            
            # Extraer identificadores
            m_account = message_json.get('_m_account')
            run_id = message_json.get('run_id')
            flow_id = message_json.get('flow_id')
            task_id = message_json.get('task_id')
            
            if not run_id:
                raise ValueError("run_id es requerido para polling")

            logger.info(f"Identificadores polling MSDATAMODELING: {m_account}, {run_id}, {flow_id}, {task_id}")
            
            # Configuración de polling
            max_attempts = message_json.get('max_attempts', 60)  # Default: 60 intentos
            poll_interval = message_json.get('poll_interval', 30)  # Default: 30 segundos
            
            # Procesar polling
            from worker.datatransformation import CargaAutomatizacion
            carga = CargaAutomatizacion(_m_account=m_account)
            result = carga.poll_run_status(run_id, max_attempts, poll_interval)
            
            logger.info(f"Polling completado para cuenta: {m_account}, run_id: {run_id} MSDATAMODELING")
            
            # Determinar el estado final
            if result["status"] == "success":
                # Notificar al FlowController sobre el éxito
                try:
                    flow_notification = create_flow_notification_data(
                        flow_id=flow_id,
                        run_id=run_id,
                        task_id=task_id,
                        account=m_account,
                        step=config.TRIGGER_TOPIC,
                        status='completed',
                        result={
                            "run_id": run_id,
                            "polling_result": result,
                            "attempts": result.get("attempts", 0)
                        }
                    )
                    message_id = publish_message(
                        project_id=config.PROJECT_ID, 
                        topic_id=config.MS_FLOWS_CONTROLLER_TOPIC, 
                        payload=flow_notification
                    )
                    logger.info(f"Notificación de éxito enviada al FlowController: {message_id} MSDATAMODELING")
                except Exception as e:
                    logger.warning(f"No se pudo enviar notificación al FlowController: {e}")
                
            else:  # status == "error"
                # Notificar al FlowController sobre el error
                try:
                    flow_notification = create_flow_notification_data(
                        flow_id=flow_id,
                        run_id=run_id,
                        task_id=task_id,
                        account=m_account,
                        step=config.TRIGGER_TOPIC,
                        status='error',
                        error=result.get("error", "Error desconocido en polling")
                    )
                    message_id = publish_message(
                        project_id=config.PROJECT_ID, 
                        topic_id=config.MS_FLOWS_CONTROLLER_TOPIC, 
                        payload=flow_notification
                    )
                    logger.info(f"Notificación de error enviada al FlowController: {message_id}")
                except Exception as notify_error:
                    logger.error(f"No se pudo enviar notificación de error al FlowController: {notify_error}")
                
            return {
                "status": result["status"],
                "message": f"Polling completado para run_id: {run_id}",
                "m_account": m_account,
                "run_id": run_id,
                "flow_id": flow_id,
                "task_id": task_id,
                "result": result
            }
            
        except Exception as e:
            logger.error(f"Error procesando solicitud de polling: {str(e)}", exc_info=True)
            
            # Extraer identificadores para notificación de error
            try:
                message_json = decode_message_data(cloud_event)
                m_account = message_json.get('_m_account', 'unknown')
                run_id = message_json.get('run_id', 'unknown')
                flow_id = message_json.get('flow_id', 'unknown')
                task_id = message_json.get('task_id', 'unknown')
            except:
                m_account = run_id = flow_id = task_id = 'unknown'
            
            # Notificar al FlowController sobre el error
            try:
                flow_notification = create_flow_notification_data(
                    flow_id=flow_id,
                    run_id=run_id,
                    task_id=task_id,
                    account=m_account,
                    step=config.TRIGGER_TOPIC,
                    status='error',
                    error=str(e)
                )
                message_id = publish_message(
                    project_id=config.PROJECT_ID, 
                    topic_id=config.MS_FLOWS_CONTROLLER_TOPIC, 
                    payload=flow_notification
                )
                logger.info(f"Notificación de error enviada al FlowController: {message_id}")
            except Exception as notify_error:
                logger.error(f"No se pudo enviar notificación de error al FlowController: {notify_error}")
            
            return {
                "status": "error",
                "message": f"Error procesando solicitud de polling: {str(e)}",
                "m_account": m_account,
                "run_id": run_id,
                "flow_id": flow_id,
                "task_id": task_id
            }
