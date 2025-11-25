"""
Servicio de notificaciones para flujos completados o fallidos.
"""
import logging
import json
from typing import Dict, Any, Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class NotificationService:
    """Servicio para enviar notificaciones de éxito y fallo de flujos."""
    
    def __init__(self):
        self.project_id = self._get_project_id()
    
    def _get_project_id(self) -> str:
        """Obtiene el project_id desde variables de entorno."""
        import os
        project_id = os.getenv('_M_PROJECT_ID') or os.getenv('GCP_PROJECT') or os.getenv('GOOGLE_CLOUD_PROJECT')
        if not project_id:
            logger.warning("No se encontró PROJECT_ID en variables de entorno")
        return project_id
    
    def send_flow_notification(self, flow_state: Dict[str, Any], notification_type: str) -> bool:
        """
        Envía una notificación basada en el estado del flujo.
        
        Args:
            flow_state: Estado completo del flujo
            notification_type: 'success' o 'fail'
            
        Returns:
            bool: True si la notificación se envió exitosamente
        """
        try:
            # Verificar si el flujo tiene configuración de notificaciones
            flow_config = flow_state.get('flow_config', {})
            notifications_config = flow_config.get('notifications')

            # Fallback: si no hay notifications en el estado, leer la definición del flujo desde GCS
            if not notifications_config:
                try:
                    from storage.repositories import FlowDefinitionRepository
                    repo = FlowDefinitionRepository()
                    definition = repo.get_flow_definition(
                        account=flow_state.get('account'),
                        flow_id=flow_state.get('flow_id')
                    ) or {}
                    notifications_config = definition.get('notifications')
                    if notifications_config:
                        logger.info("📧 Notifications recuperadas de la definición del flujo en GCS")
                except Exception as e:
                    logger.warning(f"No se pudo recuperar notifications desde definición de flujo: {e}")
            
            if not notifications_config:
                logger.info(f"📧 No hay configuración de notificaciones para el flujo {flow_state.get('flow_id')}")
                return True  # No es un error, simplemente no hay notificaciones configuradas
            
            # Obtener configuración específica del tipo de notificación
            notification_config = notifications_config.get(notification_type)
            if not notification_config:
                logger.info(f"📧 No hay configuración de notificación '{notification_type}' para el flujo {flow_state.get('flow_id')}")
                return True
            
            # Verificar que tenga los campos requeridos
            if not self._validate_notification_config(notification_config, notification_type):
                return False
            
            # Preparar datos de la notificación
            notification_data = self._prepare_notification_data(flow_state, notification_config, notification_type)
            
            # Enviar notificación según el tipo
            success = self._send_notification(notification_data, notification_config)
            
            if success:
                logger.info(f"✅ Notificación '{notification_type}' enviada exitosamente para flujo {flow_state.get('flow_id')}")
            else:
                logger.error(f"❌ Error enviando notificación '{notification_type}' para flujo {flow_state.get('flow_id')}")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Error en send_flow_notification: {str(e)}")
            return False
    
    def _validate_notification_config(self, config: Dict[str, Any], notification_type: str) -> bool:
        """Valida que la configuración de notificación tenga los campos requeridos."""
        required_fields = ['type', 'recipients', 'message']
        
        for field in required_fields:
            if field not in config or not config[field]:
                logger.warning(f"📧 Campo requerido '{field}' faltante en configuración de notificación '{notification_type}'")
                return False
        
        # Validar que 'type' sea una lista
        if not isinstance(config['type'], list) or not config['type']:
            logger.warning(f"📧 Campo 'type' debe ser una lista no vacía en configuración de notificación '{notification_type}'")
            return False
        
        return True
    
    def _prepare_notification_data(self, flow_state: Dict[str, Any], notification_config: Dict[str, Any], notification_type: str) -> Dict[str, Any]:
        """Prepara los datos de la notificación."""
        flow_id = flow_state.get('flow_id')
        run_id = flow_state.get('run_id')
        account = flow_state.get('account')
        
        # Obtener información de los pasos
        steps = flow_state.get('flow_config', {}).get('steps', [])
        completed_steps = [s.get('id') for s in steps if s.get('status') == 'completed']
        failed_steps = [s.get('id') for s in steps if s.get('status') == 'failed']
        
        # Preparar datos básicos
        notification_data = {
            'flow_id': flow_id,
            'run_id': run_id,
            'account': account,
            'notification_type': notification_type,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'total_steps': len(steps),
            'completed_steps': completed_steps,
            'failed_steps': failed_steps,
            'message': notification_config.get('message', ''),
            'recipients': notification_config.get('recipients', ''),
            'notification_types': notification_config.get('type', [])
        }
        
        # Agregar información específica según el tipo
        if notification_type == 'success':
            notification_data['status'] = 'completed'
            notification_data['completed_at'] = flow_state.get('completed_at')
        elif notification_type == 'fail':
            notification_data['status'] = 'failed'
            notification_data['error'] = flow_state.get('error', 'Error desconocido')
            notification_data['failed_task'] = flow_state.get('failed_task')
        
        return notification_data
    
    def _send_notification(self, notification_data: Dict[str, Any], notification_config: Dict[str, Any]) -> bool:
        """Envía la notificación según el tipo configurado."""
        notification_types = notification_config.get('type', [])
        
        success = True
        
        for notification_type in notification_types:
            if notification_type == 'email':
                success &= self._send_email_notification(notification_data, notification_config)
            elif notification_type == 'slack':
                success &= self._send_slack_notification(notification_data, notification_config)
            elif notification_type == 'webhook':
                success &= self._send_webhook_notification(notification_data, notification_config)
            else:
                logger.warning(f"📧 Tipo de notificación no soportado: {notification_type}")
                success = False
        
        return success
    
    def _send_email_notification(self, notification_data: Dict[str, Any], notification_config: Dict[str, Any]) -> bool:
        """Envía notificación por email usando Pub/Sub topic notifications."""
        try:
            from google.cloud import pubsub_v1
            
            recipients = notification_config.get('recipients', '')
            message = notification_data.get('message', '')
            flow_id = notification_data.get('flow_id', '')
            notification_type = notification_data.get('notification_type', 'N')
            account = notification_data.get('account', '')
            
            logger.info(f"📧 EMAIL NOTIFICATION - Tipo: {notification_type}")
            logger.info(f"📧 EMAIL NOTIFICATION - Para: {recipients}")
            logger.info(f"📧 EMAIL NOTIFICATION - Flujo: {flow_id}")
            logger.info(f"📧 EMAIL NOTIFICATION - Mensaje: {message}")
            
            # Determinar el tipo de notificación para el sistema de negocio
            # 'S' = Success, 'F' = Fail, 'W' = Warning
            ptype_map = {
                'success': 'S',
                'fail': 'F', 
                'warning': 'W'
            }
            ptype = ptype_map.get(notification_type, 'N')
            
            # Crear el subject basado en el tipo de notificación
            if notification_type == 'success':
                subject = f"Flujo {flow_id} completado exitosamente"
            elif notification_type == 'fail':
                subject = f"Flujo {flow_id} ha fallado"
            else:
                subject = f"Notificación del flujo {flow_id}"
            
            # Preparar el mensaje con información adicional
            body = f"{message}<br><br>"
            body += f"<strong>Detalles del flujo:</strong><br>"
            body += f"- Cuenta: {account}<br>"
            body += f"- Flujo: {flow_id}<br>"
            body += f"- Run ID: {notification_data.get('run_id', 'N/A')}<br>"
            body += f"- Timestamp: {notification_data.get('timestamp', 'N/A')}<br>"
            
            if notification_type == 'success':
                body += f"- Pasos completados: {len(notification_data.get('completed_steps', []))}<br>"
            elif notification_type == 'fail':
                body += f"- Pasos fallidos: {len(notification_data.get('failed_steps', []))}<br>"
                if notification_data.get('error'):
                    body += f"- Error: {notification_data.get('error')}<br>"
            
            # Enviar al topic de notificaciones usando el mismo formato que KPIEngine
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(self.project_id, 'notifications')
            
            # Formato exacto como en KPIEngine controller.putNotification()
            # Añadimos parámetros opcionales requeridos por la plantilla (button-link, button-action)
            logs_url = f"https://console.cloud.google.com/logs?project={self.project_id}"
            if flow_id:
                logs_url += f"&q=\"{flow_id}\""
            if notification_data.get('run_id'):
                logs_url += f"%20\"{notification_data.get('run_id')}\""
            
            notification_payload = {
                "_m_account": account,
                "type": "N",
                "data": {
                    "subject": subject,
                    "to_email": recipients,
                    "text": body,
                }
            }
            
            data = json.dumps(notification_payload).encode('utf-8')
            future = publisher.publish(topic=topic_path, data=data)
            message_id = future.result()
            
            logger.info(f"✅ Notificación de email enviada al topic 'notifications' con message_id: {message_id}")
            logger.info(f"📧 Payload enviado: {json.dumps(notification_payload, indent=2)}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error enviando notificación de email: {str(e)}")
            return False
    
    def _send_slack_notification(self, notification_data: Dict[str, Any], notification_config: Dict[str, Any]) -> bool:
        """Envía notificación a Slack."""
        try:
            # Por ahora, solo logueamos la notificación de Slack
            # En el futuro se puede integrar con Slack API
            
            recipients = notification_config.get('recipients', '')
            message = notification_data.get('message', '')
            flow_id = notification_data.get('flow_id', '')
            notification_type = notification_data.get('notification_type', '')
            
            logger.info(f"💬 SLACK NOTIFICATION - Tipo: {notification_type}")
            logger.info(f"💬 SLACK NOTIFICATION - Canal: {recipients}")
            logger.info(f"💬 SLACK NOTIFICATION - Flujo: {flow_id}")
            logger.info(f"💬 SLACK NOTIFICATION - Mensaje: {message}")
            
            # TODO: Implementar envío real a Slack
            # Por ejemplo, usando Slack Webhook:
            # import requests
            # webhook_url = notification_config.get('webhook_url')
            # payload = {'text': message}
            # requests.post(webhook_url, json=payload)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error enviando notificación de Slack: {str(e)}")
            return False
    
    def _send_webhook_notification(self, notification_data: Dict[str, Any], notification_config: Dict[str, Any]) -> bool:
        """Envía notificación a un webhook."""
        try:
            # Por ahora, solo logueamos la notificación de webhook
            # En el futuro se puede implementar envío real
            
            webhook_url = notification_config.get('webhook_url', '')
            flow_id = notification_data.get('flow_id', '')
            notification_type = notification_data.get('notification_type', '')
            
            logger.info(f"🔗 WEBHOOK NOTIFICATION - Tipo: {notification_type}")
            logger.info(f"🔗 WEBHOOK NOTIFICATION - URL: {webhook_url}")
            logger.info(f"🔗 WEBHOOK NOTIFICATION - Flujo: {flow_id}")
            logger.info(f"🔗 WEBHOOK NOTIFICATION - Datos: {json.dumps(notification_data, indent=2)}")
            
            # TODO: Implementar envío real de webhook
            # import requests
            # response = requests.post(webhook_url, json=notification_data)
            # return response.status_code == 200
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error enviando notificación de webhook: {str(e)}")
            return False


