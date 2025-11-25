#!/usr/bin/env python3
# -*- coding: utf8 -*-
from worker import appreload
import functions_framework
import json, os, datetime, base64
from cloudevents.http import CloudEvent
from google.cloud import pubsub_v1 as pubsub
import logging
from core.config import config
from core.utils.message_utils import (
    decode_message_data,
    extract_identifiers,
    create_flow_notification_data,
    create_error_notification_data,
    create_success_notification_data,
)
from core.handlers.qlik_polling_handler import QlikPollingHandler
from utils.publisher import publish_message

logger = logging.getLogger(__name__)

# Inicializar handler de polling
qlik_polling_handler = QlikPollingHandler()

@functions_framework.cloud_event
def IniciarRecargaApp(cloud_event: CloudEvent) -> None:
    try:
        # Decodificar mensaje
        try:
            message_json = decode_message_data(cloud_event)
            # logger.info(f"✅ MENSAJE DECODIFICADO: {message_json}")
            # logger.info(f'MENSAJE RECIBIDO MSQLIKAPPRELOAD: {message_json}')
        except Exception as decode_error:
            raise
        
        # Extraer identificadores
        try:
            _m_account, flow_id, run_id, task_id = extract_identifiers(message_json)
        except Exception as extract_error:
            raise
        
        # Obtener parámetros específicos para recarga de app
        app_id = message_json.get('app_id')
     

        # El proceso ha superado la validación de la petición recibida
        recarga = appreload.RecargaApp(
            _m_account=_m_account
        )
        resultado = recarga.Iniciar(app_id)

        # Determinar éxito
        if isinstance(resultado, dict):
            reload_id = resultado.get('reload_id')
            result_payload = resultado.get('result', {})

            # Notificar al FlowController éxito
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
                        "result": result_payload,
                    },
                )
                message_id = publish_message(
                    project_id=config.PROJECT_ID,
                    topic_id='ms-flows-controller',
                    payload=flow_notification,
                )
                logger.info(f"Notificación de éxito enviada al FlowController: {message_id}")
            except Exception as e:
                logger.warning(f"No se pudo notificar éxito al FlowController: {e}")

    except Exception as e:
        # Notificar error a FlowController
        try:
            # Reintentar decodificar para obtener contexto básico
            message_json = decode_message_data(cloud_event)
            _m_account, flow_id, run_id, task_id = extract_identifiers(message_json)
        except Exception:
            _m_account = None
            flow_id = run_id = task_id = None

        # Notificar FlowController
        try:
            flow_notification = create_flow_notification_data(
                flow_id=flow_id or '',
                run_id=run_id or '',
                task_id=task_id or '',
                account=_m_account or '',
                step=config.TRIGGER_TOPIC,
                status='error',
                error=str(e),
            )
            publish_message(
                project_id=config.PROJECT_ID,
                topic_id='ms-flows-controller',
                payload=flow_notification,
            )
        except Exception as notify_error:
            logger.error(f"No se pudo enviar notificación de error al FlowController: {notify_error}")

@functions_framework.cloud_event
def PollingEstadoRecargaQlik(cloud_event: CloudEvent) -> dict:
    """
    Punto de entrada para polling de estado de recarga de Qlik.
    Monitorea el estado de una recarga de Qlik hasta que termine.
    
    Args:
        cloud_event: Evento de Cloud Function con los datos del mensaje
        
    Returns:
        dict: Resultado del polling
    """
    logger.info(f'INICIO POLLING DE ESTADO RECARGA QLIK EN LA FUNCION MSQLIKAPPRELOAD.')
    return qlik_polling_handler.handle_polling_request(cloud_event)

def __validate_event(event):
    try:
        # Validamos que el evento contenga el objeto data
        data = json.loads(base64.b64decode(event.data["message"]["data"]).decode())
        # Validamos que por cada tipo de notificación, existan los valores obligatorios
        try: 
            _m_account = data['_m_account'] 
            if _m_account == None:
                raise ('_m_account tiene un valor vacío')
        except Exception as e:
            raise ('_m_account no llega como parámetro')
        try: 
            app_id = data['app_id'] 
            if app_id == None:
                raise ('app_id tiene un valor vacío')
        except Exception as e:
            raise ('app_id no llega como parámetro')
        try: 
            reload_token = data['reload_token'] 
            if reload_token == None:
                raise ('reload_token tiene un valor vacío')
        except Exception as e:
            raise ('reload_token no llega como parámetro')
    except Exception as e:
        msg=f'El proceso __validate_event ha fallado, error:{type(e)}: {e}.'
        print(msg)
        raise(msg)
    
    return _m_account, app_id, reload_token

if __name__ == '__main__':
    ini = datetime.datetime.now()
    print("Inicio: ", ini)
    reload_token = ''
    recarga = appreload.RecargaApp(_m_account='silken')
    recarga.Iniciar(
        "app-id-example", reload_token 
    )
    
    fin = datetime.datetime.now()
    print("Fin: ", fin)
    print("Duración: ", fin.timestamp()-ini.timestamp())
















