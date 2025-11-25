#!/usr/bin/env python3
# -*- coding: utf8 -*-
import json, os, datetime, base64, logging
import functions_framework
from cloudevents.http import CloudEvent
from google.cloud import pubsub_v1 as pubsub
from worker import ulysescloud
from utils.publisher import send_to_flows_controller

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
__EXTRACTOR_NAME = 'ulysescloud'


@functions_framework.cloud_event
def IniciarExtraccion(cloud_event: CloudEvent) -> None:
    try:
        # Get message data from Pub/Sub Cloud Event
        if cloud_event.data:
            event = cloud_event.data
            if 'message' in event and 'data' in event['message']:
                pubsub_message = base64.b64decode(event['message']['data']).decode('utf-8')
                message = json.loads(pubsub_message)
                logger.info(f"Received message: {json.dumps(message, indent=2)}")
            else:
                raise ValueError("No data in Pub/Sub message")
        else:
            raise ValueError("No data in Cloud Event")
        # Validate the message
        validate_message(message)
       
        # Extraer datos según el formato del mensaje
        if "config" in message:
            # Mensaje del flow controller
            config = message["config"]
            _m_account = config.get("_m_account")
            connection_id = config.get("connection_id")
            datasets = config.get("datasets", [])
            flow_id = message.get("flow_id", None)
            task_id = message.get("task_id", None)
            run_id = message.get("run_id", None)
            start_date = config.get("start_date", None)
            end_date = config.get("end_date", None)
            hotel_IDs = config.get("hotel_IDs", None)
        else:
            # Mensaje directo (formato original)
            _m_account = message.get("_m_account")
            connection_id = message.get("connection_id")
            datasets = message.get("datasets", [])
            flow_id = message.get("flow_id", None)
            task_id = message.get("task_id", None)
            run_id = message.get("run_id", None)
            start_date = message.get("start_date", None)
            end_date = message.get("end_date", None)
            hotel_IDs = message.get("hotel_IDs", None)
     
        # El proceso ha superado la validación de la peticion recibida
        extraccion = ulysescloud.Extraccion(
            _m_account=_m_account,
            connection_id =connection_id,
            flow_id =flow_id,
            task_id = task_id,
            run_id = run_id,
            hotel_IDs=hotel_IDs,
            start_date=start_date
            )
        # Iniciar la extracción y capturar el resultado
        result = extraccion.Iniciar(datasets)
        
        if result:
            try:
                message_id = send_to_flows_controller(flow_id, _m_account, run_id, task_id, "completed")
            except Exception as e:
                print(f"Error enviando estado 'completed' al flows controller: {e}")
        else:
            try:
                message_id = send_to_flows_controller(flow_id, _m_account, run_id, task_id, "failed", "Extraction returned False")
            except Exception as e:
                print(f"Error enviando estado 'failed' al flows controller: {e}")

    except BaseException as e:
        msg=f'La funcion de ejecución IniciarExtraccion de UlysesCloud ha terminado en error:{type(e)}: {e}.'
        
        # Enviar estado de fallo al flows controller
        try:
            # Intentar obtener las variables del contexto del mensaje
            try:
                message_data = json.loads(base64.b64decode(cloud_event.data["message"]["data"]).decode())
                if "config" in message_data:
                    flow_id = message_data.get("flow_id", None)
                    task_id = message_data.get("task_id", None)
                    run_id = message_data.get("run_id", None)
                    _m_account = message_data["config"].get("_m_account", "unknown")
                else:
                    flow_id = message_data.get("flow_id", None)
                    task_id = message_data.get("task_id", None)
                    run_id = message_data.get("run_id", None)
                    _m_account = message_data.get("_m_account", "unknown")
            except:
                flow_id = None
                task_id = None
                run_id = None
                _m_account = "unknown"
            
            send_to_flows_controller(flow_id, _m_account, run_id, task_id, "failed", str(e))
        except Exception as flows_error:
            logger.error(f"Error enviando fallo a flows controller: {flows_error}")
        

def validate_message(message):
    try:
        """Validate the incoming Pub/Sub message"""
        # El flow controller envía el mensaje con estructura de step
        # Necesitamos extraer los campos del config
        if "config" in message:
            # Mensaje del flow controller
            config = message["config"]
            required_fields = ["_m_account","connection_id", "datasets"]
            
            for field in required_fields:
                if field not in config:
                    raise ValueError(f"Required field '{field}' missing from config")
            
            if not config["datasets"]:
                raise ValueError("No datasets specified in the config")
        else:
            # Mensaje directo (formato original)
            required_fields = ["_m_account","connection_id", "datasets"]
            
            for field in required_fields:
                if field not in message:
                    raise ValueError(f"Required field '{field}' missing from message")
            
            if not message["datasets"]:
                raise ValueError("No datasets specified in the message")
            
        return True
       
    except Exception as e:
        msg=f'El proceso validate_message ha fallado, error:{type(e)}: {e}.'
        print(msg)
        raise Exception(msg)
 
if __name__ == "__main__":
    ini = datetime.datetime.now()
    print("Inicio: ", ini)
    # Simular un mensaje de Pub/Sub
    
    test_message = {
        "_m_account": "silken",
        "connection_id":"silken_ulysescloud",
        "datasets": [
            "property.invoice",
            ]
    }

    # Codificar el mensaje como lo haría Pub/Sub
    encoded_message = base64.b64encode(json.dumps(test_message).encode("utf-8")).decode("utf-8")
    
    # Crear un objeto similar a un Cloud Event
    mock_cloud_event = type('obj', (object,), {
        'data': {
            'message': {
                'data': encoded_message
            }
        }
    })
    
    print("Iniciando prueba local del extractor Ulyses Cloud...")
    print(f"Mensaje de prueba: {json.dumps(test_message, indent=2)}")
    
    # Llamar a la función
    try:
        result = IniciarExtraccion(mock_cloud_event)
        #print("Resultado de la extracción:")
        #print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"Error durante la prueba: {str(e)}")
    
    fin = datetime.datetime.now()
    print("Fin: ", fin)
    print("Duración: ", fin.timestamp()-ini.timestamp())
