#!/usr/bin/env python3
# -*- coding: utf8 -*-
from worker import holded
import functions_framework
import json, os, datetime, base64
from cloudevents.http import CloudEvent
from utils.publisher import send_to_flows_controller
import logging
logger = logging.getLogger(__name__)

@functions_framework.cloud_event
def IniciarExtraccion(cloud_event: CloudEvent) -> None:
    try:
        data =__validate_event(cloud_event)
        _m_account=data[0] 
        datasets=data[1]
        connection_id=data[2]
        flow_id=data[3]
        run_id=data[4]
        task_id=data[5]
    
        # El proceso ha superado la validación de la peticion recibida
        print(f"Holis cuenta {_m_account}.")
        extraccion = holded.Extraccion(_m_account=_m_account, connection_id=connection_id   )
        result = extraccion.Iniciar(datasets)
        if result:
            send_to_flows_controller(flow_id, _m_account, run_id, task_id, "completed")
            print("fin de la ejecución - ÉXITO")
        else:
            send_to_flows_controller(flow_id, _m_account, run_id, task_id, "failed", "Extraction returned False")
            print("fin de la ejecución - FALLO")

    except Exception as e:
        msg=f'La funcion de ejecución IniciarExtraccion de Holded ha terminado en error:{type(e)}: {e}.'
        print(msg)
        
        # Enviar estado de fallo al flows controller
        try:
            # Intentar obtener las variables del contexto del mensaje
            try:
                message_data = json.loads(base64.b64decode(cloud_event.data["message"]["data"]).decode())
                if "config" in message_data:
                    flow_id = message_data.get("flow_id", None)
                    task_id = message_data.get("task_id", None)
                    run_id = message_data.get("run_id", None)
                    _m_account = message_data["config"].get("_m_account") or message_data["config"].get("account") or message_data.get("account", "unknown")
                else:
                    flow_id = message_data.get("flow_id", None)
                    task_id = message_data.get("task_id", None)
                    run_id = message_data.get("run_id", None)
                    _m_account = message_data.get("_m_account") or message_data.get("account", "unknown")
            except:
                flow_id = None
                task_id = None
                run_id = None
                _m_account = "unknown"
            
            send_to_flows_controller(flow_id, _m_account, run_id, task_id, "failed", str(e))
        except Exception as flows_error:
            print(f"Error enviando fallo a flows controller: {flows_error}")

def __validate_event(event):
    try:
        # Validamos que el evento contenga el objeto data
        data = json.loads(base64.b64decode(event.data["message"]["data"]).decode())
        print(f"data: {data}")
        logger.info(f"data: {data}")
        
        # El flow controller envía el mensaje con estructura de step
        # Necesitamos extraer los campos del config si existe
        if "config" in data:
            # Mensaje del flow controller
            config = data["config"]
            _m_account = config.get("_m_account") or config.get("account") or data.get("account")
            connection_id = config.get("connection_id")
            datasets = config.get("datasets", [])
            flow_id = data.get("flow_id", None)
            run_id = data.get("run_id", None)
            task_id = data.get("task_id", None)
        else:
            # Mensaje directo (formato original)
            _m_account = data.get("_m_account") or data.get("account")
            connection_id = data.get("connection_id")
            datasets = data.get("datasets", [])
            flow_id = data.get("flow_id", None)
            run_id = data.get("run_id", None)
            task_id = data.get("task_id", None)
        
        # Validamos que por cada tipo de notificacion, existan los valores obligatorios
        if _m_account == None:
            raise ValueError('account no llega como parámetro')
        if connection_id == None:
            raise ValueError('connection_id no llega como parámetro')
        if datasets == None or len(datasets) == 0:
            raise ValueError('datasets no llega como parámetro o está vacío')
        if flow_id == None:
            raise ValueError('flow_id no llega como parámetro')
            
    except ValueError as e:
        # Re-lanzar ValueError directamente
        raise
    except Exception as e:
        msg=f'El proceso __validate_event ha fallado, error:{type(e)}: {e}.'
        print(msg)
        raise Exception(msg)
    
    return _m_account, datasets, connection_id, flow_id, run_id, task_id 
 
 
if __name__ == '__main__':
    ini = datetime.datetime.now()
    print("Inicio: ", ini)
    
    extraccion = holded.Extraccion(_m_account='mind', connection_id="mind_holded")
    extraccion.Iniciar([
        "accounting.dailyledger"
    ])
    
    fin = datetime.datetime.now()
    print("Fin: ", fin)
    print("Duración: ", fin.timestamp()-ini.timestamp())
