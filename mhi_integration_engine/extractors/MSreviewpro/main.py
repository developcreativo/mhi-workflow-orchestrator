#!/usr/bin/env python3
import json, os, datetime, base64
import functions_framework
from cloudevents.http import CloudEvent
from google.cloud import pubsub_v1 as pubsub
from worker import reviewpro
from utils.publisher import send_to_flows_controller


@functions_framework.cloud_event
def IniciarExtraccion(cloud_event: CloudEvent) -> None:
    try:
        data =__validate_event(cloud_event)
        _m_account=data[0] 
        datasets=data[1]
        flow_id=data[2]
        run_id=data[3]
        task_id=data[4]
    
        # El proceso ha superado la validación de la peticion recibida
        print(f"Holis cuenta {_m_account}.")
        extraccion = reviewpro.Extraccion(_m_account=_m_account)
        result = extraccion.Iniciar(datasets)
        
        if result:
            send_to_flows_controller(flow_id, _m_account, run_id, task_id, "completed")
            print("fin de la ejecución - ÉXITO")
        else:
            send_to_flows_controller(flow_id, _m_account, run_id, task_id, "failed", "Extraction returned False")
            print("fin de la ejecución - FALLO")

    except Exception as e:
        msg=f'La funcion de ejecución IniciarExtraccion de ReviewPro ha terminado en error:{type(e)}: {e}.'
        print(msg)
        
        # Enviar estado de fallo al flows controller
        try:
            # Intentar obtener las variables del contexto del mensaje
            try:
                message_data = json.loads(base64.b64decode(cloud_event.data["message"]["data"]).decode())
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
            print(f"Error enviando fallo a flows controller: {flows_error}")
        

def __validate_event(event):
    try:
        # Validamos que el evento contenga el objeto data
        data = json.loads(base64.b64decode(event.data["message"]["data"]).decode())
        # Validamos que por cada tipo de notificacion, existan los valores obligatorios
        try: 
            _m_account = data['_m_account'] 
            if _m_account == None:
                raise ValueError('_m_account tiene un valor vacío')
        except Exception as e:
            raise ValueError('_m_account no llega como parámetro')
        try: 
            datasets = data['datasets'] 
            if datasets == None:
                raise ValueError('datasets tiene un valor vacío')
        except Exception as e:
            raise ValueError('datasets no llega como parámetro')
        
        try: 
            flow_id = data.get('flow_id', None)
        except Exception as e:
            flow_id = None
        try: 
            run_id = data.get('run_id', None)
        except Exception as e:
            run_id = None
        try: 
            task_id = data.get('task_id', None)
        except Exception as e:
            task_id = None
            
    except Exception as e:
        msg=f'El proceso __validate_event ha fallado, error:{type(e)}: {e}.'
        print(msg)
        raise Exception(msg)
    
    return _m_account, datasets, flow_id, run_id, task_id
 
 
if __name__ == '__main__':
    ini = datetime.datetime.now()
    print("Inicio: ", ini)
    
    extraccion = reviewpro.Extraccion(_m_account='fergus')
    extraccion.Iniciar([
        "lodging"
    ])
    
    fin = datetime.datetime.now()
    print("Fin: ", fin)
    print("Duración: ", fin.timestamp()-ini.timestamp())
