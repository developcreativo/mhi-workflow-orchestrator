#!/usr/bin/env python3
# -*- coding: utf8 -*-
import functions_framework
import json, os, datetime, base64
from cloudevents.http import CloudEvent
from google.cloud import pubsub_v1 as pubsub
from tests import test_fergus
#from dotenv import load_dotenv
#load_dotenv()

@functions_framework.cloud_event
def IniciarExtraccion(cloud_event: CloudEvent) -> None:
    try:
        data =__validate_event(cloud_event)
        _m_account=data[0] 
        datasets=data[1]
    
        # El proceso ha superado la validación de la peticion recibida
        print(f"Holis cuenta {_m_account}.")
        #print(f"Cargaremos estos datasets: {datasets}")
        test_fergus

    except Exception as e:
        msg=f'La funcion de ejecución IniciarExtraccion de UlysesCloud ha terminado en error:{type(e)}: {e}.'
        # enviar email
        topic_id   = 'notifications'
        project_id = str(os.environ.get('_M_PROJECT_ID'))
        contenido = f"Se ha producido un error en la integración, el punto del error es el siguiente:"
        contenido += os.linesep 
        contenido += f'{type(e)}: {e}'
        contenido += os.linesep 
        contenido += "Visita Google Cloud Logging para más información: https://console.cloud.google.com/logs"
        data = json.dumps({
            "_m_account":_m_account,
            "type":"S",
            "data":{
                "subject":f"Error en AVALON para la cuenta: {_m_account}",
                "text":contenido
            }
        }).encode('utf-8')
        publisher  = pubsub.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)
        future = publisher.publish(topic=topic_path, data=data)
        print(future.result())
        print(msg)

def __validate_event(event):
    try:
        # Validamos que el evento contenga el objeto data
        data = json.loads(base64.b64decode(event.data["message"]["data"]).decode())
        # Validamos que por cada tipo de notificacion, existan los valores obligatorios
        try: 
            _m_account = data['_m_account'] 
            if _m_account == None:
                raise ('_m_account tiene un valor vacío')
        except Exception as e:
            raise ('_m_account no llega como parámetro')
        try: 
            datasets = data['datasets'] 
            if _m_account == None:
                raise ('datasets tiene un valor vacío')
        except Exception as e:
            raise ('datasets no llega como parámetro')
    except Exception as e:
        msg=f'El proceso __validate_event ha fallado, error:{type(e)}: {e}.'
        print(msg)
        raise(msg)
    
    return _m_account, datasets
 
 
if __name__ == '__main__':
    ini = datetime.datetime.now()
    print("Inicio: ", ini)
    
    extraccion = avalon.Extraccion(_m_account='fergus')
    extraccion.Iniciar([
        "RECCardex"
    ])
    
    fin = datetime.datetime.now()
    print("Fin: ", fin)
    print("Duración: ", fin.timestamp()-ini.timestamp())
