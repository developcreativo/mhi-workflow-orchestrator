import base64
import json
from datetime import datetime
from worker import notifications
import functions_framework
from cloudevents.http import CloudEvent


@functions_framework.cloud_event
def notificationsWorker(cloud_event: CloudEvent):
    #print('Envío de Notificaciones: INICIO')
    try:
        data = __validate_event(cloud_event)
        # El proceso ha superado la validación de la peticion recibida
        _m_account=data[0] 
        _m_type=data[1]
        _m_data=data[2]

        client = notifications.Client()
        response = client.executeNotification(_m_account=_m_account, type=_m_type, parameters=_m_data)

        return response
    except BaseException as e:
        msg=f'El proceso notificationsWorker ha fallado: OS error:{type(e)}: {e}.'
        print(msg)
        raise Exception(msg)

def __validate_event(event):
    try:
        
        # Validamos que el evento contenga el objeto data

        data_aux = base64.b64decode(event.data["message"]["data"])
        data = json.loads(data_aux.decode('utf-8'))
        # Validamos que por cada tipo de notificacion, existan los valores obligatorios
        try: 
            _m_account = data['_m_account'] 
            if _m_account == None:
                raise ('_m_account tiene un valor vacío')
        except Exception as e:
            raise ('_m_account no llega como parámetro')
        try:
            _m_data = data['data']
        except Exception as e:
            raise ('data no llega como parámetro') 
        try: 
            _m_type = data['type']
            match _m_type:
                case 'S':
                    madatory_keys = ['subject','text']
                case 'N':
                    madatory_keys = ['subject','to_email','text']
                case 'P':
                    madatory_keys = ['subject','to_email','template','parameters']
            keys = [key for key in _m_data if key in madatory_keys]
            if keys != madatory_keys:
                msg = f'Error en la notificación para {_m_account} del tipo {_m_type} debe contener las siguientes claves: {madatory_keys} y tiene estas: {_m_data.keys()}'
                raise RuntimeError(msg)
        except Exception as e:
            raise ('type no llega como parámetro')
    except Exception as e:
        msg=f'El proceso __validate_event ha fallado, error:{type(e)}: {e}.'
        print(msg)
        raise(msg)
    
    return _m_account, _m_type, _m_data


if __name__ == "__main__":
    ini = datetime.now()
    print("Inicio: ", ini)

    event={
        'attributes': {
            'specversion': '1.0', 
            'id': '12252319714819340', 
            'source': '//pubsub.googleapis.com/projects/mind-hotel-insights-dev/topics/notifications', 
            'type': 'google.cloud.pubsub.topic.v1.messagePublished', 
            'datacontenttype': 'application/json', 
            'time': '2024-09-12T12:01:45.733Z'
            }, 
        'data': {
            'message': {
                'data': 'ewogICAgIl9tX2FjY291bnQiOiJzaWxrZW4iLAogICAgInR5cGUiOiJTIiwKICAgICJkYXRhIjp7CiAgICAgICAgInN1YmplY3QiOiAiRXJyb3IgZW4gZWwgcHJvY2VzYW1pZW50byBkZSBsYSBpbnRlZ3JhY2nDs24gVUxZU0VTIENMT1VEIiwKICAgICAgICAidGV4dCI6IkVsIGVycm9yIHNlIGhhIHByb2R1Y2lkbyBlbiBlbCBtZXRvZG8gX19yZXJlcmUgIgogICAgfQp9', 
                'messageId': '12252319714819340', 
                'message_id': '12252319714819340', 
                'publishTime': '2024-09-12T12:01:45.733Z', 
                'publish_time': '2024-09-12T12:01:45.733Z'}, 
                'subscription': 'projects/mind-hotel-insights-dev/subscriptions/eventarc-europe-southwest1-utilnotificationsworker-652482-sub-104'}
        }
    
    response = notificationsWorker(event)
    print(response.text)
    fin = datetime.now()
    print("Fin: ", fin)
    print("Duración: ", fin.timestamp()-ini.timestamp())