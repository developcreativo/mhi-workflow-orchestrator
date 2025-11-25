import base64
import json
import logging
import functions_framework
from utils.publisher import send_to_flows_controller
from worker.axional import Extraer

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
__EXTRACTOR_NAME = 'axional'

def validate_message(message):
    """Validate the incoming Pub/Sub message"""
    required_fields = ["_m_account", "datasets"]
    
    for field in required_fields:
        if field not in message:
            raise ValueError(f"Required field '{field}' missing from message")
    
    if not message["datasets"]:
        raise ValueError("No datasets specified in the message")
        
    return True

@functions_framework.cloud_event
def IniciarExtraccion(cloud_event):
    """Cloud Function entry point"""
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
        # Get parameters from the message
        _m_account = message.get("_m_account")
        datasets = message.get("datasets", [])
        start_date = message.get("start_date", None)
        end_date = message.get("end_date", None)
        companyIds = message.get("companyIds", [])
        flow_id = message.get("flow_id", None)
        run_id = message.get("run_id", None)
        task_id = message.get("task_id", None)
        # Initialize the extractor
        extractor = Extraer(_m_account=_m_account,start_date=start_date,end_date=end_date,companyIds=companyIds)
        # Start extraction
        result = extractor.Iniciar(datasets)
        
        if result:
            send_to_flows_controller(flow_id, _m_account, run_id, task_id, "completed")
            # return {"success": True, "message": "Extraction completed", "datasets": datasets}
        else:
            send_to_flows_controller(flow_id, _m_account, run_id, task_id, "failed", "Extraction failed")
        
    except Exception as e:
        send_to_flows_controller(flow_id, _m_account, run_id, task_id, "failed", str(e))
        
# Código para pruebas locales
if __name__ == "__main__":
    # Simular un mensaje de Pub/Sub
    test_message = {
        "_m_account": "cursach",
        "datasets": [
            "ApuntesContables"
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
    
    # Llamar a la función
    try:
        result = IniciarExtraccion(mock_cloud_event)
        print("Resultado de la extracción:")
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"Error durante la prueba: {str(e)}")