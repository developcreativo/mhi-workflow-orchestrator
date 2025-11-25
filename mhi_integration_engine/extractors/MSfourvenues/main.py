import base64
import json
import logging
import functions_framework
from worker.fourvenues import Extraer
from utils.publisher import send_to_flows_controller
# Set up logging
#logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
__EXTRACTOR_NAME = 'fourvenues'

def validate_message(message):
    """Validate the incoming Pub/Sub message"""
    required_fields = ["_m_account","datasets","connection_id"]

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
        connection_id = message.get("connection_id")
        datasets = message.get("datasets")
        flow_id = message.get("flow_id", None)
        run_id = message.get("run_id",None)
        task_id = message.get("task_id",None)
        start_date = message.get("start_date",None)
        end_date = message.get("end_date",None)
        events = message.get("events",[]) # lista de IDs de eventos a extraer

        # Initialize the extractor
        extractor = Extraer(
            _m_account=_m_account,
            connection_id=connection_id,
            extractor_name = __EXTRACTOR_NAME,
            flow_id=flow_id,
            run_id=run_id,
            task_id=task_id,
            start_date=start_date,
            end_date=end_date,
            events=events
            )
        # Start extraction
        result = extractor.Iniciar(datasets)
        
        if result:
            # return {"success": True, "message": "Extraction completed", "datasets": datasets}
            send_to_flows_controller(flow_id, _m_account, run_id, task_id, "completed")
        else:
            send_to_flows_controller(flow_id, _m_account, run_id, task_id, "failed", "Extraction failed")
        
    except Exception as e:
        send_to_flows_controller(flow_id, _m_account, run_id, task_id, "failed", str(e))
        
# Código para pruebas locales
if __name__ == "__main__":
    # Simular un mensaje de Pub/Sub
    test_message = {
        "_m_account": "cursach",
        "connection_id": "cursach_fourvenues",
             "datasets": [
                "channels",
                "users"]
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
    
    logger.info("Iniciando prueba local del extractor Forvenues...")
    logger.info(f"Mensaje de prueba: {json.dumps(test_message, indent=2)}")
    
    # Llamar a la función
    try:
        result = IniciarExtraccion(mock_cloud_event)
        logger.info("Resultado de la extracción:")
        logger.info(json.dumps(result, indent=2))
    except Exception as e:
        logger.error(f"Error durante la prueba: {str(e)}")