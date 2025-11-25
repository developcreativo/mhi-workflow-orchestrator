import base64,json,logging, requests,sys
import functions_framework
from worker.madisa import Extraer
from utils.controller import Controller
from utils.publisher import send_to_flows_controller
# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

__EXTRACTOR_NAME = 'MADISA'

def validate_message(message):
    """Validate the incoming Pub/Sub message"""
    required_fields = ["_m_account", "datasets"]
    
    for field in required_fields:
        if field not in message:
            raise ValueError(f"Required field '{field}' missing from message")
    
    if not message["datasets"]:
        raise ValueError("No datasets specified in the message")
        
    return True

def get_external_ip():
    try:
        # Intenta obtener la IP pública de salida
        response = requests.get('http://ifconfig.me')
        external_ip = response.text.strip()
        return external_ip
    except Exception as e:
        return f"Error al obtener la IP externa: {e}", 500
        
@functions_framework.cloud_event
def IniciarExtraccion(cloud_event):
    """Cloud Function entry point"""
    try:
        get_external_ip()
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
        _m_account  = message.get("_m_account")
        datasets    = message.get("datasets", [])
        start_date  = message.get("start_date")
        end_date    = message.get("end_date")
        companyIds  = message.get("companyIds", [])
        flow_id     = message.get("flow_id", None)
        run_id      = message.get("run_id", None)
        task_id     = message.get("task_id", None)
        
        # Initialize the extractor
        extractor = Extraer(_m_account=_m_account, 
                           start_date=start_date, 
                           end_date=end_date, 
                           companies=companyIds)
        
        # Start extraction
        results = extractor.Iniciar(datasets)

        # Enviar mensaje a Data transformation-- 
        
        # Verificar si todos los datasets se procesaron exitosamente
        failed_datasets = [r for r in results if r.get("status") == "error"]
        if failed_datasets:
            error_message = f"Failed datasets: {[d['dataset'] for d in failed_datasets]}"
            send_to_flows_controller(flow_id, _m_account, run_id, task_id, "failed", error_message)
        else:
            send_to_flows_controller(flow_id, _m_account, run_id, task_id, "completed")
        
    except BaseException as e:
        msg=f'La funcion de ejecución IniciarExtraccion de Madisa ha terminado en error:{type(e)}: {e}.'
        logger.error(msg)
        
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
            logger.error(f"Error enviando fallo a flows controller: {flows_error}")
        

# Código para pruebas locales
if __name__ == "__main__":
    # Configuración de variables de entorno para pruebas locales
 
    # Simular un mensaje de Pub/Sub
    test_message = {
        "_m_account": "cursach",
        "datasets": ["MainTrans","ItemPos","DiscountPos","PayPos","VatPos","CardPos"]
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
    
    logger.info("Iniciando prueba local del extractor Madisa...")
    logger.info(f"Mensaje de prueba: {json.dumps(test_message, indent=2)}")
    
    # Llamar a la función
    try:
        result = IniciarExtraccion(mock_cloud_event)
        logger.info("Resultado de la extracción:")
        logger.info(json.dumps(result, indent=2))
    except Exception as e:
        logger.error(f"Error durante la prueba: {str(e)}")