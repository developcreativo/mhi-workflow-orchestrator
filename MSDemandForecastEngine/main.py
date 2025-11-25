import base64, json, logging
import functions_framework
from worker.demandforecast import Forecaster
from utils.controller import Controller
from utils.publisher import publish_message

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

__EXTRACTOR_NAME = 'MSDemandForecastEngine'

def validate_message(message):
    """Validate the incoming Pub/Sub message"""
    required_fields = ["_m_account"]
    
    for field in required_fields:
        if field not in message:
            raise ValueError(f"Required field '{field}' missing from message")
    return True


@functions_framework.cloud_event
def IniciarForecast(cloud_event):
    """Cloud Function entry point"""
   # _m_account = 'fergus'  # Initialize outside try block
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
        idHotel    = message.get("idHotel")
        flow_id    = message.get("flow_id")
        run_id     = message.get("run_id")
        task_id    = message.get("task_id")
        
        # Initialize the extractor
        print(f"INICIO de ejecución de {__EXTRACTOR_NAME} para la cuenta {_m_account}")
        forecaster = Forecaster(_m_account=_m_account, 
                           idHotel=idHotel)
        # Start extraction
        results = forecaster.iniciar_forecast(_m_account=_m_account)
        
        # Enviar callback de éxito al MSFlowsController
        print(f"FIN de ejecución de forecast de {__EXTRACTOR_NAME} para la cuenta {_m_account}")
        
        if flow_id and run_id and task_id:
            try:
                # Crear notificación de éxito para el FlowController
                flow_notification = {
                    "flow_id": flow_id,
                    "run_id": run_id,
                    "task_id": task_id,
                    "account": _m_account,
                    "step": "ms-demand-forecast-engine",
                    "status": "completed",
                    "result": {
                        "idHotel": idHotel,
                        "forecast_result": results
                    },
                    "timestamp": message.get("timestamp")
                }
                
                # Enviar notificación al FlowController
                publish_message("ms-flows-controller", flow_notification)
                logger.info(f"✅ Callback de éxito enviado al MSFlowsController para flow_id={flow_id}, task_id={task_id}")
                
            except Exception as callback_error:
                logger.error(f"❌ Error enviando callback de éxito: {callback_error}")
        else:
            logger.warning("⚠️ No se envió callback: faltan flow_id, run_id o task_id en el mensaje")
        
        return results
    except BaseException as e:
            account_info = _m_account if '_m_account' in locals() else "desconocida"
            flow_id = locals().get('flow_id')
            run_id = locals().get('run_id')
            task_id = locals().get('task_id')
            # Enviar callback de error al MSFlowsController si tenemos los IDs del flujo
            if flow_id and run_id and task_id:
                try:
                    error_notification = {
                        "flow_id": flow_id,
                        "run_id": run_id,
                        "task_id": task_id,
                        "account": account_info,
                        "step": "ms-demand-forecast-engine",
                        "status": "failed",
                        "error": str(e),
                        "timestamp": locals().get('message', {}).get("timestamp")
                    }
                    
                    publish_message("ms-flows-controller", error_notification)
                    logger.error(f"❌ Callback de error enviado al MSFlowsController para flow_id={flow_id}, task_id={task_id}")
                    
                except Exception as callback_error:
                    logger.error(f"❌ Error enviando callback de error: {callback_error}")
            else:
                logger.warning("⚠️ No se envió callback de error: faltan flow_id, run_id o task_id")
            
            # controller.putNotification(ptype='S', subject=subject, body=body)
            # controller.putNotification(ptype='S', subject=subject, body=body)
            

# Código para pruebas locales
if __name__ == "__main__":
    # Configuración de variables de entorno para pruebas locales
 
    # Simular un mensaje de Pub/Sub
    test_message = {
        "_m_account": "fergus",
    # "idHotel": ["CAREMA SPLASH"] 
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
    
    print("Iniciando prueba local del forecaster...")
    print(f"Mensaje de prueba: {json.dumps(test_message, indent=2)}")
    
    # Llamar a la función
    try:
        result = IniciarForecast(mock_cloud_event)
    except Exception as e:
        print(f"Error durante la prueba: {str(e)}")