
import base64,json,logging
import functions_framework
from utils.controller import Controller
from utils.publisher import send_to_flows_controller  # pyright: ignore[reportMissingImports]
from worker.kpiengine import CalculoKPIs

# Set up logging
#logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

@functions_framework.cloud_event
def IniciarCalculo(cloud_event):
    """
    Cloud Function entry point to evaluate KPIs for a given hotel and month.
    Expects a Pub/Sub message with: _m_account, CodigoHotel, idPeriodo (format AAAAMM).
    """
    try:
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

        account = message.get("_m_account")
        idReporte = message.get("idReporte")
        # CodigoHotel = message.get("CodigoHotel",None)                     -----> AL FINAL NO LO HACEMOS
        idPeriodo = message.get("idPeriodo",None)
        escenario = message.get("escenario", "ACTUAL")
        
        # Extraer información del flujo para comunicación con FlowController
        flow_id = message.get("flow_id")
        run_id = message.get("run_id")
        task_id = message.get("task_id")
        
        if not account or not idReporte:
            raise ValueError("Missing one or more required parameters: '_m_account', 'idReporte'")

        engine = CalculoKPIs(account, idReporte, escenario)

        result = engine.Iniciar(idPeriodo)
        
        if result and result.get("status") == "success":
            msg = f"Success: FIN KPI Engine para cuenta={account}, período={idPeriodo}"
            
            # Comunicar éxito al FlowController si está disponible la información del flujo
            if flow_id and run_id and task_id:
                try:
                    send_to_flows_controller(flow_id, account, run_id, task_id, "completed")
                    logger.info(f"[KPI-ENGINE] Success reported to ms-flows-controller for flow_id={flow_id}, task_id={task_id}")
                except Exception as flow_error:
                    logger.warning(f"[KPI-ENGINE] Error communicating success to ms-flows-controller: {flow_error}")
            
            return {"status": "success", "message": msg}
        else:
            original_msg = result.get("message", "") if result else "Sin respuesta"
            msg = f"Warning: FIN KPI Engine para cuenta={account}, período={idPeriodo}. {original_msg}"  
            logger.warning(msg)
            
            # Comunicar fallo al FlowController si está disponible la información del flujo
            if flow_id and run_id and task_id:
                try:
                    send_to_flows_controller(flow_id, account, run_id, task_id, "failed", original_msg)
                    logger.info(f"[KPI-ENGINE] Failure reported to ms-flows-controller for flow_id={flow_id}, task_id={task_id}")
                except Exception as flow_error:
                    logger.warning(f"[KPI-ENGINE] Error communicating failure to FlowController: {flow_error}")
            
            raise Exception(msg)

    except Exception as e:
        # 3. Capturar cualquier error que haya ocurrido, incluido el AttributeError original.
        error_msg = f"Error: FIN KPI Engine para cuenta={account}, período={idPeriodo}, al intentar procesar el resultado de IniciarCalculo: {str(e)}"
        logger.error(error_msg)
        
        # Comunicar fallo al FlowController si está disponible la información del flujo
        if 'flow_id' in locals() and 'run_id' in locals() and 'task_id' in locals():
            if flow_id and run_id and task_id:
                try:
                    send_to_flows_controller(flow_id, account, run_id, task_id, "failed", str(e))
                    logger.info(f"[KPI-ENGINE] Exception failure reported to FlowController for flow_id={flow_id}, task_id={task_id}")
                except Exception as flow_error:
                    logger.warning(f"[KPI-ENGINE] Error communicating exception failure to FlowController: {flow_error}")
        
        # controller = Controller(account, "KPIEngine")
        # subject = f"La evaluación de KPI para {account} ha fallado"
        # body = f"Se ha producido un error:<br><br>tipo: {type(e)}, error: {e}.<br><br>"
        # body += "Visita Google Cloud Logging para más información: https://console.cloud.google.com/logs.<br>"
        # controller.putNotification(ptype='S', subject=subject, body=body)
        # raise Exception(error_msg)
        

# Ejecución local
if __name__ == "__main__":
    test_message = {
        "_m_account": "fergus",
        "idReporte": "CGES",
        # "CodigoHotel": ["BAH", "PLC"],
        # "idPeriodo": 202503,
        # "escenario": "PRESUPUESTO",
        # Información del flujo para testing
        "flow_id": "test-flow-id",
        "run_id": "test-run-id", 
        "task_id": "test-task-id"
    }

    encoded_message = base64.b64encode(json.dumps(test_message).encode("utf-8")).decode("utf-8")

    mock_cloud_event = type('obj', (object,), {
        'data': {
            'message': {
                'data': encoded_message
            }
        }
    })

    print("Iniciando evaluación KPI local...")
    IniciarCalculo(mock_cloud_event)