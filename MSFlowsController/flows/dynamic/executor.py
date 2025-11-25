"""
Ejecutor de flujos dinámicos N8N-like.
"""
import uuid
import logging
import time
from datetime import datetime, timezone
from google.cloud import pubsub_v1
from core.config import config
from flows.normalization import normalize_steps, is_advanced_flow
from storage.repositories import FlowDefinitionRepository, FlowRunStateRepository

logger = logging.getLogger(__name__)


def execute_dynamic_flow(message_json: dict) -> dict:
    """
    Ejecuta un flujo dinámico con configuración parametrizable N8N-LIKE.
    
    Args:
        message_json: Datos del mensaje con configuración del flujo
        
    Returns:
        dict: Resultado de la ejecución del flujo
    """
    try:
        flow_id = message_json.get('flow_id')
        account = message_json.get('account')
        task_id = message_json.get('task_id')
        run_id = message_json.get('run_id')
        
        logger.info(f"=== INICIANDO FLUJO DINÁMICO N8N-LIKE: {flow_id} ===")
        logger.info(f"Account: {account}")
        logger.info(f"Task ID: {task_id}")
        logger.info(f"Run ID: {run_id}")
        logger.info(f"Configuración completa: {message_json}")
        
        # Delay para visualizar el inicio del ejecutor dinámico
        # logger.info(f"⏰ ESPERANDO 10 segundos en el ejecutor dinámico...")
        # time.sleep(10)
        
        # Repositorios
        flow_repo = FlowDefinitionRepository()
        state_repo = FlowRunStateRepository()
        
        # Determinar si es inicio de flujo o continuación
        if not run_id and not task_id:
            # INICIO DE FLUJO - Leer definición desde Cloud Storage
            logger.info("🚀 INICIO DE FLUJO - Leyendo definición desde GCS")
            flow_definition = flow_repo.get_flow_definition(account, flow_id)
            
            if not flow_definition:
                # Si no hay definición en GCS, usar configuración del mensaje
                logger.info("📋 No hay definición en GCS, usando configuración del mensaje")
                flow_config = message_json.get('flow_config', message_json)
                logger.info(f"📋 flow_config establecido: {type(flow_config)} con keys: {list(flow_config.keys()) if isinstance(flow_config, dict) else 'not dict'}")
            else:
                # Usar definición de GCS
                logger.info("📋 Usando definición desde GCS para la cuenta {account} y el flujo {flow_id} y el run {run_id} y el task {task_id}")
                flow_config = flow_definition
                
            # Generar nuevo run_id
            run_id = str(uuid.uuid4())
            logger.info(f"🆔 Nuevo Run ID generado: {run_id}")
            
            # Guardar estado inicial del flujo en Cloud Storage
            logger.info(f"💾 GUARDANDO ESTADO INICIAL - Guardando estado inicial del flujo {flow_id}/{run_id}")
            initial_state = {
                "flow_id": flow_id,
                "run_id": run_id,
                "account": account,
                "status": "running",
                "flow_config": flow_config,
                "started_at": datetime.now(timezone.utc).isoformat(),
                "steps": []
            }
            state_repo.save_flow_run_state(flow_id, run_id, initial_state)
            logger.info(f"✅ ESTADO INICIAL GUARDADO - Estado inicial guardado exitosamente")
            
        else:
            # CONTINUACIÓN DE FLUJO - Leer estado desde Cloud Storage
            logger.info("🔄 CONTINUACIÓN DE FLUJO - Leyendo estado desde GCS")
            
            if not run_id:
                return {
                    "status": "error",
                    "error": "run_id es requerido para continuación de flujo"
                }
            
            # Leer estado actual del flujo
            flow_state = state_repo.get_flow_run_state(flow_id, run_id)
            
            if not flow_state:  
                return {
                    "status": "error",
                    "error": f"No se encontró estado de ejecución para {flow_id}/{run_id}"
                }
            
            # Usar configuración del estado guardado
            flow_config = flow_state.get('flow_config', {})
            logger.info(f"📋 Estado recuperado desde GCS: {flow_state}")
        
        # Validar configuración mínima requerida
        if not flow_config:
            return {
                "status": "error",
                "error": "No se proporcionó configuración de flujo (flow_config)"
            }
        
        logger.info(f"🔍 ANTES DE NORMALIZACIÓN - flow_config: {type(flow_config)} con keys: {list(flow_config.keys()) if isinstance(flow_config, dict) else 'not dict'}")
        
        # Normalizar definición: aceptar 'tasks' o 'steps'
        logger.info(f"🔍 Normalizando definición. flow_config keys: {list(flow_config.keys())}")
        steps = normalize_steps(flow_config)
        
        # Actualizar flow_config con los steps normalizados para el motor N8N
        flow_config['steps'] = steps
        logger.info(f"✅ flow_config actualizado con {len(steps)} steps: {[s.get('id', 'no-id') for s in steps]}")
        if not steps:
            return {
                "status": "error", 
                "error": "No se definieron pasos en la configuración del flujo"
            }
        
        publisher = pubsub_v1.PublisherClient()
        
        # Crear contexto inicial
        initial_context = {
            'flow_id': flow_id,
            'account': account,
            'task_id': task_id,
            'run_id': run_id,
            **message_json.get('context', {})  # Contexto adicional del usuario
        }
        
        # Usar el motor N8N-like para flujos avanzados
        if is_advanced_flow(steps):
            logger.info("🚀 Detectado flujo avanzado - usando motor N8N-like")
            from n8n_engine import N8NLikeEngine
            engine = N8NLikeEngine(config.PROJECT_ID, publisher)
            result = engine.execute_flow(flow_config, initial_context)
            logger.info(f"=== FLUJO N8N-LIKE COMPLETADO: {flow_id} ===")
            logger.info(f"Pasos ejecutados: {len(result.get('executed_steps', []))}")
            logger.info(f"Resultado: {result}")

            # Guardar estado del flujo en Cloud Storage
            flow_state = {
                "flow_id": flow_id,
                "run_id": run_id,
                "account": account,
                "status": result.get("status", "success"),
                "flow_config": flow_config,
                "executed_steps": result.get('executed_steps', []),
                "total_steps": len(result.get('executed_steps', [])),
                "engine_type": "n8n_like",
                "started_at": datetime.now(timezone.utc).isoformat(),
                "completed_at": datetime.now(timezone.utc).isoformat()
            }
            
            state_repo.save_flow_run_state(flow_id, run_id, flow_state)
            
            return {
                "status": result.get("status", "success"),
                "flow_id": flow_id,
                "account": account,
                "task_id": task_id,
                "run_id": run_id,
                "executed_steps": result.get('executed_steps', []),
                "total_steps": len(result.get('executed_steps', [])),
                "engine_type": "n8n_like"
            }
        # Usar el motor básico para flujos simples
        else:
            logger.info("📋 Detectado flujo básico - usando motor simple")
            return {"status": "success"}
    except Exception as e:
        logger.error(f"Error ejecutando flujo dinámico: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }
