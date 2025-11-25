"""
Utilidades para normalización de definiciones de flujos.
"""
import logging

logger = logging.getLogger(__name__)


def normalize_steps(flow_config: dict) -> list:
    """
    Normaliza la definición de pasos desde diferentes formatos.
    Acepta 'tasks' (dict/list) o 'steps' y los convierte a formato 'steps'.
    
    Args:
        flow_config: Configuración del flujo
        
    Returns:
        list: Lista de pasos normalizados
    """
    if 'steps' in flow_config and isinstance(flow_config['steps'], list):
        steps = flow_config['steps']
        logger.info(f"📋 Usando 'steps' existente: {len(steps)} pasos")
    else:
        defined_tasks = flow_config.get('tasks')
        logger.info(f"📋 Buscando 'tasks': {type(defined_tasks)} - {defined_tasks}")
        if isinstance(defined_tasks, list):
            steps = defined_tasks
            logger.info(f"📋 Convertido 'tasks' lista a 'steps': {len(steps)} pasos")
        elif isinstance(defined_tasks, dict):
            steps = [
                dict({"id": name}, **cfg) if isinstance(cfg, dict) else {"id": name}
                for name, cfg in defined_tasks.items()
            ]
            logger.info(f"📋 Convertido 'tasks' dict a 'steps': {len(steps)} pasos")
        else:
            steps = []
            logger.warning(f"⚠️ No se encontraron 'tasks' válidos, usando lista vacía")
    
    return steps


def is_advanced_flow(steps: list) -> bool:
    """
    Determina si un flujo es avanzado (N8N-like) o básico.
    Sistema COMPLETAMENTE dinámico - cualquier tipo con configuración es avanzado
    
    Args:
        steps: Lista de pasos del flujo
        
    Returns:
        bool: True si es un flujo avanzado
    """
    for step in steps:
        step_type = step.get('type', 'action')
        config = step.get('config', {})
        
        # CUALQUIER paso con configuración es considerado avanzado
        # Esto permite que cualquier Cloud Function personalizada sea tratada como avanzada
        if config:
            logger.info(f"🚀 Paso '{step_type}' detectado como avanzado por tener configuración")
            return True
        
        # Solo tipos básicos sin configuración son considerados básicos
        basic_types = ['action', 'trigger']
        if step_type in basic_types and not config:
            continue
        
        # Si tiene expresiones con {{}}, es avanzado
        step_data = step.get('data', {})
        if has_expressions(step_data):
            return True
    
    return False


def has_expressions(data: dict) -> bool:
    """
    Verifica si los datos contienen expresiones {{variable}}
    
    Args:
        data: Diccionario de datos
        
    Returns:
        bool: True si contiene expresiones
    """
    import re
    
    def check_value(value):
        if isinstance(value, str):
            return bool(re.search(r'\{\{[^}]+\}\}', value))
        elif isinstance(value, dict):
            return any(check_value(v) for v in value.values())
        elif isinstance(value, list):
            return any(check_value(v) for v in value)
        return False
    
    return check_value(data)
