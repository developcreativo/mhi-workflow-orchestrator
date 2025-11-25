#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
Punto de entrada principal para la Cloud Function DataTransformationWorker.
"""
import functions_framework
import logging
from cloudevents.http import CloudEvent
from core.config import config
from core.handlers.data_transformation_handler import DataTransformationHandler

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inicializar handler
data_transformation_handler = DataTransformationHandler()

@functions_framework.cloud_event
def IniciarTransformacionDatos(cloud_event: CloudEvent) -> dict:
    """
    Punto de entrada principal para la Cloud Function.
    Procesa solicitudes de transformación de datos.
    
    Args:
        cloud_event: Evento de Cloud Function con los datos del mensaje
        
    Returns:
        dict: Resultado del procesamiento
    """
    logger.info(f'INICIO TRANSFORMACION DE DATOS EN LA FUNCION MSDATAMODELING.') # TODO: Ver si se puede eliminar
    return data_transformation_handler.handle_data_transformation_request(cloud_event)


if __name__ == '__main__':
    from worker.datatransformation import CargaAutomatizacion
    import datetime
    
    ini = datetime.datetime.now()
    print("Inicio: ", ini)
    job_id = "70403104286944"  # Job ID correcto para dbt Cloud
    carga = CargaAutomatizacion(_m_account='silken')
    carga.Iniciar(job_id)
    
    fin = datetime.datetime.now()
    print("Fin: ", fin)
    print("Duración: ", fin.timestamp()-ini.timestamp())