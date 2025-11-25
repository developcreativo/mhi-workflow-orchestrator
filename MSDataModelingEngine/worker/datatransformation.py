#!/usr/bin/env python3
# -*- coding: utf8 -*-
import os, json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from typing import Dict, Any
import logging
import time
from core.config import config
from utils import controller
from utils.publisher import publish_message
from core.utils.message_utils import create_flow_notification_data
import requests

logger = logging.getLogger(__name__)
def ejecutar_dbt_job(secret: dict, job_id):
    """
    Ejecuta un job de dbt Cloud usando la API
    """
    # Configuración
    url = f"https://ub031.us1.dbt.com/api/v2/accounts/{secret['account_id']}/jobs/{job_id}/run"
    
    # Payload con la causa del trigger
    
    payload = json.dumps({
        "cause": "Triggered via API"
    })
    
    # Headers con el token de autorización
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {secret["secret_token_key"]}'
    }
    print(f'HEADERS: {headers}')
    print(f'PAYLOAD: {payload}')
    print(f'URL: {url}')
    try:
        # Realizar la petición POST
        response = requests.request("POST", url, headers=headers, data=payload)
        
        # Mostrar el código de estado
        print(f"Status Code: {response.status_code}")
        
        # Mostrar la respuesta completa
        print("Response:")
        print(response.text)
        
        # Si la respuesta es exitosa, parsear el JSON
        if response.status_code == 200:
            data = response.json()
            if data.get('status', {}).get('is_success'):
                run_id = data.get('data', {}).get('id')
                print(f"\n✅ Job ejecutado exitosamente!")
                print(f"Run ID: {run_id}")
                print(f"Estado: {data.get('data', {}).get('status_humanized', 'Unknown')}")
                return run_id
            else:
                print(f"\n❌ Error en la respuesta: {data.get('status', {}).get('user_message', 'Unknown error')}")
        else:
            print(f"\n❌ Error HTTP: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error en la petición: {e}")
        return None


def consultar_estado_run(secret: dict, run_id, poll_status=False, max_attempts=60, poll_interval=30):
    """
    Consulta el estado de un run específico
    
    Args:
        secret: Secret de la cuenta
        run_id: ID del run a consultar
        poll_status: Si True, hace polling hasta que termine la ejecución
        max_attempts: Número máximo de intentos de polling (solo si poll_status=True)
        poll_interval: Intervalo entre polls en segundos (solo si poll_status=True)
    """
    print(f'SECRET: {secret}')
    print(f'RUN ID: {run_id}')
    print(f'POLL STATUS: {poll_status}')
    print(f'MAX ATTEMPTS: {max_attempts}')
    print(f'POLL INTERVAL: {poll_interval}')
    url = f"https://ub031.us1.dbt.com/api/v2/accounts/{secret['account_id']}/runs/{run_id}/"
    
    headers = {
        'Authorization': f'Bearer {secret["secret_token_key"]}'
    }
    print(f'HEADERS: {headers}')
    print(f'URL: {url}')
    try:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            print(f"\n📊 Estado del Run {run_id}:")
            print(f"Estado: {data.get('data', {}).get('status_humanized', 'Unknown')}")
            print(f"Progreso: {data.get('data', {}).get('in_progress', False)}")
            print(f"Completado: {data.get('data', {}).get('is_complete', False)}")
            print(f"Exitoso: {data.get('data', {}).get('is_success', False)}")
            
            # Si se solicita polling, verificar si necesita continuar
            if poll_status:
                job_status = data.get('data', {}).get('status')
                print(f"Estado numérico: {job_status}")
                
                # Estados finales: Success (10), Error (20), Cancelled (30)
                if job_status in [10, 20, 30]:
                    print(f"✅ Ejecución terminada con estado: {job_status}")
                    return data
                # Estados en progreso: Queued (1), Starting (2), Running (3)
                elif job_status in [1, 2, 3]:
                    print(f"⏳ Ejecución en progreso (estado: {job_status}). Iniciando polling...")
                    return poll_run_status_simple(secret, run_id, max_attempts, poll_interval)
                else:
                    print(f"⚠️ Estado desconocido: {job_status}")
                    return data
            
            return data
        else:
            print(f"❌ Error al consultar estado: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Error al consultar estado: {e}")
        return None


def poll_run_status_simple(secret: dict, run_id, max_attempts=60, poll_interval=30):
    """
    Versión simplificada de polling para usar con consultar_estado_run
    """
    print(f"🔄 Iniciando polling para run {run_id}...")
    
    for attempt in range(max_attempts):
        print(f"Intento {attempt + 1}/{max_attempts}")
        
        # Consultar estado actual
        result = consultar_estado_run(secret, run_id, poll_status=False)
        
        if result:
            job_status = result.get('data', {}).get('status')
            
            # Estados finales
            if job_status == 10:  # Success
                print(f"✅ Ejecución completada exitosamente!")
                return result
            elif job_status in [20, 30]:  # Error, Cancelled
                print(f"❌ Ejecución falló con estado: {job_status}")
                return result
            elif job_status in [1, 2, 3]:  # En progreso
                print(f"⏳ Ejecución en progreso... Esperando {poll_interval}s")
                if attempt < max_attempts - 1:
                    time.sleep(poll_interval)
                continue
            else:
                print(f"⚠️ Estado desconocido: {job_status}")
                if attempt < max_attempts - 1:
                    time.sleep(poll_interval)
                continue
        else:
            print(f"❌ Error al obtener estado en intento {attempt + 1}")
            if attempt < max_attempts - 1:
                time.sleep(poll_interval)
            continue
    
    print(f"⏰ Timeout: La ejecución no terminó después de {max_attempts} intentos")
    return None


class CargaAutomatizacion():
    def __init__(self, _m_account:str):
        # TODO aqui algo se pierde o tarda en obtenerlo siempre ya que nos da siempre 404 cuando se obtienen los parametros o puede 
        # tambien sea un problema de la construccion de la url con replace que al remplazar los parametros deja algo mal estructurado 
        if type(_m_account) == str:
            self.__M_ACCOUNT = _m_account
        else:
            raise 'El valor de mhi_account no es un valor válido'
        # Instanciamos basandonos en el fichero de configuración
        # Dejamos la configuración en memoria para acceder rapidamente a ella cada vez que necesitemos
        __CONFIG_FILE = 'config.json'
        dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        dir_path_file = os.path.join(dir_path, __CONFIG_FILE)
        with open(dir_path_file, 'r') as f:
            self.__CONFIG =  json.load(f)
        # Obtenemos todas las variables generalistas de un extractor
        self.__UTIL_NAME = self.__CONFIG['utilName']
        print(f'UTIL NAME: {self.__UTIL_NAME}')
        self.secret = None
        try:
            secret = json.loads(controller.getSecret(self.__CONFIG['secret_name']))
            self.secret = secret
        except Exception as e:
            msg = f'Error al obtener el secret: {type(e)}: {e}.'
            logger.error(msg)
            raise
        # FUTURE FIN 
        # Construcción de la Base URL
        self.__BASE_URL = str(self.__CONFIG['base_url'])
        self.__BASE_URL = self.__BASE_URL.replace('{{account_id}}', self.secret['account_id'])
        self.__BASE_URL_RETRIEVE = str(self.__CONFIG['base_url_retrieve'])
        self.__BASE_URL_RETRIEVE = self.__BASE_URL_RETRIEVE.replace('{{account_id}}', self.secret['account_id'])
        self.__HEADER = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + self.secret['token']
        }
        
        # Configuración centralizada
        self.__config = config

    def Iniciar(self, job_id: str, flow_id: str = None, run_id: str = None, task_id: str = None) -> Dict[str, Any]:
        logger.info(f'INICIO TRANSFORMACION DE DATOS PARA LA CUENTA {self.__M_ACCOUNT}.')
        logger.info(f'🔍 DEBUG - Parámetros recibidos: job_id={job_id}, flow_id={flow_id}, run_id={run_id}, task_id={task_id}')
        logger.info(f'🔍 DEBUG - Tipos de parámetros: run_id={type(run_id)}, flow_id={type(flow_id)}, task_id={type(task_id)}')
        try:
            print("🚀 Ejecutando job de dbt Cloud...")
            print(f'SECRET NAME: {self.__CONFIG["secret_name"]}')
            print(f'Iniciando obtencion del secret...')
            secret = json.loads(controller.getSecret(self.__CONFIG['secret_name']))
            secret['secret_token_key'] = self.__CONFIG['secret_name']
            print(f'Secret obtenido: {secret}')
            print(f'Iniciando ejecucion del job...')
            # Ejecutar el job
            dbt_run_id = ejecutar_dbt_job(secret, job_id)
            if dbt_run_id:
                print(f"\n🔍 Consultando estado del run {dbt_run_id} con polling...")
                
                # Usar la nueva funcionalidad de polling integrada
                result = consultar_estado_run(secret, dbt_run_id, poll_status=True, max_attempts=60, poll_interval=30)
                
                if result:
                    job_status = result.get('data', {}).get('status')
                    status_humanized = result.get('data', {}).get('status_humanized', 'Unknown')
                    
                    if job_status == 10:  # Success
                        logger.info(f'✅ TRANSFORMACION COMPLETADA EXITOSAMENTE PARA CUENTA {self.__M_ACCOUNT}')
                        
                        # Enviar notificación de éxito al MSFlowsController
                        if flow_id and run_id:
                            try:
                                # Usar task_id si está disponible, sino usar un valor por defecto
                                effective_task_id = task_id if task_id else config.TRIGGER_TOPIC
                                
                                # Debug: verificar que run_id sea el correcto del flujo
                                logger.info(f'🔍 DEBUG - Antes de crear notificación: flow_id={flow_id}, run_id={run_id}, dbt_run_id={dbt_run_id}')
                                
                                flow_notification = create_flow_notification_data(
                                    flow_id=flow_id,
                                    run_id=run_id,
                                    task_id=effective_task_id,
                                    account=self.__M_ACCOUNT,
                                    step=config.TRIGGER_TOPIC,
                                    status='completed',
                                    result={
                                        "job_id": job_id,
                                        "dbt_run_id": dbt_run_id,  # ID del run de dbt
                                        "flow_run_id": run_id,  # ID del run del flujo
                                        "dbt_status": job_status,
                                        "status_humanized": status_humanized,
                                        "transformation_result": result
                                    }
                                )
                                message_id = publish_message(
                                    project_id=config.PROJECT_ID,
                                    topic_id=config.MS_FLOWS_CONTROLLER_TOPIC,
                                    payload=flow_notification
                                )
                                logger.info(f"✅ Notificación de éxito enviada al MSFlowsController: {message_id}")
                            except Exception as e:
                                logger.warning(f"⚠️ No se pudo enviar notificación al MSFlowsController: {e}")
                        else:
                            logger.warning(f"⚠️ No se envió notificación: faltan flow_id o run_id")
                        
                        return {
                            "status": "success",
                            "run_id": run_id,
                            "job_id": job_id,
                            "dbt_status": job_status,
                            "status_humanized": status_humanized,
                            "data": result
                        }
                    elif job_status in [20, 30]:  # Error, Cancelled
                        logger.error(f'❌ TRANSFORMACION FALLÓ PARA CUENTA {self.__M_ACCOUNT}')
                        
                        # Enviar notificación de error al MSFlowsController
                        if flow_id and run_id:
                            try:
                                # Usar task_id si está disponible, sino usar un valor por defecto
                                effective_task_id = task_id if task_id else config.TRIGGER_TOPIC
                                
                                # Debug: verificar que run_id sea el correcto del flujo
                                logger.info(f'🔍 DEBUG - Antes de crear notificación de error: flow_id={flow_id}, run_id={run_id}, dbt_run_id={dbt_run_id}')
                                
                                flow_notification = create_flow_notification_data(
                                    flow_id=flow_id,
                                    run_id=run_id,
                                    task_id=effective_task_id,
                                    account=self.__M_ACCOUNT,
                                    step=config.TRIGGER_TOPIC,
                                    status='error',
                                    error=f"Ejecución falló con estado: {job_status}",
                                    result={
                                        "job_id": job_id,
                                        "dbt_run_id": dbt_run_id,  # ID del run de dbt
                                        "flow_run_id": run_id,  # ID del run del flujo
                                        "dbt_status": job_status,
                                        "status_humanized": status_humanized,
                                        "transformation_result": result
                                    }
                                )
                                message_id = publish_message(
                                    project_id=config.PROJECT_ID,
                                    topic_id=config.MS_FLOWS_CONTROLLER_TOPIC,
                                    payload=flow_notification
                                )
                                logger.info(f"❌ Notificación de error enviada al MSFlowsController: {message_id}")
                            except Exception as e:
                                logger.warning(f"⚠️ No se pudo enviar notificación de error al MSFlowsController: {e}")
                        else:
                            logger.warning(f"⚠️ No se envió notificación de error: faltan flow_id o run_id")
                        
                        return {
                            "status": "error",
                            "run_id": run_id,
                            "job_id": job_id,
                            "dbt_status": job_status,
                            "status_humanized": status_humanized,
                            "error": f"Ejecución falló con estado: {job_status}",
                            "data": result
                        }
                    else:
                        logger.warning(f'⚠️ ESTADO DESCONOCIDO PARA CUENTA {self.__M_ACCOUNT}: {job_status}')
                        return {
                            "status": "unknown",
                            "run_id": run_id,
                            "job_id": job_id,
                            "dbt_status": job_status,
                            "status_humanized": status_humanized,
                            "data": result
                        }
                else:
                    logger.error(f'❌ ERROR AL OBTENER RESULTADO FINAL PARA CUENTA {self.__M_ACCOUNT}')
                    return {
                        "status": "error",
                        "run_id": run_id,
                        "job_id": job_id,
                        "error": "No se pudo obtener el resultado final de la ejecución"
                    }
            else:
                logger.error(f'❌ NO SE PUDO EJECUTAR EL JOB PARA CUENTA {self.__M_ACCOUNT}')
                return {
                    "status": "error",
                    "job_id": job_id,
                    "error": "No se pudo ejecutar el job de dbt"
                }
                
        except Exception as e:
            logger.error(f'❌ ERROR EN TRANSFORMACION PARA CUENTA {self.__M_ACCOUNT}: {e}')
            return {
                "status": "error",
                "job_id": job_id,
                "error": f"Error en la transformación: {str(e)}"
            }