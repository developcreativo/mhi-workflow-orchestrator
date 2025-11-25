#!/usr/bin/env python3
# -*- coding: utf8 -*-
import os, json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from utils import controller
import logging
import time
from typing import Dict, Any

logger = logging.getLogger(__name__)

class CargaAutomatizacion():
    def __init__(self, _m_account:str):
        # Siempre evaluamos que nos llegue el campo _m_account
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
        self.__BASE_URL = self.__CONFIG['base_url']
        self.__BASE_URL_RETRIEVE = self.__CONFIG['base_url_retrieve']
        self.secret = None
        try:
            # Obtenemos el SECRET de conexion a la fuente
            secret = json.loads(controller.getSecret(account=self.__M_ACCOUNT))
            self.secret = secret
        except Exception as e:
            msg=f'Hay un error en obtener el secret, error:{type(e)}: {e}.'
            logger.error(msg)
            raise(msg)
        # FUTURE FIN
        # Construcción de la Base URL
        self.__BASE_URL = str(self.__CONFIG['base_url'])
        self.__BASE_URL = self.__BASE_URL.replace('{{tenant_name}}', self.secret['tenant_name'])
        self.__BASE_URL = self.__BASE_URL.replace('{{deployment_location}}', self.secret['deployment_location'])
        self.__BASE_URL_RETRIEVE = str(self.__CONFIG['base_url_retrieve'])
        self.__BASE_URL_RETRIEVE = self.__BASE_URL_RETRIEVE.replace('{{tenant_name}}', self.secret['tenant_name'])
        self.__BASE_URL_RETRIEVE = self.__BASE_URL_RETRIEVE.replace('{{deployment_location}}', self.secret['deployment_location'])

    def Iniciar(self, automation, execution_token):
        base_url = self.__BASE_URL
        url = base_url.replace('{{automation_id}}', automation)
        retry_strategy = Retry(
            total=5,  # Maximum number of retries
            backoff_factor=2, # Time to wait between retries
            status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
        )

        # Create an HTTP adapter with the retry strategy and mount it to session
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount('https://', adapter)
        header= {'Accept': 'application/json', 'Content-Type': 'application/json', 'Authorization': f'Bearer {execution_token}'}
        
        # Preparar el body JSON según la documentación de Qlik
        payload = {
            "inputs": {},
            "context": "api"
        }
        response = session.post(url=url, headers=header, json=payload)
        # Verificar si la respuesta es exitosa antes de intentar parsear JSON
        if response.status_code in [200, 201]:
            logger.info(f'🔧 WORKER: Procesando respuesta exitosa...')
            try:
                response_data = response.json()
            except Exception as json_error:
                raise Exception(f'Error parseando respuesta JSON: {json_error}')
        else:
            raise Exception(f'Qlik devolvió HTML en lugar de JSON. Status: {response.status_code}')
        
        if response.status_code in [200, 201]:
            # Obtener run_id de la respuesta (Qlik devuelve 'guid' en lugar de 'id')
            run_id = response_data.get('id') or response_data.get('guid')
            print(f'🔧 WORKER: run_id extraído: {run_id}')
            
            if not run_id:
                print(f'🔧 WORKER: No se encontró run_id, generando uno...')
                # Si por alguna razón Qlik no devuelve 'id' ni 'guid', generar uno basado en automation_id
                logger.warning(f'Qlik no devolvió ID ni GUID de ejecución, generando uno')
                run_id = f"qlik-{automation}-{int(time.time())}"
                print(f'🔧 WORKER: run_id generado: {run_id}')
            else:
                print(f'🔧 WORKER: run_id obtenido de Qlik: {run_id}')
            
            try:
                polling_result = self.poll_run_status(run_id, header, automation)
            except Exception as polling_error:
                raise
            
            # Retornar el resultado del polling
            return {
                "status": "success",
                "run_id": run_id,
                "automation_id": automation,
                "qlik_status": polling_result.get("status"),
                "execution_time": polling_result.get("execution_time"),
                "attempts": polling_result.get("attempts", 0)
            }
        #     raise(msg)

    def poll_run_status(self, run_id: str, header: dict, automation_id: str, max_attempts: int = 60, poll_interval: int = 30) -> Dict[str, Any]:
        """
        Hace polling del estado de una ejecución de Qlik hasta que termine (success o error).
        
        Args:
            run_id: ID de la ejecución a monitorear (GUID de Qlik)
            max_attempts: Número máximo de intentos de polling (default: 60)
            poll_interval: Intervalo entre polls en segundos (default: 30)
            
        Returns:
            dict: Resultado final de la ejecución
            
        Raises:
            requests.RequestException: Si hay error en las peticiones
            TimeoutError: Si se excede el tiempo máximo de polling
        """
        logger.info(f'INICIO POLLING DE ESTADO QLIK PARA RUN_ID: {run_id} EN CUENTA {self.__M_ACCOUNT}')
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount('https://', adapter)
        
        url_retrieve = self.__BASE_URL_RETRIEVE.replace('{{run_id}}', run_id)
        url_retrieve = url_retrieve.replace('{{automation_id}}', automation_id)
        
        for attempt in range(max_attempts):
            try:
                response = session.get(url=url_retrieve, headers=header)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    status = data.get('status', 'unknown')
                    
                    # Estados finales de Qlik según documentación
                    if status in ['finished', 'finished with warnings']:
                        return {
                            "status": "success",
                            "run_id": run_id,
                            "qlik_status": status,
                            "data": data,
                            "attempts": attempt + 1
                        }
                    elif status in ['failed', 'stopped', 'must stop', 'exceeded limit']:
                        error_msg = f'La ejecución de Qlik falló con estado: {status}'
                        if 'error' in data:
                            error_msg += f'. Error: {data["error"]}'
                        return {
                            "status": "error",
                            "run_id": run_id,
                            "qlik_status": status,
                            "error": error_msg,
                            "data": data,
                            "attempts": attempt + 1
                        }
                    else:
                        # Estados intermedios (running, starting, queued, not started)
                        if attempt < max_attempts - 1:  # No esperar en el último intento
                            time.sleep(poll_interval)
                        continue
                        
                else:
                    logger.error(f'Error en polling Qlik: {response.status_code}: {response.text}')
                    if attempt < max_attempts - 1:
                        time.sleep(poll_interval)
                        continue
                    else:
                        raise requests.RequestException(f'Error en polling final: {response.status_code}: {response.text}')
                        
            except requests.RequestException as e:
                logger.error(f'Error en polling Qlik intento {attempt + 1}: {e}')
                if attempt < max_attempts - 1:
                    time.sleep(poll_interval)
                    continue
                else:
                    raise
        
        # Si llegamos aquí, se agotaron los intentos
        timeout_msg = f'Timeout: La ejecución Qlik {run_id} no terminó después de {max_attempts} intentos ({max_attempts * poll_interval}s)'
        logger.error(timeout_msg)
        raise TimeoutError(timeout_msg)

        
        