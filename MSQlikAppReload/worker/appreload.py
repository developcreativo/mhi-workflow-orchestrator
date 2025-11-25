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

class RecargaApp():
    def __init__(self, _m_account: str):
        # Siempre evaluamos que nos llegue el campo _m_account
        if type(_m_account) == str:
            self.__M_ACCOUNT = _m_account
        else:
            raise 'El valor de mhi_account no es un valor válido'
        
        # Instanciamos basándonos en el fichero de configuración
        # Dejamos la configuración en memoria para acceder rápidamente a ella cada vez que necesitemos
        __CONFIG_FILE = 'config.json'
        dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        dir_path_file = os.path.join(dir_path, __CONFIG_FILE)
        with open(dir_path_file, 'r') as f:
            self.__CONFIG = json.load(f)
        
        # Obtenemos todas las variables generalistas de un extractor
        self.__UTIL_NAME = self.__CONFIG['utilName']
        self.__BASE_URL = self.__CONFIG['base_url']
        self.__BASE_URL_RETRIEVE = self.__CONFIG['base_url_retrieve']
        self.secret = None
        
        try:
            # Obtenemos el SECRET de conexión a la fuente
            secret = json.loads(controller.getSecret(account=self.__M_ACCOUNT))
            self.secret = secret
        except Exception as e:
            msg = f'Hay un error en obtener el secret, error:{type(e)}: {e}.'
            logger.error(msg)
            raise(msg)
        
        print(f'CONFIG: {self.__CONFIG}')
        print(f'UTIL NAME: {self.__UTIL_NAME}')
        print(f'M_ACCOUNT: {self.__M_ACCOUNT}')
        print(f'SECRET: {self.secret}')
        
        # Construcción de la Base URL
        self.__BASE_URL = str(self.__CONFIG['base_url'])
        self.__BASE_URL = self.__BASE_URL.replace('{{tenant_name}}', self.secret['tenant_name'])
        self.__BASE_URL = self.__BASE_URL.replace('{{deployment_location}}', self.secret['deployment_location'])
        self.__BASE_URL_RETRIEVE = str(self.__CONFIG['base_url_retrieve'])
        self.__BASE_URL_RETRIEVE = self.__BASE_URL_RETRIEVE.replace('{{tenant_name}}', self.secret['tenant_name'])
        self.__BASE_URL_RETRIEVE = self.__BASE_URL_RETRIEVE.replace('{{deployment_location}}', self.secret['deployment_location'])
        print(f'BASE URL: {self.__BASE_URL}')
        print(f'BASE URL RETRIEVE: {self.__BASE_URL_RETRIEVE}')

    def Iniciar(self, app_id):
        print(f'INICIO RECARGA DE APLICACIÓN {app_id} DE MSQLIKAPPRELOAD PARA LA CUENTA {self.__M_ACCOUNT}.')
        print(f'🔧 WORKER: Iniciando proceso con app_id={app_id}...')
        
        base_url = self.__BASE_URL
        secret = self.secret
        print(f'🔧 WORKER: Secret obtenida: {secret}')
        print(f'🔧 WORKER: Base URL obtenida: {base_url}')
        
        url = base_url  # Para reloads, la URL base ya está completa
        print(f'🔧 WORKER: URL construida: {url}')
        logger.info(f'URL MSQLIKAPPRELOAD: {url}')
        
        retry_strategy = Retry(
            total=5,  # Maximum number of retries
            backoff_factor=2,  # Time to wait between retries
            status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
        )

        # Create an HTTP adapter with the retry strategy and mount it to session
        print(f'🔧 WORKER: Configurando estrategia de retry...')
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount('https://', adapter)
        print(f'🔧 WORKER: Sesión HTTP configurada')
        
        header = {
            'Accept': 'application/json', 
            'Content-Type': 'application/json', 
            'Authorization': f'Bearer {secret}'
        }
        print(f'🔧 WORKER: Headers configurados: {header}')
        print(f'🔧 WORKER: Realizando POST a: {url}')
        
        # Preparar el body JSON según la documentación de Qlik para reloads
        payload = {
            "appId": app_id,
            "partial": False,  # Recarga completa por defecto
            "reloadMode": "default"  # Modo de recarga por defecto
        }
        print(f'🔧 WORKER: Payload preparado: {payload}')

        response = session.post(url=url, headers=header, json=payload)
        print(f'🔧 WORKER: POST completado, status: {response.status_code}')
        
        # Log completo de la respuesta para debugging
        print(f'🔧 WORKER: Response headers: {dict(response.headers)}')
        print(f'🔧 WORKER: Response content type: {response.headers.get("content-type", "unknown")}')

        logger.info(f'🔧 WORKER: Response: {response.text}')
        
        # Verificar si la respuesta es exitosa antes de intentar parsear JSON
        if response.status_code in [200, 201]:
            logger.info(f'🔧 WORKER: Procesando respuesta exitosa...')
            try:
                response_data = response.json()
                logger.info(f'🔧 WORKER: Response JSON en la creación de la recarga: {response_data}')
            except Exception as json_error:
                logger.error(f'🔧 WORKER: Error parseando JSON: {json_error}')
                print(f'🔧 WORKER: Response text: {response.text}')
                # Retornar error en lugar de hacer raise para evitar loops
                return {
                    "status": "error",
                    "app_id": app_id,
                    "error": f'Error parseando respuesta JSON: {json_error}',
                    "response_text": response.text
                }
        else:
            print(f'🔧 WORKER: Error en respuesta: {response.status_code}')
            print(f'🔧 WORKER: Response text: {response.text}')
            logger.error(f'Error en respuesta Qlik: {response.status_code}')
            logger.error(f'Contenido de respuesta: {response.text}')
            # Retornar error en lugar de hacer raise para evitar loops
            return {
                "status": "error",
                "app_id": app_id,
                "error": f'Qlik devolvió error {response.status_code}: {response.text}',
                "response_status": response.status_code,
                "response_text": response.text
            }
        
        logger.info(f'🔧 WORKER: Continuando con procesamiento...')
        logger.info(f'FIN RECARGA DE APLICACIÓN {app_id} DE QLIK PARA LA CUENTA {self.__M_ACCOUNT}.')
        logger.info(f'RESPONSE STATUS CODE: {response.status_code}')
        
        print(f'🔧 WORKER: Verificando status code...')
        # Qlik devuelve 201 (Created) según la documentación oficial
        logger.info(f'🔧 WORKER: Status code válido, extrayendo reload_id...reload_id: {response_data}')
        
        if response.status_code in [200, 201]:
            print(f'🔧 WORKER: Status code válido, extrayendo reload_id...')
            # Obtener reload_id de la respuesta (Qlik devuelve 'id' para reloads)
            reload_id = response_data.get('id')
            print(f'🔧 WORKER: reload_id extraído: {reload_id}')
            
            if not reload_id:
                print(f'🔧 WORKER: No se encontró reload_id, generando uno...')
                # Si por alguna razón Qlik no devuelve 'id', generar uno basado en app_id
                logger.warning(f'Qlik no devolvió ID de recarga, generando uno')
                logger.warning(f'Respuesta completa: {response_data}')
                reload_id = f"qlik-reload-{app_id}-{int(time.time())}"
                print(f'🔧 WORKER: reload_id generado: {reload_id}')
            else:
                print(f'🔧 WORKER: reload_id obtenido de Qlik: {reload_id}')
            
            print(f'🔧 WORKER: Continuando con polling de estado...')
            logger.info(f'ID de recarga obtenido de Qlik: {reload_id}')
            
            # Implementar polling para obtener el estado final de la recarga
            print(f'🔧 WORKER: Iniciando polling de estado...')
            print(f'🔧 WORKER: Parámetros para polling: reload_id={reload_id}, app_id={app_id}')
            try:
                polling_result = self.poll_reload_status(reload_id, header, app_id)
                print(f'🔧 WORKER: Polling completado: {polling_result}')
            except Exception as polling_error:
                print(f'🔧 WORKER: Error en polling: {polling_error}')
                logger.error(f'Error en polling: {polling_error}', exc_info=True)
                # Retornar error en lugar de hacer raise para evitar loops
                return {
                    "status": "error",
                    "app_id": app_id,
                    "error": f'Error en polling: {polling_error}',
                    "reload_id": reload_id
                }
            
            # Retornar el resultado del polling
            return {
                "status": "success",
                "reload_id": reload_id,
                "app_id": app_id,
                "qlik_status": polling_result.get("status"),
                "execution_time": polling_result.get("execution_time"),
                "attempts": polling_result.get("attempts", 0)
            }

    def poll_reload_status(self, reload_id: str, header: dict, app_id: str, max_attempts: int = 60, poll_interval: int = 30) -> Dict[str, Any]:
        """
        Hace polling del estado de una recarga de Qlik hasta que termine (success o error).
        
        Args:
            reload_id: ID de la recarga a monitorear
            header: Headers de autenticación
            app_id: ID de la aplicación
            max_attempts: Número máximo de intentos de polling (default: 60)
            poll_interval: Intervalo entre polls en segundos (default: 30)
            
        Returns:
            dict: Resultado final de la recarga
            
        Raises:
            requests.RequestException: Si hay error en las peticiones
            TimeoutError: Si se excede el tiempo máximo de polling
        """
        logger.info(f'INICIO POLLING DE ESTADO RECARGA QLIK PARA RELOAD_ID: {reload_id} EN CUENTA {self.__M_ACCOUNT}')
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount('https://', adapter)
        
        url_retrieve = self.__BASE_URL_RETRIEVE.replace('{{reload_id}}', reload_id)
        
        for attempt in range(max_attempts):
            try:
                logger.info(f'POLLING RECARGA QLIK INTENTO {attempt + 1}/{max_attempts} PARA RELOAD_ID: {reload_id}')
                
                response = session.get(url=url_retrieve, headers=header)
                
                if response.status_code == 200:
                    data = response.json()
                    logger.info(f'RESPONSE POLLING RECARGA QLIK: {data}')
                    
                    status = data.get('status', 'unknown')
                    
                    logger.info(f'ESTADO ACTUAL RECARGA QLIK: {status}')
                    
                    # Estados finales de Qlik para recargas según documentación oficial
                    if status in ['SUCCEEDED']:
                        logger.info(f'✅ RECARGA QLIK COMPLETADA CON ÉXITO PARA RELOAD_ID: {reload_id}')
                        return {
                            "status": "success",
                            "reload_id": reload_id,
                            "qlik_status": status,
                            "data": data,
                            "attempts": attempt + 1
                        }
                    elif status in ['FAILED', 'CANCELED', 'EXCEEDED_LIMIT']:
                        logger.error(f'❌ RECARGA QLIK FALLÓ PARA RELOAD_ID: {reload_id}')
                        error_msg = f'La recarga de Qlik falló con estado: {status}'
                        if 'error' in data:
                            error_msg += f'. Error: {data["error"]}'
                        return {
                            "status": "error",
                            "reload_id": reload_id,
                            "qlik_status": status,
                            "error": error_msg,
                            "data": data,
                            "attempts": attempt + 1
                        }
                    else:
                        # Estados activos (QUEUED, RELOADING, CANCELING)
                        logger.info(f'⏳ RECARGA QLIK EN PROGRESO PARA RELOAD_ID: {reload_id} (estado: {status}). Esperando {poll_interval}s...')
                        
                        if attempt < max_attempts - 1:  # No esperar en el último intento
                            time.sleep(poll_interval)
                        continue
                        
                else:
                    logger.error(f'Error en polling recarga Qlik: {response.status_code}: {response.text}')
                    if attempt < max_attempts - 1:
                        time.sleep(poll_interval)
                        continue
                    else:
                        raise requests.RequestException(f'Error en polling final: {response.status_code}: {response.text}')
                        
            except requests.RequestException as e:
                logger.error(f'Error en polling recarga Qlik intento {attempt + 1}: {e}')
                if attempt < max_attempts - 1:
                    time.sleep(poll_interval)
                    continue
                else:
                    raise
        
        # Si llegamos aquí, se agotaron los intentos
        timeout_msg = f'Timeout: La recarga Qlik {reload_id} no terminó después de {max_attempts} intentos ({max_attempts * poll_interval}s)'
        logger.error(timeout_msg)
        raise TimeoutError(timeout_msg)
