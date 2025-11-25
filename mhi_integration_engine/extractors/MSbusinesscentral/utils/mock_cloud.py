"""
Utilidades de simulación para pruebas locales
Este archivo proporciona simulaciones de servicios de Google Cloud
para pruebas en entornos de desarrollo local.
"""

import json
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class MockSecretManager:
    """Simulación de Secret Manager para pruebas locales"""
    
    def __init__(self, secrets_dir="./local_secrets"):
        """
        Inicializa simulación de Secret Manager
        
        Args:
            secrets_dir (str): Directorio donde se almacenan los secretos locales
        """
        self.secrets_dir = secrets_dir
        os.makedirs(secrets_dir, exist_ok=True)
        
    def access_secret_version(self, request):
        """
        Simula acceso a un secreto
        
        Args:
            request (dict): Solicitud con el nombre del secreto
            
        Returns:
            object: Objeto con el payload del secreto
        """
        # Extraer el nombre del secreto de la ruta completa
        parts = request["name"].split("/")
        secret_name = parts[-3]  # Formato: projects/PROJECT_ID/secrets/SECRET_NAME/versions/latest
        
        secret_path = os.path.join(self.secrets_dir, f"{secret_name}.json")
        
        if not os.path.exists(secret_path):
            # Crear un secreto de prueba si no existe
            self._create_test_secret(secret_name, secret_path)
            
        with open(secret_path, 'r') as f:
            secret_content = f.read()
            
        # Crear objeto de respuesta similar al de Google Cloud
        class SecretPayload:
            def __init__(self, data):
                self.data = data.encode('utf-8')
                
        class SecretResponse:
            def __init__(self, payload):
                self.payload = payload
                
        return SecretResponse(SecretPayload(secret_content))
    
    def _create_test_secret(self, secret_name, secret_path):
        """
        Crea un secreto de prueba para Business Central
        
        Args:
            secret_name (str): Nombre del secreto
            secret_path (str): Ruta donde guardar el secreto
        """
        logger.info(f"Creando secreto de prueba en {secret_path}")
        
        # Determinar qué tipo de secreto crear basado en el nombre
        if "businesscentral" in secret_name:
            secret_content = {
                "tenant_id": "test-tenant-id",
                "client_id": "test-client-id",
                "client_secret": "test-client-secret",
                "environment": "Production",
                "deployment": "Cloud",
                "start_date": "2022-01-01"
            }
            
        else:
            # Secreto genérico para otros extractores
            secret_content = {
                "api_key": "test-api-key",
                "start_date": "2022-01-01"
            }
            
        with open(secret_path, 'w') as f:
            json.dump(secret_content, f, indent=2)

class MockStorageClient:
    """Simulación de Storage Client para pruebas locales"""
    
    def __init__(self, storage_dir="./local_storage"):
        """
        Inicializa simulación de Storage
        
        Args:
            storage_dir (str): Directorio base para almacenamiento local
        """
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)
        
    def bucket(self, bucket_name):
        """
        Simula obtener un bucket
        
        Args:
            bucket_name (str): Nombre del bucket
            
        Returns:
            MockBucket: Simulación de un bucket
        """
        return MockBucket(bucket_name, self.storage_dir)

class MockBucket:
    """Simulación de Bucket para pruebas locales"""
    
    def __init__(self, name, base_dir):
        """
        Inicializa simulación de Bucket
        
        Args:
            name (str): Nombre del bucket
            base_dir (str): Directorio base
        """
        self.name = name
        self.dir = os.path.join(base_dir, name)
        os.makedirs(self.dir, exist_ok=True)
        
    def blob(self, blob_path):
        """
        Simula obtener un blob
        
        Args:
            blob_path (str): Ruta del blob
            
        Returns:
            MockBlob: Simulación de un blob
        """
        return MockBlob(blob_path, self.dir)

class MockBlob:
    """Simulación de Blob para pruebas locales"""
    
    def __init__(self, path, base_dir):
        """
        Inicializa simulación de Blob
        
        Args:
            path (str): Ruta del blob
            base_dir (str): Directorio base
        """
        self.path = path
        full_path = os.path.join(base_dir, path)
        self.dir = os.path.dirname(full_path)
        self.file_path = full_path
        os.makedirs(self.dir, exist_ok=True)
        
    def exists(self):
        """
        Verifica si el blob existe
        
        Returns:
            bool: True si existe, False en caso contrario
        """
        return os.path.exists(self.file_path)
    
    def download_as_string(self):
        """
        Descarga el contenido del blob como string
        
        Returns:
            bytes: Contenido del blob
        """
        if not self.exists():
            return b"{}"
            
        with open(self.file_path, 'rb') as f:
            return f.read()
    
    def upload_from_string(self, content, content_type=None):
        """
        Sube contenido al blob
        
        Args:
            content (str/bytes): Contenido a subir
            content_type (str, optional): Tipo de contenido
        """
        # Crear directorio si no existe
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        
        # Escribir contenido
        mode = 'wb' if isinstance(content, bytes) else 'w'
        with open(self.file_path, mode) as f:
            f.write(content)
        
        logger.info(f"Guardado en: {self.file_path}")
        return self.file_path

class MockPublisherClient:
    """Simulación de Publisher Client para pruebas locales"""
    
    def __init__(self, pubsub_dir="./local_pubsub"):
        """
        Inicializa simulación de Publisher
        
        Args:
            pubsub_dir (str): Directorio para mensajes de PubSub
        """
        self.pubsub_dir = pubsub_dir
        os.makedirs(pubsub_dir, exist_ok=True)
        
    def topic_path(self, project_id, topic_id):
        """
        Simula crear una ruta de topic
        
        Args:
            project_id (str): ID del proyecto
            topic_id (str): ID del topic
            
        Returns:
            str: Ruta del topic
        """
        return f"projects/{project_id}/topics/{topic_id}"
    
    def publish(self, topic_path, data):
        """
        Simula publicar un mensaje
        
        Args:
            topic_path (str): Ruta del topic
            data (bytes): Datos a publicar
            
        Returns:
            MockPublishFuture: Simulación del resultado de publicación
        """
        # Extraer topic_id del topic_path
        parts = topic_path.split('/')
        topic_id = parts[-1]
        
        # Crear directorio para el topic si no existe
        topic_dir = os.path.join(self.pubsub_dir, topic_id)
        os.makedirs(topic_dir, exist_ok=True)
        
        # Guardar mensaje con timestamp como nombre de archivo
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        message_path = os.path.join(topic_dir, f"message_{timestamp}.json")
        
        # Convertir datos a string si son bytes
        if isinstance(data, bytes):
            data = data.decode('utf-8')
            
        # Guardar mensaje
        with open(message_path, 'w') as f:
            f.write(data)
            
        logger.info(f"Mensaje publicado en {topic_id}: {data}")
        
        # Retornar un future simulado
        return MockPublishFuture(message_path)

class MockPublishFuture:
    """Simulación del Future de PubSub para pruebas locales"""
    
    def __init__(self, message_id):
        """
        Inicializa simulación de PublishFuture
        
        Args:
            message_id (str): ID del mensaje
        """
        self.message_id = message_id
        
    def result(self):
        """
        Simula obtener el resultado del future
        
        Returns:
            str: ID del mensaje
        """
        return self.message_id

def setup_mock_clients():
    """
    Configura y retorna clientes mock para pruebas locales
    
    Returns:
        tuple: Secret Manager, Storage Client y Publisher Client
    """
    # Crear directorios base para pruebas
    base_dir = "./local_tests"
    os.makedirs(base_dir, exist_ok=True)
    
    # Configurar Secret Manager mock
    secret_manager = MockSecretManager(os.path.join(base_dir, "secrets"))
    
    # Configurar Storage Client mock
    storage_client = MockStorageClient(os.path.join(base_dir, "storage"))
    
    # Configurar Publisher Client mock
    publisher_client = MockPublisherClient(os.path.join(base_dir, "pubsub"))
    
    return secret_manager, storage_client, publisher_client

# Clase mock para simular respuestas de API
class MockResponse:
    """Simulación de respuesta de requests para pruebas locales"""
    
    def __init__(self, status_code, json_data, text=None):
        """
        Inicializa simulación de Response
        
        Args:
            status_code (int): Código de estado HTTP
            json_data (dict): Datos JSON de respuesta
            text (str, optional): Texto de respuesta
        """
        self.status_code = status_code
        self._json_data = json_data
        self.text = text or json.dumps(json_data)
        
    def json(self):
        """
        Simula método json() de Response
        
        Returns:
            dict: Datos JSON
        """
        return self._json_data