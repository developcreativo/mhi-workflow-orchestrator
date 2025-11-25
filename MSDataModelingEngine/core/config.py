"""
Configuración centralizada para DataTransformationWorker.
"""
import os


class Config:
    """Configuración centralizada para DataTransformationWorker."""
    
    # GCP Configuration
    PROJECT_ID = os.getenv('_M_PROJECT_ID') or os.getenv('GCP_PROJECT') or os.getenv('GOOGLE_CLOUD_PROJECT')
    
    # Pub/Sub Configuration
    @property
    def notifications_topic(self):
        return f"projects/{self.PROJECT_ID}/topics/notifications"
    
    # HTTP Configuration
    HTTP_TOTAL_RETRIES = int(os.getenv('HTTP_TOTAL_RETRIES', '5'))
    HTTP_BACKOFF_FACTOR = float(os.getenv('HTTP_BACKOFF_FACTOR', '2.0'))
    HTTP_TIMEOUT_SECONDS = int(os.getenv('HTTP_TIMEOUT_SECONDS', '30'))
    
    # Retry Configuration
    RETRY_STATUS_CODES = [429, 500, 502, 503, 504]
    
    # Flow Configuration
    TRIGGER_TOPIC = os.getenv('TRIGGER_TOPIC', 'ms-data-modeling-engine')
    MS_FLOWS_CONTROLLER_TOPIC = os.getenv('_M_MS_FLOWS_CONTROLLER_TOPIC', 'ms-flows-controller')
    
    @classmethod
    def validate(cls):
        """Valida que la configuración requerida esté presente."""
        if not cls.PROJECT_ID:
            raise ValueError("No se encontró PROJECT_ID en las variables de entorno")
        return True


# Instancia global de configuración
config = Config()






