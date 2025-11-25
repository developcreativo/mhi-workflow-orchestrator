"""
Configuración centralizada para MSQlikAppReload.
"""
import os


class Config:
    """Configuración centralizada para MSQlikAppReload."""

    # GCP Configuration
    PROJECT_ID = (
        os.getenv('_M_PROJECT_ID')
        or os.getenv('GCP_PROJECT')
        or os.getenv('GOOGLE_CLOUD_PROJECT')
    )

    # Flow Configuration
    TRIGGER_TOPIC = os.getenv('TRIGGER_TOPIC', 'ms-qlik-app-reload')

    @classmethod
    def validate(cls):
        if not cls.PROJECT_ID:
            raise ValueError("No se encontró PROJECT_ID en las variables de entorno")
        return True


# Instancia global de configuración
config = Config()
















