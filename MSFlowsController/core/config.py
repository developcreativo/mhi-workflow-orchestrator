"""
Configuración centralizada para el FlowController.
"""
import os
from typing import Optional
from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """Configuración centralizada para el FlowController."""
    
    # Capturamos las posibles variables de entorno para el Project ID
    m_project_id: Optional[str] = Field(default=None, alias='_M_PROJECT_ID')
    gcp_project: Optional[str] = Field(default=None, alias='GCP_PROJECT')
    google_cloud_project: Optional[str] = Field(default=None, alias='GOOGLE_CLOUD_PROJECT')
    
    # Bucket Configuration
    FLOWS_BUCKET: str = Field(default='ocean_flows_graphs', validation_alias='FLOWS_BUCKET')
    RUNS_BUCKET: str = Field(default='ocean_flows_runs', validation_alias='RUNS_BUCKET')
    
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')

    @computed_field
    def PROJECT_ID(self) -> str:
        """
        Obtiene el Project ID de las variables de entorno disponibles.
        Prioridad: _M_PROJECT_ID > GCP_PROJECT > GOOGLE_CLOUD_PROJECT
        """
        val = self.m_project_id or self.gcp_project or self.google_cloud_project
        if not val:
             # Fallback final por si acaso Pydantic no lo capturó (ej. seteadas después de instanciar)
             val = os.getenv('_M_PROJECT_ID') or os.getenv('GCP_PROJECT') or os.getenv('GOOGLE_CLOUD_PROJECT')
        
        if not val:
            raise ValueError("No se encontró PROJECT_ID en las variables de entorno")
        return val

    @property
    def flows_topic(self) -> str:
        return f"projects/{self.PROJECT_ID}/topics/flows-controller"
    
    def validate(self):
        """Valida que la configuración requerida esté presente."""
        # Al acceder a PROJECT_ID se dispara la validación
        _ = self.PROJECT_ID
        return True

# Instancia global de configuración
config = Settings()
