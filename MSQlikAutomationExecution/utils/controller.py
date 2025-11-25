
from google.cloud import secretmanager
import os 
import logging

logger = logging.getLogger(__name__)

# Constantes
__DATAVISUALIZATION_TOOL = 'qlik'
__GCP_PROJECT_ID = str(os.environ.get('_M_PROJECT_ID'))
__SECRET_VERSION = 'latest'

def getSecret(account):
    client = secretmanager.SecretManagerServiceClient()
    secret_id = f'{account}_{__DATAVISUALIZATION_TOOL}'
    try:
        secret_name = client.secret_path(__GCP_PROJECT_ID, secret_id) + '/versions/'+__SECRET_VERSION
        response = client.access_secret_version(name=secret_name)
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        msg=f'Error al obtener el secret en getSecret, error:{type(e)}: {e}.'
        logger.error(msg)
        raise(msg)
