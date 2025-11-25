
from google.cloud import secretmanager
import os 
import logging

logger = logging.getLogger(__name__)

# Constantes
__TOOL = 'qlik'
__GCP_PROJECT_ID = str(os.environ.get('_M_PROJECT_ID'))
__SECRET_VERSION = 'latest'

def getSecret(account):
    client = secretmanager.SecretManagerServiceClient()
    secret_id = f'{account}_{__TOOL}'
    try:
        secret_name = client.secret_path(__GCP_PROJECT_ID, secret_id) + '/versions/'+__SECRET_VERSION
        logger.info(f'SECRET NAME: {secret_name}')
        response = client.access_secret_version(name=secret_name)
        logger.info(f'RESPONSE: {response}')
        logger.info(f'RESPONSE PAYLOAD: {response.payload.data.decode("UTF-8")}')
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        msg=f'Error al obtener el secret en getSecret, error:{type(e)}: {e}.'
        logger.error(msg)
        raise(msg)
