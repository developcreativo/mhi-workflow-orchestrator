
from google.cloud import secretmanager
import os 

# Constantes
__DATAVISUALIZATION_TOOL = 'qlik'
__GCP_PROJECT_ID = str(os.environ.get('_M_PROJECT_ID'))
__SECRET_VERSION = 'latest'

def getSecret(secret_name):
    client = secretmanager.SecretManagerServiceClient()
    secret_id = secret_name
    try:
        secret_name = client.secret_path(__GCP_PROJECT_ID, secret_id) + '/versions/'+__SECRET_VERSION
        response = client.access_secret_version(name=secret_name)
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        msg=f'Error al obtener el secret en getSecret, error:{type(e)}: {e}.'
        print(msg)
        raise(msg)
