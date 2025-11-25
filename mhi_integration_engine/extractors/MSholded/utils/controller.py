
from google.cloud import secretmanager
from google.cloud import storage
from google.cloud import pubsub_v1 as pubsub
import os 
import json
import jinja2
import base64
import time
from datetime import datetime, timezone

# Constantes
__STORAGE_INTEGRATION_BUCKET =  str(os.environ.get('_M_BUCKET_DATA'))
__STORAGE_LASTEST_INCREMENTAL_BUCKET =  str(os.environ.get('_M_BUCKET_LASTEST_EXECUTION'))
#__STORAGE_INTEGRATION_BUCKET =  'ocean_data'
#__STORAGE_LASTEST_INCREMENTAL_BUCKET =  'lastest_incremental_data'
__GCP_PROJECT_ID = str(os.environ.get('_M_PROJECT_ID'))
__TEMPLATE_FILE = "load_status_template.json"
__SECRET_VERSION = 'latest'
__RETURN = {
    0:'fail',
    1:'success',
    2:'warning'
}


def getSecret(connection_id):
    # Cloud function por http
    client = secretmanager.SecretManagerServiceClient()
    secret_id = connection_id
    try:
        secret_name = client.secret_path(__GCP_PROJECT_ID, secret_id) + '/versions/'+__SECRET_VERSION
        response = client.access_secret_version(name=secret_name)
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        msg=f'Error al obtener el secret en getSecret, error:{type(e)}: {e}.'
        print(msg)
        raise(msg)


def getLastestSucccessExecution(account, datasource, dataset):
    # cloud function por http
    # 1) Instanciamos el cliente de GCS
    client = storage.Client()
    bucket = client.bucket(__STORAGE_LASTEST_INCREMENTAL_BUCKET )
    blob_name = f'{datasource}/{dataset}/{account}.json'
    # 2) Leemos el fichero y lo retornamos como json y si no lo encontramos es que no existe y devolvemos None para que el proceso continue con los parátros por defecto
    try:
        blob = bucket.blob(blob_name)
        if blob.exists():
            content = blob.download_as_string()
            return json.loads(content)
    except Exception as e:
        msg=f' No se ha encontrado el fichero que se solicita.\n'
        msg+f' El proceso de carga incremental empezará por el valor por defecto del cliente,\n'
        msg+f' en su defecto por el valor por defecto del extractor,\n'
        msg+f' en otro caso no comenzará para mantener la integridad de datos y el administrador es informado.\n'
        msg+f' Se ha capturado el siguiente error:{type(e)}: {e}'
        print(msg)
        return None


def postLastestSucccessExecution(account, datasource, dataset, field, value):
    # cloud function que escribe en un topic de pubsub = lasted-execution-success
    # 1) Instanciamos GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(__STORAGE_LASTEST_INCREMENTAL_BUCKET)
    # 2) Leemos el template del sistema de ficheros
    try:
        dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), './'))
        dir_path_file = os.path.join(dir_path, __TEMPLATE_FILE)
        with open(dir_path_file, 'r') as f:
            template = f.read()
    except Exception as e:
        msg=f'El proceso de leer el template ha fallado, error:{type(e)}: {e}.'
        print(msg)
        raise msg
        
    # 3) Cambiamos los parámetros por los valores de la funcion
    try:
        rendered = jinja2.Template(template).render(account=account,datasource=datasource,dataset=dataset,incrementalstrategy='incremental',incrementalfield=field, incrementalvalue=value)
    except Exception as e:
        msg=f'El proceso de renderizado del template ha fallado, error:{type(e)}: {e}.'
        print(msg)
        raise msg
    # 4) Guardamos el fichero en GCS
    try:
        blob_name = f'{datasource}/{dataset}/{account}.json'
        blob = bucket.blob(blob_name)
        blob.upload_from_string(data=rendered,content_type='application/json')  
    except Exception as e:
        msg=f'El guardar el fichero con la ultima carga exitosa en GCS ha fallado, error:{type(e)}: {e}.'
        print(msg)
        raise msg
        

def saveStorage(account, datasource, dataset, data_df, folder=None, filename=None):
    # En principio se mantiene como auxiliar del extractor, no en un microservicio por las limitaciones de enviar los datos por algun mecanismo que no sea la escritura directa via api
    #print(f'     Almacenando dataset {dataset} en datalake')
    # Generamos el nombre del fichero con la ruta en una variable que luego pasaremos a GCS para su almacenaje
    data_source = f'int_{datasource}'
    _extension = 'parquet'
    if filename != None:
        file_name = f'{filename}_{account}.{_extension}'
    else:
        file_name   = f'{account}.{_extension}'
    
    if folder != None:
        folder = f'/{folder}'
    else:
        folder=''
    
    destination = f'gs://{__STORAGE_INTEGRATION_BUCKET}/{data_source}/{dataset}{folder}/{file_name}'
    # IMPORTANTE!! En esta linea añadimos en el dataset el campo _mhi_account con el nombre de cuenta
    data_df['_m_extracted_ts'] = int(time.time_ns())
    data_df['_m_account'] = account
    data_df['_m_datasource'] = datasource
    
    try:
        data_df.to_parquet(destination, compression='gzip', engine='pyarrow', index=False)
        print(f'     SUCCESS: Almacenados {len(data_df.index)} registros en el fichero {dataset}{folder}/{file_name} ')
        return(__RETURN[1])
    except Exception as e:
        msg=f'El proceso de guardar la DATA en GCS de la cuenta {account} para el datasource {datasource} y el dataset {dataset} ha fallado, error:{type(e)}: {e}.'
        print(msg)
        raise (msg)
    
def __putMessageInBus(topic, message):
    publisher = pubsub.PublisherClient()
    topic_name = 'projects/{project_id}/topics/{topic}'.format(
        project_id=__GCP_PROJECT_ID,
        topic=topic,  # Set this to something appropriate.
    )
    try:
        publisher.create_topic(name=topic_name)
        future = publisher.publish(topic_name, message, spam='eggs')
    except Exception as e:
        msg=f'La function {__name__} ha fallado'
        msg=f' El proceso de publicar un mensaje en el topic: {topic}, ha fallado.'
        msg+f' Se ha capturado el siguiente error:{type(e)}: {e}.'
        return(__RETURN[0])
    print(future.result())
    return(__RETURN[1])

def postDataTransformation(account, datasource):
    # Esto se ejectuta al ponerse un mensaje en el topic 'data-transformation'
    topic='data-transformation'
    data={
        "account":account,
        "datasource":datasource
    }
    try:
        future = __putMessageInBus(topic,  base64.b64encode(json.dumps(data).encode("utf-8")))
        print(f'Adiós {account}, buen trabajo por la extracción de {datasource} !!!!, le pasamos el testigo al modulo de transformación 👋')
    except Exception as e:
        msg=f'La function {__name__} ha fallado'
        msg=f' El proceso de ejecutar una transfomacion de la cuenta {account} para el datasource {datasource} ha fallado.'
        msg+f' Se ha capturado el siguiente error:{type(e)}: {e}.'
        putNotification(type=1,subject=msg, body=msg)
        return(__RETURN[0])  
    print(future)
    return(__RETURN[1])


def putNotification(type, subject,body,account=None,to_email=None,from_email=None, cc=None,bcc=None,template=None,data=None):
    # se queda en este fichero como auxiliar del repositorio del extractor
    topic = 'notifications'
    
    data = {
        'type':type,
        'subject': subject,
        'text':body,
        'account':account,
        'to_email': to_email,
        'from_email':from_email,
        'cc':cc,
        'bcc':bcc,
        'template': template,
        'data': data
    }

    try:
        future = __putMessageInBus(topic,  base64.b64encode(json.dumps(data).encode("utf-8")))
        print(subject)
        print(body)
    except Exception as e:
        msg=f'La function {__name__} ha fallado'
        print(msg)
        return(__RETURN[0])
    print(future)
    return(__RETURN[1])

def generar_rango(ini:int, fin:int):
    # La entrada en ini será un 01/01/YYYY 00:00:00:00
    # El valor del fin será siempre algo como 31/12/YYYY 23:59:59:59
    
    start_year =  datetime.fromtimestamp(ini).year
    end_year =  datetime.fromtimestamp(fin).year
    value_list=[]
    for i in range(start_year,end_year+1):
        # Por lo tanto, se generarán duplas de valores que contendrán el inicio y final de cada semestre
        # donde, dupla 1 irá de 01/01/YYYY 00:00:00:00 a 30/06/YYYY 23:59:59:59
        primer_semestre_ini = int(datetime(year=i, month=1, day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc).timestamp())
        primer_semestre_fin = int(datetime(year=i, month=6, day=30, hour=23, minute=59, second=59, microsecond=59, tzinfo=timezone.utc).timestamp())
        dupla1 = {primer_semestre_ini,primer_semestre_fin}
        value_list.append(dupla1)
        
        # y dupla 2 contendrá los datos del segundo trimestre
        segundo_semestre_ini = int(datetime(year=i, month=7, day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc).timestamp())
        segundo_semestre_fin = int(datetime(year=i, month=12, day=31, hour=23, minute=59, second=59, microsecond=59, tzinfo=timezone.utc).timestamp())
        dupla2 = {segundo_semestre_ini,segundo_semestre_fin}
        value_list.append(dupla2)
    
    return value_list
            