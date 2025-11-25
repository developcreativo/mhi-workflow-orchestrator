import os,json,jinja2,base64,time,requests
from datetime import datetime, timezone
import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from requests.auth import HTTPBasicAuth
from google.cloud import secretmanager
from google.cloud import storage
from google.cloud import pubsub_v1 as pubsub
from schemas import get_schema as _get_schema


# Constantes
__STORAGE_INTEGRATION_BUCKET =  str(os.environ.get('_M_BUCKET_DATA'))
__STORAGE_LASTEST_INCREMENTAL_BUCKET =  str(os.environ.get('_M_BUCKET_LASTEST_EXECUTION'))
#__STORAGE_INTEGRATION_BUCKET =  'ocean_data'
#__STORAGE_LASTEST_INCREMENTAL_BUCKET =  'lastest_incremental_data'
__RETURN = {
    0:'error',
    1:'success',
    2:'warning'
}
__GCP_PROJECT_ID = str(os.environ.get('_M_PROJECT_ID'))
__TEMPLATE_FILE = "load_status_template.json"
__SECRET_VERSION = 'latest'

def getSecret(account, datasource):
    # Cloud function por http
    client = secretmanager.SecretManagerServiceClient()
    secret_id = f'{account}_{datasource}'
    try:
        secret_name = client.secret_path(__GCP_PROJECT_ID, secret_id) + '/versions/'+__SECRET_VERSION
        response = client.access_secret_version(name=secret_name)
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        msg=f'Error al obtener el secret en getSecret, error:{type(e)}: {e}.'
        print(msg)
        raise(msg)

def getSecretConnection(connection_id):
    # Cloud function por http
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


def readEndpoint(url, user, password, header, params=None) -> json.dumps:
        try:
            # Reintentos cuando falla el end-point
            retry_strategy = Retry(
                total=5,
                backoff_factor=2,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session = requests.Session()
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            
            response = session.get(url=url,  auth=HTTPBasicAuth(user,password), headers=header, params=params)
            if response.status_code == 200:
                return response.json()
            else:
                msg = f'La obtención de datos del endpoint no ha terminado en éxito: {response.status_code}: {response.text}'
                print(msg)
                raise(msg)
        except requests.RequestException as e:
            msg = f'El request para el endpoint: {url} ha fallado, error:{type(e)}: {e}.'
            print(msg)
            raise(msg)

def getLastestSucccessExecution(account, connection_id, datasource, dataset):
    # cloud function por http
    print(f"Extrayendo ultima ejecución de la cuenta {account} para la conexion {connection_id}")
    client = storage.Client()
    bucket = client.bucket(__STORAGE_LASTEST_INCREMENTAL_BUCKET )
    blob_name = f'{datasource}/{dataset}/{connection_id}.json'
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


def postLastestSucccessExecution(account, connection_id, datasource, dataset, field, value):
    # cloud function que escribe en un topic de pubsub = lasted-execution-success
    storage_client = storage.Client()
    bucket = storage_client.bucket(__STORAGE_LASTEST_INCREMENTAL_BUCKET)
    try:
        dir_path = os.path.abspath(os.path.join(os.path.dirname(__file__), './'))
        dir_path_file = os.path.join(dir_path, __TEMPLATE_FILE)
        with open(dir_path_file, 'r') as f:
            template = f.read()
    except Exception as e:
        msg=f'El proceso de leer el template ha fallado, error:{type(e)}: {e}.'
        print(msg)
        raise msg
        
    try:
        rendered = jinja2.Template(template).render(
            account=account,
            connection_id=connection_id,
            datasource=datasource,
            dataset=dataset,
            incrementalstrategy='incremental',
            incrementalfield=field,
            incrementalvalue=value
        )
    except Exception as e:
        msg=f'El proceso de renderizado del template ha fallado, error:{type(e)}: {e}.'
        print(msg)
        raise msg

    try:
        blob_name = f'{datasource}/{dataset}/{connection_id}.json'
        blob = bucket.blob(blob_name)
        blob.upload_from_string(data=rendered,content_type='application/json')  
    except Exception as e:
        msg=f'El guardar el fichero con la ultima carga exitosa en GCS ha fallado, error:{type(e)}: {e}.'
        print(msg)
        raise msg


def _extract_field_names_from_schema(schema, prefix=""):
    """
    Extrae recursivamente todos los nombres de campos de un schema PyArrow
    incluyendo campos anidados en structs y listas
    """
    field_names = set()
    if isinstance(schema, pa.Schema):
        fields = schema
    elif hasattr(schema, '__iter__'):
        fields = schema
    else:
        return field_names
    
    for field in fields:
        current_name = f"{prefix}{field.name}" if prefix else field.name
        field_names.add(current_name)
        if pa.types.is_struct(field.type):
            nested_prefix = f"{current_name}."
            nested_names = _extract_field_names_from_schema(field.type, nested_prefix)
            field_names.update(nested_names)
        elif pa.types.is_list(field.type):
            if pa.types.is_struct(field.type.value_type):
                nested_prefix = f"{current_name}[]."
                nested_names = _extract_field_names_from_schema(field.type.value_type, nested_prefix)
                field_names.update(nested_names)
    return field_names


# ====== NUEVO: Normalización recursiva contra el schema ======

def _prune_to_schema(value, atype: pa.DataType):
    """
    Recorta/normaliza recursivamente 'value' para que encaje en 'atype'.
    - struct: elimina claves extra y normaliza anidados.
    - list<...>: normaliza cada ítem (si no es list, devuelve None).
    - primitivos: si llega dict/list => None (evita ArrowTypeError).
    - No convierte tipos: el cast vendrá después (ya lo tienes implementado).
    """
    if value is None:
        return None

    if pa.types.is_struct(atype):
        if not isinstance(value, dict):
            return None
        out = {}
        for f in atype:
            v = value.get(f.name, None)
            out[f.name] = _prune_to_schema(v, f.type)
        return out

    if pa.types.is_list(atype):
        if not isinstance(value, list):
            return None
        elem_type = atype.value_type
        return [_prune_to_schema(v, elem_type) for v in value]

    if pa.types.is_map(atype):
        if not isinstance(value, dict):
            return None
        key_type = atype.key_type
        item_type = atype.item_type
        items = []
        for k, v in value.items():
            items.append({
                "key": _prune_to_schema(k, key_type),
                "value": _prune_to_schema(v, item_type)
            })
        return items

    # Primitivos: si viene un dict/list donde esperamos escalar => None
    if isinstance(value, (dict, list)):
        return None
    return value


def _clean_record_against_schema(record: dict, schema: pa.Schema) -> dict:
    """Genera un dict con SOLO los campos del schema y valores recortados a su tipo."""
    out = {}
    for f in schema:
        v = record.get(f.name, None)
        out[f.name] = _prune_to_schema(v, f.type)
    return out


def saveStorage(
    account, connection_id, datasource, dataset, data_df,
    folder=None, filename=None, flow_id=None, task_id=None, run_id=None
):
    """
    Guarda en Parquet (GCS) usando el schema del dataset.
    Reglas:
      - Solo se guardan los campos presentes en el schema del dataset (el resto se descartan).
      - Si hay diferencias de tipo, intenta cast seguro; si falla, fuerza cast por columna y solo imprime aviso.
      - Nunca falla por mapeo parcial ni por tipos heterogéneos; en último recurso hace fallback a string.
    """
    
    #print(f"\n{'='*70}")
    #print(f"DEBUG: Diagnosticando datos para dataset '{dataset}'")
    #print(f"{'='*70}")

    # 1) Obtener schema del dataset
    schema = _get_schema(dataset)

    # 2) Normalizar entrada a lista de dicts
    if hasattr(data_df, "to_dict"):
        try:
            records = data_df.to_dict(orient="records")
        except TypeError:
            records = data_df.to_dict("records")
    elif isinstance(data_df, list):
        records = data_df
    else:
        raise TypeError("data_df debe ser pandas.DataFrame o list[dict]")

    # DEBUG: Analizar tipos del primer registro
    if records:
        #print(f"\nAnalizando tipos en primer registro:")
        sample_count = 0
        for key, value in records[0].items():
            if sample_count < 20:
                value_type = type(value).__name__
                #print(f"  {key}: {value_type} = {repr(value)[:100]}")
                sample_count += 1
        #if len(records[0]) > 20:
            #print(f"  ... y {len(records[0]) - 20} campos más")
    
    #print(f"\nCampos en el schema: {len(schema)} campos")
    
    # 3) LIMPIEZA RECURSIVA contra el schema (⬅️ clave para evitar ArrowTypeError)
    try:
        cleaned = [_clean_record_against_schema(rec, schema) for rec in records]
    except Exception as e:
        print(f"Está fallando la limpieza recursiva {e}")
        raise

    # 4) Crear tabla con el schema objetivo
    #print(f"\nCreando tabla PyArrow...")
    try:
        table = pa.Table.from_pylist(cleaned, schema=schema)
        #print(f"✓ Tabla creada exitosamente con {table.num_rows} filas y {len(table.column_names)} columnas")
    except Exception as e:
        print(f"\n{'!'*70}")
        print(f"ERROR al crear tabla PyArrow:")
        print(f"{'!'*70}")
        print(f"{type(e).__name__}: {e}")
        
        # Intentar identificar el campo problemático
        print(f"\nIntentando identificar campo(s) problemático(s)...")
        problematic_fields = []
        for field in schema:
            try:
                single_field_data = [{field.name: rec.get(field.name, None)} for rec in cleaned[:min(5, len(cleaned))]]
                single_schema = pa.schema([field])
                pa.Table.from_pylist(single_field_data, schema=single_schema)
            except Exception as field_error:
                problematic_fields.append(field.name)
                print(f"\n⚠️  CAMPO PROBLEMÁTICO: '{field.name}'")
                print(f"   Tipo esperado en schema: {field.type}")
                if cleaned and field.name in cleaned[0]:
                    print(f"   Valor ejemplo: {repr(cleaned[0].get(field.name, None))[:200]}")
                    print(f"   Tipo del valor: {type(cleaned[0].get(field.name, None)).__name__}")
                print(f"   Error: {field_error}")
        
        if problematic_fields:
            print(f"\n{'='*70}")
            print(f"RESUMEN: {len(problematic_fields)} campo(s) problemático(s) encontrado(s):")
            for pf in problematic_fields:
                print(f"  - {pf}")
            print(f"{'='*70}")
        
        raise

    # 5) Cast seguro/forzado por columna + fallback a string
    forced = []
    try:
        table = table.cast(schema, safe=True)
    except Exception as e:
        print(f"[DEBUG] Error en cast inicial: {e}")
        arrays = []
        for field in schema:
            name = field.name
            col = table[name] if name in table.column_names else pa.nulls(table.num_rows, type=field.type)
            try:
                arrays.append(col.cast(field.type, safe=True))
            except Exception as cast_error:
                print(f"[ERROR] Fallo cast seguro en campo '{name}' (tipo esperado: {field.type})")
                print(f"        Tipo actual: {col.type}")
                print(f"        Error: {cast_error}")
                try:
                    arrays.append(col.cast(field.type, safe=False))
                    forced.append(name)
                    print(f"        ✓ Cast forzado exitoso")
                except Exception as force_error:
                    print(f"        ✗ Cast forzado falló: {force_error}")
                    print(f"        → Aplicando fallback a string")
                    arrays.append(col.cast(pa.string(), safe=False))
                    forced.append(f"{name} (fallback string)")
        table = pa.table(arrays, schema=pa.schema(schema))

    if forced:
        print("[AVISO] Conversión forzada en:", ", ".join(forced))

    # 6) Metadatos
    extracted_ts = int(time.time_ns())
    #print("[DEBUG] Añadiendo columnas de metadatos...")
    try:
        table = table.append_column("_m_extracted_ts", pa.array([extracted_ts] * table.num_rows, type=pa.int64()))
        table = table.append_column("_m_datasource", pa.array([datasource] * table.num_rows, type=pa.string()))
        table = table.append_column("_m_account", pa.array([account] * table.num_rows, type=pa.string()))
        table = table.append_column("_m_connection_id", pa.array([connection_id] * table.num_rows, type=pa.string()))
        table = table.append_column("_m_flow_id", pa.array([flow_id] * table.num_rows, type=pa.string()))
        table = table.append_column("_m_task_id", pa.array([task_id] * table.num_rows, type=pa.string()))
        table = table.append_column("_m_run_id", pa.array([run_id] * table.num_rows, type=pa.string()))
    except Exception as e:
        print(f"  ✗ Error añadiendo metadatos: {e}")
        raise

    # 7) Escritura en GCS
    data_source = f'int_{datasource}'
    file_name = f'{filename}_{connection_id}.parquet' if filename else f'{connection_id}.parquet'
    folder = f'/{folder}' if folder else ''
    destination = f'gs://{__STORAGE_INTEGRATION_BUCKET}/{data_source}/{dataset}{folder}/{file_name}'

    try:
        with fsspec.open(destination, mode="wb") as fout:
            pq.write_table(table, fout, compression='zstd')
        print(f"✓ {table.num_rows} registros guardados en {destination}")
        return __RETURN[1]
    except Exception as e:
        print(f"[ERROR] Fallo al escribir en GCS: {e}")
        raise RuntimeError(f"Error en GCS: {e}")
     

def __putMessageInBus(topic, message):
    publisher = pubsub.PublisherClient()
    topic_name = 'projects/{project_id}/topics/{topic}'.format(
        project_id=__GCP_PROJECT_ID,
        topic=topic,
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
    # La entrada en ini será un 01/01/YYYY 00:00:00:00
    # El valor del fin será siempre algo como 31/12/YYYY 23:59:59:59
    start_year =  datetime.fromtimestamp(ini).year
    end_year =  datetime.fromtimestamp(fin).year
    value_list=[]
    for i in range(start_year,end_year+1):
        primer_semestre_ini = int(datetime(year=i, month=1, day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc).timestamp())
        primer_semestre_fin = int(datetime(year=i, month=6, day=30, hour=23, minute=59, second=59, microsecond=59, tzinfo=timezone.utc).timestamp())
        dupla1 = {primer_semestre_ini,primer_semestre_fin}
        value_list.append(dupla1)
        
        segundo_semestre_ini = int(datetime(year=i, month=7, day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc).timestamp())
        segundo_semestre_fin = int(datetime(year=i, month=12, day=31, hour=23, minute=59, second=59, microsecond=59, tzinfo=timezone.utc).timestamp())
        dupla2 = {segundo_semestre_ini,segundo_semestre_fin}
        value_list.append(dupla2)
    
    return value_list
