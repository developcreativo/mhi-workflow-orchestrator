import json, requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from requests.auth import HTTPBasicAuth
import pyarrow as pa

# ============================================================================
# CONFIGURACIÓN - AJUSTA ESTOS VALORES
# ============================================================================
DATASET_NAME = "property.invoice"
URL = "https://publicapi-silken.ulysescloud.com/public/api/v1/con/chain/1/property/20/invoice?limit=50000&showProductDeposit=true&showProductService=true&showProductAccommodation=true&showProductPayment=true&showProductFee=true&showProductCancellationFee=true&showReservationRoomStaySummary=true&showDetailedCommission=true"
USER = "int-qlik"
PASSWORD = "18Silken88001"
HEADER = {'Accept': 'application/json', 'content-type': 'application/json'}
PARAMS = {"from":"2024-03-03","to":"2024-03-06"
}
DATA_ATTRIBUTE = "list"  # Si los datos están en data['list'], sino None

def readEndpoint(url, user, password, header, params=None) -> json.dumps:
    try:
        # Estos pasos los hacemos para hacer reintentos cuando falla el end-point
        # Define the retry strategy
        retry_strategy = Retry(
            total=5,  # Maximum number of retries
            backoff_factor=2, # Time to wait between retries
            status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
        )

        # Create an HTTP adapter with the retry strategy and mount it to session
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

# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================
def map_arrow_type_to_code(field_type, field_name="", depth=0):
    """
    Convierte un tipo PyArrow a código Python
    Maneja structs anidados recursivamente con indentación correcta
    
    Args:
        field_type: Tipo PyArrow
        field_name: Nombre del campo
        depth: Nivel de anidamiento actual (para indentación)
    """
    base_indent = '    ' * (2 + depth)
    
    # Structs (recursivo)
    if pa.types.is_struct(field_type):
        # Verificar si TODOS los campos del struct son null
        all_null = True
        for i in range(field_type.num_fields):
            sub_field = field_type[i]
            if sub_field.type != pa.null():
                all_null = False
                break
        
        # Si todos son null, mejor convertir a string para evitar problemas
        if all_null:
            return 'pa.string()  # Struct vacío convertido a string'
        
        lines = ['pa.struct([']
        for i in range(field_type.num_fields):
            sub_field = field_type[i]
            # Normalizar nombres
            field_name_normalized = sub_field.name
            field_name_normalized = field_name_normalized.replace('nameI21n', 'nameI18n')
            field_name_normalized = field_name_normalized.replace('descriptionI21n', 'descriptionI18n')
            
            sub_type_code = map_arrow_type_to_code(
                sub_field.type, 
                field_name_normalized, 
                depth + 1
            )
            
            if '\n' in sub_type_code:
                lines.append(f'{base_indent}    pa.field("{field_name_normalized}",')
                for line in sub_type_code.split('\n'):
                    lines.append(f'{base_indent}        {line}')
                lines.append(f'{base_indent}    , nullable={sub_field.nullable}),')
            else:
                lines.append(f'{base_indent}    pa.field("{field_name_normalized}", {sub_type_code}, nullable={sub_field.nullable}),')
        
        lines.append(f'{base_indent}])')
        return '\n'.join(lines)
    
    # Lists
    elif pa.types.is_list(field_type):
        value_type = field_type.value_type
        inner_type_code = map_arrow_type_to_code(value_type, "", depth)
        
        if '\n' in inner_type_code:
            lines = ['pa.list_(']
            for line in inner_type_code.split('\n'):
                lines.append(f'{base_indent}    {line}')
            lines.append(f'{base_indent})')
            return '\n'.join(lines)
        else:
            return f'pa.list_({inner_type_code})'
    
    # Tipos primitivos
    elif pa.types.is_int64(field_type):
        return 'pa.int64()'
    elif pa.types.is_int32(field_type):
        return 'pa.int32()'
    elif pa.types.is_float64(field_type):
        return 'pa.float64()'
    elif pa.types.is_float32(field_type):
        return 'pa.float64()'
    elif pa.types.is_string(field_type) or pa.types.is_large_string(field_type):
        return 'pa.string()'
    elif pa.types.is_boolean(field_type):
        return 'pa.bool_()'
    elif pa.types.is_timestamp(field_type):
        return "pa.timestamp('us', tz='UTC')"
    elif pa.types.is_date32(field_type) or pa.types.is_date64(field_type):
        return 'pa.date32()'
    elif pa.types.is_decimal(field_type):
        return f'pa.decimal128({field_type.precision}, {field_type.scale})'
    elif pa.types.is_null(field_type):
        # Si el tipo es null, usar string como fallback
        return 'pa.string()  # Null type convertido a string'
    else:
        return f'pa.string()  # Original: {field_type}'


def generate_schema_code(sample_size=1000):
    """
    Genera schema estable convirtiendo structs con baja completitud a JSON string
    """
    
    print("Obteniendo datos de la API...")
    data = readEndpoint(URL, USER, PASSWORD, HEADER, PARAMS)
    
    if DATA_ATTRIBUTE:
        records = data[DATA_ATTRIBUTE]
    else:
        records = data if isinstance(data, list) else [data]
    
    if not records:
        raise RuntimeError("No se obtuvieron datos de la API")
    
    print(f"Se obtuvieron {len(records)} registros totales")
    
    # Usar la mayor muestra posible
    records_to_analyze = records[:sample_size] if len(records) > sample_size else records
    print(f"Analizando {len(records_to_analyze)} registros...")
    
    # CORRECCIÓN: Convertir a DataFrame validando estructura
    import pandas as pd
    
    # Verificar que todos los registros sean diccionarios
    if not all(isinstance(r, dict) for r in records_to_analyze):
        raise RuntimeError("Los registros no son diccionarios válidos")
    
    df = pd.DataFrame(records_to_analyze)
    
    # Verificar que las columnas sean strings
    print(f"Columnas detectadas: {list(df.columns)[:10]}...")  # Mostrar primeras 10
    
    # PASO CRÍTICO: Detectar y convertir structs con baja completitud
    print("\nAnalizando completitud de campos...")
    fields_converted = []
    
    for col in df.columns:
        # Asegurar que col es un nombre válido
        if not isinstance(col, str):
            print(f"⚠️ Columna con nombre no-string detectada: {col} (tipo: {type(col)})")
            continue
            
        try:
            non_null_count = df[col].notna().sum()
            completeness_pct = (non_null_count / len(df)) * 100
            
            # Verificar si contiene structs
            sample_non_null = df[col].dropna()
            if len(sample_non_null) > 0:
                has_struct = sample_non_null.apply(lambda x: isinstance(x, dict)).any()
                
                # Si tiene structs y baja completitud (<50%), convertir a JSON string
                if has_struct and completeness_pct < 50:
                    print(f"  {col}: {completeness_pct:.1f}% completitud - CONVIRTIENDO A STRING")
                    df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
                    fields_converted.append(col)
        except Exception as e:
            print(f"⚠️ Error procesando columna '{col}': {e}")
            continue
    
    if fields_converted:
        print(f"\n✓ {len(fields_converted)} campos convertidos a JSON string")
    
    # Ahora convertir a Arrow - los structs problemáticos ya son strings
    table = pa.Table.from_pandas(df, preserve_index=False)
    
    print(f"\nCampos en schema: {len(table.schema)}")
    
    # Detectar profundidad
    max_depth = 0
    def get_depth(field_type, current_depth=0):
        nonlocal max_depth
        if pa.types.is_struct(field_type):
            current_depth += 1
            max_depth = max(max_depth, current_depth)
            for i in range(field_type.num_fields):
                get_depth(field_type[i].type, current_depth)
        elif pa.types.is_list(field_type):
            get_depth(field_type.value_type, current_depth)
    
    for field in table.schema:
        get_depth(field.type)
    
    print(f"Profundidad de anidamiento: {max_depth}")
    
    # Generar código
    lines = []
    lines.append('"""')
    lines.append(f'Schema PyArrow para {DATASET_NAME}')
    lines.append(f'Generado desde {len(records_to_analyze)} registros')
    if fields_converted:
        lines.append(f'Campos convertidos a JSON string por baja completitud (<50%):')
        for field in fields_converted:
            lines.append(f'  - {field}')
    lines.append(f'Profundidad: {max_depth} niveles')
    lines.append('"""')
    lines.append('import pyarrow as pa')
    lines.append('')
    lines.append('')
    lines.append('def get_schema():')
    lines.append('    """')
    lines.append(f'    Retorna el schema PyArrow para {DATASET_NAME}')
    lines.append('    """')
    lines.append('    ')
    lines.append('    schema = pa.schema([')
    
    for field in table.schema:
        field_name = field.name.replace('nameI21n', 'nameI18n').replace('descriptionI21n', 'descriptionI18n')
        type_code = map_arrow_type_to_code(field.type, field_name, depth=0)
        
        if '\n' in type_code:
            lines.append(f'        pa.field("{field_name}",')
            for line in type_code.split('\n'):
                lines.append(f'            {line}')
            lines.append(f'        , nullable={field.nullable}),')
        else:
            lines.append(f'        pa.field("{field_name}", {type_code}, nullable={field.nullable}),')
    
    lines.append('    ])')
    lines.append('    ')
    lines.append('    metadata = {')
    lines.append(f'        b"dataset_name": b"{DATASET_NAME}",')
    lines.append('        b"dataset_version": b"v1",')
    lines.append(f'        b"dataset_description": b"Schema for {DATASET_NAME}"')
    lines.append('    }')
    lines.append('    ')
    lines.append('    return schema.with_metadata(metadata)')
    
    return '\n'.join(lines)

# ============================================================================
# EJECUTAR
# ============================================================================
if __name__ == "__main__":
    try:
        print("="*70)
        print("GENERADOR DE SCHEMA PYARROW - MULTI-NIVEL")
        print("="*70)
        
        # Puedes cambiar sample_size aquí según necesites
        code = generate_schema_code(sample_size=200)
        
        print("\n" + "="*70)
        print("CÓDIGO GENERADO")
        print("="*70)
        print()
        print(code)
        print()
        
        # Guardar en archivo
        filename = f"{DATASET_NAME}.py"
        
        print("="*70)
        print(f"Guardando schema en archivo: {filename}")
        print("="*70)
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(code)
        
        print(f"✓ Archivo guardado exitosamente: {filename}")
        print("="*70)
        
    except Exception as e:
        print(f"\nERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()