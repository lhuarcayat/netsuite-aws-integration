import sys
import boto3
import json
import time
import jwt
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import StringIO
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError

# -------------------------------
# CONFIGURACIÓN DYNAMODB
# -------------------------------
class WatermarkManager:
    def __init__(self, table_name, region_name):
        self.dynamodb = boto3.resource('dynamodb', region_name=region_name)
        self.table = self.dynamodb.Table(table_name)
        
    def get_watermark(self, table_name):
        """Obtener último watermark para una tabla"""
        try:
            response = self.table.get_item(Key={'table_name': table_name})
            if 'Item' in response:
                return {
                    'last_modified_date': response['Item']['last_modified_date'],
                    'last_execution_date': response['Item']['last_execution_date'],
                    'total_records': response['Item'].get('total_records', 0)
                }
            return None
        except ClientError as e:
            print(f"Error obteniendo watermark para {table_name}: {e}")
            return None
    
    def update_watermark(self, table_name, last_modified_date, execution_date, record_count):
        """Actualizar watermark para una tabla"""
        try:
            self.table.put_item(
                Item={
                    'table_name': table_name,
                    'last_modified_date': last_modified_date,
                    'last_execution_date': execution_date,
                    'total_records': record_count,
                    'updated_at': datetime.utcnow().isoformat()
                }
            )
            print(f"Watermark actualizado para {table_name}: {last_modified_date}")
        except ClientError as e:
            print(f"Error actualizando watermark para {table_name}: {e}")

# -------------------------------
# FUNCIÓN PARA CARGAR COLUMNAS DESDE S3
# -------------------------------
def load_table_columns_from_s3(bucket_name, key_path):
    """Cargar las columnas de las tablas desde un archivo JSON en S3"""
    try:
        s3_client = boto3.client("s3")
        print(f"Cargando configuración de columnas desde s3://{bucket_name}/{key_path}")
        
        json_obj = s3_client.get_object(Bucket=bucket_name, Key=key_path)
        json_content = json_obj["Body"].read().decode("utf-8")
        table_columns = json.loads(json_content)
        
        return table_columns
        
    except Exception as e:
        print(f"Error cargando configuración de columnas desde S3: {e}")
        return {}

def normalize_table_name(table_name):
    """Normalizar nombres de tabla para hacer coincidencias"""
    return table_name.lower()

def convert_lastmodifieddate_to_datetime(df):
    """Convertir columna lastmodifieddate de string DD-MM-YYYY a datetime"""
    if 'lastmodifieddate' in df.columns:
        try:
            # Convertir de DD-MM-YYYY string a datetime
            df['lastmodifieddate'] = pd.to_datetime(df['lastmodifieddate'], format='%d/%m/%Y', errors='coerce')
            print(f"✅ Convertido lastmodifieddate a datetime. Registros con fechas válidas: {df['lastmodifieddate'].notna().sum()}")
            
            # Mostrar rango de fechas para verificación
            if df['lastmodifieddate'].notna().any():
                min_date = df['lastmodifieddate'].min()
                max_date = df['lastmodifieddate'].max()
                print(f"📅 Rango de fechas: {min_date} - {max_date}")
            
        except Exception as e:
            print(f"⚠️  Error convirtiendo lastmodifieddate: {e}")
    return df

def enforce_table_schema(df, table_name, table_columns_config):
    """
    FUNCIÓN CORREGIDA: Mantiene TODAS las columnas existentes y agrega las faltantes
    Ya no elimina columnas extra - las conserva todas
    """
    try:
        normalized_table = normalize_table_name(table_name)
        
        if normalized_table not in table_columns_config:
            print(f"⚠️  No hay schema definido para {table_name}, usando columnas existentes")
            return df
        
        expected_columns = table_columns_config[normalized_table]
        current_columns = list(df.columns)
        
        print(f"🔧 Enforzando schema para {table_name}")
        print(f"📋 Columnas esperadas: {len(expected_columns)}")
        print(f"📋 Columnas actuales: {len(current_columns)}")
        
        # CAMBIO: Crear DataFrame manteniendo TODAS las columnas actuales
        df_schema_enforced = df.copy()
        
        # Agregar solo las columnas faltantes del schema
        missing_columns = set(expected_columns) - set(current_columns)
        for col in missing_columns:
            df_schema_enforced[col] = None
            print(f"➕ Agregada columna faltante del schema: {col}")
        
        # Reportar columnas extra (pero YA NO las eliminamos)
        extra_columns = set(current_columns) - set(expected_columns)
        if extra_columns:
            print(f"ℹ️  Columnas extra conservadas: {extra_columns}")
        
        print(f"✅ Schema aplicado. Columnas finales: {len(df_schema_enforced.columns)}")
        print(f"   - Columnas del schema: {len(expected_columns)}")
        print(f"   - Columnas extra conservadas: {len(extra_columns)}")
        
        return df_schema_enforced
        
    except Exception as e:
        print(f"❌ Error enforzando schema para {table_name}: {e}")
        return df

def get_max_lastmodified_date(df):
    """Obtener la fecha máxima de lastmodifieddate de manera segura"""
    if 'lastmodifieddate' not in df.columns:
        return None
    
    try:
        # Convertir temporalmente a datetime para obtener el máximo
        df_temp = df.copy()
        # Convertir de DD/MM/YYYY string a datetime para comparación
        df_temp['lastmodifieddate_dt'] = pd.to_datetime(df_temp['lastmodifieddate'], format='%d/%m/%Y', errors='coerce')
        
        # Obtener el máximo excluyendo valores nulos
        valid_dates = df_temp['lastmodifieddate_dt'].dropna()
        if len(valid_dates) > 0:
            max_date = valid_dates.max()
            print(f"📅 Fecha máxima encontrada: {max_date}")
            return max_date
        
        return None
        
    except Exception as e:
        print(f"⚠️ Error obteniendo fecha máxima: {e}")
        return None

def create_dataframe_with_all_strings(records):
    """
    Crear DataFrame forzando TODAS las columnas como string
    Evita completamente la inferencia automática de pandas
    """
    if not records:
        return pd.DataFrame()
    
    print("📊 Creando DataFrame con todas las columnas como string...")
    
    # Crear DataFrame normal primero
    df = pd.DataFrame(records)
    
    # Forzar TODAS las columnas a string
    for col in df.columns:
        df[col] = df[col].astype("string")
        # Convertir 'nan' string de vuelta a None para mantener nulls
        df.loc[df[col] == 'nan', col] = None
    
    print(f"✅ DataFrame creado con {len(df.columns)} columnas (todas string) y {len(df)} filas")
    return df

def create_pyarrow_string_schema(df):
    """
    Crear esquema PyArrow donde TODAS las columnas son string
    Esto evita que PyArrow haga inferencia automática de tipos
    """
    try:
        # Crear schema donde todas las columnas son string
        fields = []
        for column in df.columns:
            fields.append(pa.field(column, pa.string()))
        
        schema = pa.schema(fields)
        print(f"🔧 Schema PyArrow creado con {len(fields)} columnas como string")
        return schema
    
    except Exception as e:
        print(f"❌ Error creando schema PyArrow: {e}")
        return None

def save_dataframe_as_parquet_with_string_schema(df, s3_path):
    """
    Guardar DataFrame como Parquet usando esquema de string explícito
    Evita inferencia automática de PyArrow
    """
    try:
        print(f"💾 Guardando DataFrame con schema string explícito en: {s3_path}")
        
        # Crear schema donde todas las columnas son string
        schema = create_pyarrow_string_schema(df)
        if schema is None:
            # Fallback al método anterior si falla
            df.to_parquet(s3_path, engine='pyarrow', compression='snappy', index=False)
            return True
        
        # Convertir DataFrame a PyArrow Table con schema explícito
        # Primero convertir valores None a string "null" para PyArrow
        df_for_arrow = df.copy()
        for col in df_for_arrow.columns:
            df_for_arrow[col] = df_for_arrow[col].fillna("null").astype(str)
        
        # Crear PyArrow Table con schema explícito
        table = pa.Table.from_pandas(df_for_arrow, schema=schema, preserve_index=False)
        
        # Escribir usando PyArrow directamente
        pq.write_table(table, s3_path, compression='snappy')
        
        print(f"✅ Archivo Parquet guardado exitosamente con schema string")
        return True
        
    except Exception as e:
        print(f"❌ Error guardando con schema string explícito: {e}")
        print("🔄 Intentando método fallback...")
        
        # Fallback al método anterior
        try:
            df.to_parquet(s3_path, engine='pyarrow', compression='snappy', index=False)
            print("✅ Guardado exitoso con método fallback")
            return True
        except Exception as e2:
            print(f"❌ Error también con método fallback: {e2}")
            return False

# -------------------------------
# PARSEAR ARGUMENTOS
# -------------------------------
args = getResolvedOptions(sys.argv, [
    "INPUT_S3_BUCKET_NAME",
    "S3_KEY_PATH",
    "OUTPUT_S3_BUCKET_NAME",
    "SECRET_NAME",
    "REGION_NAME",
    "TABLE_COLUMNS_S3_KEY",
    "DYNAMODB_WATERMARK_TABLE" 
])

S3_INPUT_BUCKET = args["INPUT_S3_BUCKET_NAME"]
S3_KEY_PATH = args["S3_KEY_PATH"]
SECRET_NAME = args["SECRET_NAME"]
REGION_NAME = args["REGION_NAME"]
S3_OUTPUT_BUCKET = args['OUTPUT_S3_BUCKET_NAME']
TABLE_COLUMNS_S3_KEY = args["TABLE_COLUMNS_S3_KEY"]
DYNAMODB_WATERMARK_TABLE = args["DYNAMODB_WATERMARK_TABLE"]

# Fecha de ejecución para particionamiento
EXECUTION_DATE = datetime.utcnow().strftime('%Y-%m-%d')

# Inicializar watermark manager
watermark_manager = WatermarkManager(DYNAMODB_WATERMARK_TABLE, REGION_NAME)

# -------------------------------
# OBTENER CREDENCIALES DESDE SECRETS MANAGER
# -------------------------------
secrets_client = boto3.client("secretsmanager", region_name=REGION_NAME)
secret_value = secrets_client.get_secret_value(SecretId=SECRET_NAME)
creds = json.loads(secret_value["SecretString"])

ACCOUNT_ID = creds["ACCOUNT_ID"]
CLIENT_ID = creds["CLIENT_ID"]
CLIENT_SECRET = creds["CLIENT_SECRET"]
CERTIFICATE_ID = creds["CERTIFICATE_ID"]
SCOPE = "rest_webservices"

# -------------------------------
# DESCARGAR CLAVE PRIVADA DESDE S3
# -------------------------------
s3_client = boto3.client("s3")
pem_obj = s3_client.get_object(Bucket=S3_INPUT_BUCKET, Key=S3_KEY_PATH)
private_key = pem_obj["Body"].read().decode("utf-8")

# -------------------------------
# CREAR JWT ASSERTION Y OBTENER TOKEN
# -------------------------------
def get_access_token():
    now = int(time.time())
    payload = {
        "iss": CLIENT_ID,
        "scope": SCOPE,
        "aud": f"https://{ACCOUNT_ID}.suitetalk.api.netsuite.com/services/rest/auth/oauth2/v1/token",
        "iat": now,
        "exp": now + 300
    }
    
    headers = {
        "alg": "PS256",
        "typ": "JWT",
        "kid": CERTIFICATE_ID
    }
    
    client_assertion = jwt.encode(payload, private_key, algorithm="PS256", headers=headers)
    
    token_url = f"https://{ACCOUNT_ID}.suitetalk.api.netsuite.com/services/rest/auth/oauth2/v1/token"
    
    data = {
        "grant_type": "client_credentials",
        "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        "client_assertion": client_assertion
    }
    
    resp = requests.post(token_url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
    if resp.status_code == 200:
        access_token = resp.json()["access_token"]
        print("Token obtenido con éxito")
        return access_token
    else:
        print("Error al obtener token:", resp.status_code, resp.text)
        return None

# -------------------------------
# CONFIGURACIÓN DE TABLAS
# -------------------------------
entity_tables = ["bom", "customer", "customersubsidiaryrelationship", "inventorybalance", "item", "vendor", "vendorsubsidiaryrelationship"]
transactions_tables = ["CustPymt", "CustInvc", "Opprtnty", "PurchOrd", "SalesOrd"]
tables = entity_tables + transactions_tables

# Cargar definición de columnas desde S3
table_columns = load_table_columns_from_s3(S3_INPUT_BUCKET, TABLE_COLUMNS_S3_KEY)

print("=== CONFIGURACIÓN CDC CON SCHEMA STRING FORZADO ===")
print(f"Fecha de ejecución: {EXECUTION_DATE}")
print(f"Tabla DynamoDB watermarks: {DYNAMODB_WATERMARK_TABLE}")
print(f"Schema enforcement: HABILITADO (string forzado en PyArrow)")
print("=" * 50)

# -------------------------------
# PROCESAR TABLAS CON CDC Y SCHEMA ENFORCEMENT
# -------------------------------
record_url = f"https://{ACCOUNT_ID}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"

start_job = time.time()
access_token = get_access_token()
if not access_token:
    print("Error: No se pudo obtener access token")
    sys.exit(1)

successful_tables = 0

for table in tables:
    print(f"\n--- Procesando tabla: {table} ---")
    
    normalized_table = normalize_table_name(table)
    
    # Verificar que la tabla tenga columnas definidas
    if normalized_table not in table_columns:
        print(f"Error: No se han definido columnas para la tabla '{table}'. Saltando...")
        continue
    
    # Obtener watermark existente
    watermark_info = watermark_manager.get_watermark(table)
    is_first_run = watermark_info is None
    
    if is_first_run:
        print(f"🆕 Primera ejecución para {table} - Extrayendo todos los registros")
        where_clause = ""
    else:
        last_date = watermark_info['last_modified_date']
        print(f"🔄 Extracción incremental para {table} desde: {last_date}")
        
        if table in entity_tables:
            # Entity tables: usar WHERE directo
            where_clause = f" WHERE lastmodifieddate >= TO_DATE('{last_date}', 'YYYY-MM-DD')"
        else:
            # Transaction tables: usar AND con TO_DATE ya que tienen WHERE type='table'
            where_clause = f" AND lastmodifieddate >= TO_DATE('{last_date}', 'YYYY-MM-DD')"
    
    # Preparar columnas
    columns = table_columns[normalized_table]
    select_fields = ",".join(columns)
    
    # Construir query con filtro CDC
    if table in entity_tables:
        base_query = f"SELECT {select_fields} FROM {table}"
    else:
        base_query = f"SELECT {select_fields} FROM transaction WHERE type='{table}'"
    
    sql_query = base_query + where_clause
    sql = {"q": sql_query}
    
    print(f"Query CDC: {sql_query[:100]}...")
    
    # Extraer datos con paginación
    df_table = pd.DataFrame()
    limit = 1000
    offset = 0
    start_table = time.time()
    max_last_modified_date = None
    
    try:
        while True:
            print(f"Consultando {table} | offset={offset}")
            
            res = requests.post(record_url, headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "Prefer": "transient"
            }, json=sql, params={"limit": limit, "offset": offset})

            if res.status_code == 200:
                data = res.json()
                cant_records = data.get("count", 0)
                
                if cant_records > 0:
                    records = data.get("items", [])
                    print(f"Datos obtenidos: {cant_records} registros")
                    
                    df_page = create_dataframe_with_all_strings(records)
                    
                    # Obtener la fecha máxima correctamente
                    page_max_date = get_max_lastmodified_date(df_page)
                    if page_max_date is not None:
                        if max_last_modified_date is None or page_max_date > max_last_modified_date:
                            max_last_modified_date = page_max_date
                    
                    df_table = pd.concat([df_table, df_page], ignore_index=True)
                
                if data.get("hasMore", False):
                    offset += limit
                else:
                    break
                    
            elif res.status_code == 401:
                print("Token expirado, renovando...")
                access_token = get_access_token()
                if not access_token:
                    break
                continue
            else:
                print(f"Error SuiteQL: {res.status_code} - {res.text}")
                break

        total_records = len(df_table)
        print(f"Total registros extraídos para {table}: {total_records}")
        
        if total_records > 0:
            # CORREGIDO: Enforza schema manteniendo todas las columnas
            df_table = enforce_table_schema(df_table, table, table_columns)
            
            # Logging mejorado después de aplicar schema
            print(f"📊 Resumen columnas para {table}:")
            print(f"   - Total columnas finales: {len(df_table.columns)}")
            expected_cols = len(table_columns.get(normalized_table, []))
            extra_cols = len(df_table.columns) - expected_cols
            print(f"   - Del schema: {expected_cols}")
            print(f"   - Extra conservadas: {extra_cols}")
            
            # IMPORTANTE: NO convertir lastmodifieddate a datetime aquí
            # porque luego se fuerza todo a string y cambia el formato
            # Silver esperará el formato original DD/MM/YYYY
            print("⚠️ Manteniendo lastmodifieddate en formato original para compatibilidad con Silver")
            
            # Guardar con particionamiento por fecha de ejecución
            partition_path = f"s3://{S3_OUTPUT_BUCKET}/{table}/execution_date={EXECUTION_DATE}/"
            
            # Generar nombre de archivo único
            timestamp = datetime.utcnow().strftime('%H%M%S')
            file_name = f"{table}_{EXECUTION_DATE}_{timestamp}.parquet"
            s3_full_path = f"{partition_path}{file_name}"
            
            try:
                # NUEVO: Usar función que fuerza schema string en PyArrow
                success = save_dataframe_as_parquet_with_string_schema(df_table, s3_full_path)
                
                if success:
                    print(f"✅ Datos guardados en: {s3_full_path}")
                    print(f"📋 Columnas guardadas: {len(df_table.columns)}")
                    print(f"🔧 Schema: TODAS las columnas como STRING")
                    
                    # Actualizar watermark si se extrajeron datos
                    if max_last_modified_date:
                        # Convertir datetime a string para DynamoDB
                        watermark_date = max_last_modified_date.strftime('%Y-%m-%d')
                        watermark_manager.update_watermark(
                            table, 
                            watermark_date,
                            EXECUTION_DATE,
                            total_records
                        )
                    
                    successful_tables += 1
                else:
                    print(f"❌ Error guardando archivo Parquet para {table}")
                
            except Exception as e:
                print(f"❌ Error guardando en S3: {e}")
        else:
            print(f"ℹ️  No hay datos nuevos para {table}")
            
            # Para tablas sin datos, crear DataFrame vacío con schema correcto
            if not is_first_run:
                # Crear archivo vacío con schema correcto para mantener consistencia
                empty_df = pd.DataFrame(columns=table_columns[normalized_table])
                empty_df = enforce_table_schema(empty_df, table, table_columns)
                
                partition_path = f"s3://{S3_OUTPUT_BUCKET}/{table}/execution_date={EXECUTION_DATE}/"
                timestamp = datetime.utcnow().strftime('%H%M%S')
                file_name = f"{table}_{EXECUTION_DATE}_{timestamp}_empty.parquet"
                s3_full_path = f"{partition_path}{file_name}"
                
                # Usar la nueva función también para archivos vacíos
                success = save_dataframe_as_parquet_with_string_schema(empty_df, s3_full_path)
                
                if success:
                    print(f"📄 Archivo vacío con schema guardado: {s3_full_path}")
                    
                    watermark_manager.update_watermark(
                        table,
                        watermark_info['last_modified_date'],
                        EXECUTION_DATE,
                        0
                    )
        
        end_table = time.time()
        print(f"⏱️  Tiempo de procesamiento: {end_table - start_table:.2f} seg")
        
    except Exception as e:
        print(f"❌ Error procesando tabla {table}: {e}")
        
    finally:
        # Limpiar memoria
        del df_table

end_job = time.time()
print(f"\n=== RESUMEN DE EJECUCIÓN ===")
print(f"📅 Fecha de ejecución: {EXECUTION_DATE}")
print(f"✅ Tablas procesadas exitosamente: {successful_tables}/{len(tables)}")
print(f"🔧 Schema enforcement: STRING FORZADO (evita conflictos Spark)")
print(f"⏱️  Tiempo total: {end_job - start_job:.2f} segundos")
print("=" * 50)