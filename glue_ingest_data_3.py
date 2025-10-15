import sys
import boto3
import json
import time
import jwt
import requests
import pandas as pd
from io import StringIO
from awsglue.utils import getResolvedOptions

# -------------------------------
# FUNCIÓN PARA CARGAR COLUMNAS DESDE S3
# -------------------------------
def load_table_columns_from_s3(bucket_name, key_path):
    """Cargar las columnas de las tablas desde un archivo JSON en S3"""
    try:
        s3_client = boto3.client("s3")
        print(f"Cargando configuración de columnas desde s3://{bucket_name}/{key_path}")
        
        # Descargar el archivo JSON desde S3
        json_obj = s3_client.get_object(Bucket=bucket_name, Key=key_path)
        json_content = json_obj["Body"].read().decode("utf-8")
        
        # Parsear el JSON
        table_columns = json.loads(json_content)
        
        return table_columns
        
    except Exception as e:
        print(f"Error cargando configuración de columnas desde S3: {e}")
        # Retornar configuración de fallback vacía
        return {}

def normalize_table_name(table_name):
    """Normalizar nombres de tabla para hacer coincidencias"""
    # Convertir a minúsculas para hacer la búsqueda
    return table_name.lower()

# -------------------------------
# PARSEAR ARGUMENTOS
# -------------------------------
args = getResolvedOptions(sys.argv, [
    "INPUT_S3_BUCKET_NAME",
    "S3_KEY_PATH",
    "OUTPUT_S3_BUCKET_NAME",
    "SECRET_NAME",
    "REGION_NAME",
    "TABLE_COLUMNS_S3_KEY" 
])

S3_INPUT_BUCKET = args["INPUT_S3_BUCKET_NAME"]
S3_KEY_PATH = args["S3_KEY_PATH"]
SECRET_NAME = args["SECRET_NAME"]
REGION_NAME = args["REGION_NAME"]
S3_OUTPUT_BUCKET = args['OUTPUT_S3_BUCKET_NAME']
TABLE_COLUMNS_S3_KEY = args["TABLE_COLUMNS_S3_KEY"] 

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

bucket_name = S3_INPUT_BUCKET
key_name = S3_KEY_PATH

pem_obj = s3_client.get_object(Bucket=bucket_name, Key=key_name)
private_key = pem_obj["Body"].read().decode("utf-8")

# -------------------------------
# CREAR JWT ASSERTION
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
    
    client_assertion = jwt.encode(
        payload,
        private_key,
        algorithm="PS256",
        headers=headers
    )
    
    # -------------------------------
    # SOLICITAR ACCESS TOKEN
    # -------------------------------
    token_url = f"https://{ACCOUNT_ID}.suitetalk.api.netsuite.com/services/rest/auth/oauth2/v1/token"
    
    data = {
        "grant_type": "client_credentials",
        "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        "client_assertion": client_assertion
    }
    
    resp = requests.post(token_url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
    if resp.status_code == 200:
        access_token = resp.json()["access_token"]
        print("Token obtenido con exito")
        return access_token
    else:
        print("Error al obtener token:", resp.status_code, resp.text)

# -------------------------------
# CONFIGURACIÓN DE TABLAS Y CARGAR COLUMNAS DESDE S3
# -------------------------------
entity_tables = ["customer", "customersubsidiaryrelationship", "vendor","vendorsubsidiaryrelationship"]
transactions_tables = ["CustPymt", "CustInvc", "Opprtnty", "PurchOrd", "SalesOrd"]
tables = entity_tables + transactions_tables

# Cargar definición de columnas desde S3
table_columns = load_table_columns_from_s3(S3_INPUT_BUCKET, TABLE_COLUMNS_S3_KEY)

# Mostrar las columnas definidas para cada tabla
print("=== COLUMNAS DEFINIDAS POR TABLA (desde S3) ===")
for table in tables:
    # Normalizar el nombre de la tabla para buscar en el JSON
    normalized_table = normalize_table_name(table)
    
    if normalized_table in table_columns:
        columns_count = len(table_columns[normalized_table])
        print(f"{table} -> {normalized_table}: {columns_count} columnas")
        print(f"  Primeras 10 columnas: {table_columns[normalized_table][:10]}")
    else:
        print(f"{table} -> {normalized_table}: ¡SIN COLUMNAS DEFINIDAS!")
print("=" * 50)

# -------------------------------
#  CONSUMIR EL API
# -------------------------------
record_url = f"https://{ACCOUNT_ID}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"

start_job = time.time()
access_token = get_access_token()
successful_tables = 0

for table in tables:
    print(f"\n--- Procesando tabla: {table} ---")
    
    normalized_table = normalize_table_name(table)
    
    # Verificar que la tabla tenga columnas definidas en el JSON
    if normalized_table not in table_columns:
        print(f"Error: No se han definido columnas para la tabla '{table}' (buscado como '{normalized_table}') en el archivo JSON. Saltando...")
        continue
    
    df_table = pd.DataFrame()
    limit = 1000
    offset = 0
    
    # Obtener las columnas específicas para esta tabla desde el JSON
    columns = table_columns[normalized_table]
    select_fields = ",".join(columns)
    print(f"Total de columnas a extraer para {table}: {len(columns)}")
    print(f"Primeras 15 columnas: {columns[:15]}")
    
    # Construir query según el tipo de tabla
    if table in entity_tables:
        sql = {"q": f"SELECT {select_fields} FROM {table}"}
    else:
        sql = {"q": f"SELECT {select_fields} FROM transaction WHERE type='{table}'"}
    
    print(f"SQL Query construido para {table}")
    start_table = time.time()
    
    try:
        while True:
            print(f"Consultando tabla {table} | limit={limit}&offset={offset}")
            res = requests.post(record_url, headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "Prefer": "transient"
            },
            json=sql,
            params={"limit": limit, "offset": offset})

            if res.status_code == 200:
                data = res.json()
                cant_records = data.get("count")
                if cant_records > 0:
                    records = data.get("items")
                    print(f"Datos obtenidos de la tabla {table}: {cant_records} registros.")
                    
                    df_page = pd.DataFrame(records)
                    df_table = pd.concat([df_table, df_page], ignore_index=True)
                
                if data.get("hasMore", False):
                    offset += limit
                else:
                    print(f"Ya no hay más páginas en la tabla {table}.")
                    break
                    
            elif res.status_code == 401 or "INVALID_LOGIN" in res.text:
                print("Token expirado, renovando...")
                access_token = get_access_token()
                continue
            else:
                print("Error SuiteQL:", res.status_code, res.text)
                break

        cant = len(df_table.index)
        print(f"Cantidad de registros de la tabla {table}: {cant}")
        
        # Mostrar qué columnas se extrajeron realmente
        if not df_table.empty:
            extracted_columns = list(df_table.columns)
            print(f"Columnas extraídas en el DataFrame: {len(extracted_columns)} de {len(columns)} esperadas")
            print(f"Primeras 10 columnas extraídas: {extracted_columns[:10]}")
            
            # Verificar si hay columnas faltantes
            missing_columns = set(columns) - set(extracted_columns)
            if missing_columns:
                print(f"Columnas no encontradas en NetSuite: {list(missing_columns)[:10]}{'...' if len(missing_columns) > 10 else ''}")
        
        # Guardar en S3 si hay datos
        if cant > 0:
            print('Guardar el DataFrame en formato Parquet en S3')
            s3_path = f"s3://{S3_OUTPUT_BUCKET}/{table}/{table}.parquet"
            try:
                df_table.to_parquet(s3_path, engine='pyarrow', compression='snappy', index=False)
                print(f"Archivo registrado correctamente en S3 en la ruta: {s3_path}")
                successful_tables += 1
            except Exception as e:
                print(f"Error guardando en S3: {e}")
        else:
            print(f"No hay datos para guardar en la tabla {table}")
        
        end_table = time.time()
        print(f"Tiempo de procesamiento de la tabla {table}: {end_table - start_table:.2f} seg")
        
    except Exception as e:
        print(f"Error procesando tabla {table}: {e}")
        # Continuar con la siguiente tabla
        
    finally:
        # Limpiar memoria
        del df_table

end_job = time.time()
print(f"\n=== Migración completada ===")
print(f"Tablas procesadas exitosamente: {successful_tables}/{len(tables)}")
print(f"Migración de datos desde NetSuite completada: {end_job - start_job:.2f} seg")