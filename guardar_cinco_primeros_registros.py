import os
import boto3
import json
import time
import jwt
import requests
import pandas as pd
from io import StringIO
import argparse
import sys

with open('config/parameters.json', 'r') as f:
    config = json.load(f)

# Construir argumentos
sys.argv.extend([
    '--input-s3-bucket', config['input_s3_bucket'],
    '--s3-key-path', config['s3_key_path'],
    '--output-s3-bucket', config['output_s3_bucket'],
    '--secret-name', config['secret_name'],
    '--region-name', config['region_name']
])

def parse_arguments():
    """Parsear argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(description='NetSuite Data Extractor')
    parser.add_argument('--input-s3-bucket', required=True, help='S3 bucket name for input')
    parser.add_argument('--s3-key-path', required=True, help='S3 key path for private key')
    parser.add_argument('--output-s3-bucket', required=True, help='S3 bucket name for output')
    parser.add_argument('--secret-name', required=True, help='AWS Secrets Manager secret name')
    parser.add_argument('--region-name', default='us-east-1', help='AWS region name')
    
    return parser.parse_args()

def get_secret_credentials(secret_name, region_name):
    """Obtener credenciales desde Secrets Manager"""
    try:
        secrets_client = boto3.client("secretsmanager", region_name=region_name)
        secret_value = secrets_client.get_secret_value(SecretId=secret_name)
        return json.loads(secret_value["SecretString"])
    except Exception as e:
        print(f"Error obteniendo credenciales: {e}")
        raise

def get_private_key_from_s3(bucket_name, key_name):
    """Descargar clave privada desde S3"""
    try:
        s3_client = boto3.client("s3")
        pem_obj = s3_client.get_object(Bucket=bucket_name, Key=key_name)
        return pem_obj["Body"].read().decode("utf-8")
    except Exception as e:
        print(f"Error descargando clave privada: {e}")
        raise

def get_access_token(account_id, client_id, certificate_id, private_key, scope="rest_webservices"):
    """Crear JWT y obtener access token"""
    now = int(time.time())
    payload = {
        "iss": client_id,
        "scope": scope,
        "aud": f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/auth/oauth2/v1/token",
        "iat": now,
        "exp": now + 300
    }
    
    headers = {
        "alg": "PS256",
        "typ": "JWT",
        "kid": certificate_id
    }
    
    client_assertion = jwt.encode(
        payload,
        private_key,
        algorithm="PS256",
        headers=headers
    )
    
    token_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/auth/oauth2/v1/token"
    
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
        print(f"Error al obtener token: {resp.status_code} {resp.text}")
        raise Exception(f"Failed to get access token: {resp.text}")
    
def get_available_transaction_types(account_id, access_token):
    """Obtener todos los tipos de transacciones disponibles en la instancia"""
    record_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"
    
    sql = {"q": "SELECT DISTINCT BUILTIN.DF(Type) AS Name, Type FROM Transaction ORDER BY BUILTIN.DF(Type)"}
    
    res = requests.post(record_url, headers={
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Prefer": "transient"
    }, json=sql, params={"limit": 1000})
    
    transaction_mapping = {}
        
    if res.status_code == 200:
        data = res.json()
        for item in data.get('items', []):
            type_code = item['type']
            type_name = item['name']
            transaction_mapping[type_code] = type_name
            # Imprimir cada tipo de transacción al momento de obtenerlo
            print(f"Tipo de transacción: {type_code} -> {type_name}")
        print(f"Total de tipos de transacciones obtenidos: {len(transaction_mapping)}")
    else:
        print(f"Error obteniendo tipos de transacciones: {res.status_code} {res.text}")
        
    return transaction_mapping

def determine_tables_to_process(entity_tables, transactions_tables, account_id, access_token):
    """Versión alternativa que maneja diccionario"""
    final_entity_tables = []
    final_transaction_tables = []
    
    # Procesar entity tables
    if entity_tables:
        final_entity_tables = entity_tables
        print(f"Usando entity tables especificadas: {final_entity_tables}")
    else:
        print("Lista de entity tables vacía - no se procesarán entity tables")
    
    # Procesar transaction tables
    if transactions_tables:
        final_transaction_tables = transactions_tables
        print(f"Usando transaction tables especificadas: {final_transaction_tables}")
    else:
        print("Lista de transaction tables vacía - auto-descubriendo...")
        final_transaction_tables = []
    
    # Combinar todas las tablas
    all_tables = final_entity_tables + final_transaction_tables
    
    print(f"\n=== RESUMEN DE TABLAS A PROCESAR ===")
    print(f"Entity tables: {len(final_entity_tables)} tablas")
    print(f"Transaction tables: {len(final_transaction_tables)} tablas") 
    print(f"Total: {len(all_tables)} tablas")
    print("=" * 50)
    
    return all_tables, final_entity_tables

def extract_table_data(table, account_id, access_token, entity_tables, max_records=5):
    """Extraer datos de una tabla específica - limitado a max_records"""
    record_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"
    df_table = pd.DataFrame()
    limit = max_records  # Cambiar límite para obtener solo los primeros registros
    offset = 0
    
    if table in entity_tables:
        sql = {"q": f"SELECT * FROM {table}"}
    else:
        transaction_fields = [
             "id","tranId","tranDate","entity","status","memo",
             "createdDate","lastModifiedDate","source","currency","location"
         ]
        select_fields = ",".join(transaction_fields)
        sql = {"q": f"SELECT {select_fields} FROM transaction WHERE type='{table}'"}
    
    print(f"SQL Query: {sql}")
    start_table = time.time()
    
    # Solo una iteración para obtener los primeros registros
    print(f"Consultando tabla {table} | limit={limit}&offset={offset}")
    
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
            print(f"Datos obtenidos de la tabla {table}: {cant_records} registros.")
            df_table = pd.DataFrame(records)
            
    elif res.status_code == 401 or "INVALID_LOGIN" in res.text:
        print("Token expirado, se necesita renovar...")
        raise Exception("Token expired - need to regenerate")
    else:
        print(f"Error SuiteQL: {res.status_code} {res.text}")

    end_table = time.time()
    print(f"Tiempo de procesamiento de la tabla {table}: {end_table - start_table:.2f} seg")
    
    return df_table

def save_to_local_csv(df, table, output_folder="data_prueba"):
    """Guardar DataFrame en archivo CSV local con formato de comillas y sin saltos de línea"""
    if len(df) > 0:
        try:
            # Crear carpeta si no existe
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)
                print(f"Carpeta '{output_folder}' creada")
            
            # Crear una copia del DataFrame para no modificar el original
            df_clean = df.copy()
            
            # Reemplazar saltos de línea por espacios en todas las columnas de texto
            for col in df_clean.columns:
                if df_clean[col].dtype == 'object':  # Solo columnas de texto
                    df_clean[col] = df_clean[col].astype(str).str.replace('\n', ' ', regex=False)
                    df_clean[col] = df_clean[col].str.replace('\r', ' ', regex=False)
                    df_clean[col] = df_clean[col].str.replace('  ', ' ', regex=True)  # Eliminar dobles espacios
                    df_clean[col] = df_clean[col].str.strip()  # Eliminar espacios al inicio y final
            
            # Definir ruta del archivo
            file_path = os.path.join(output_folder, f"{table}.csv")
            
            # Guardar CSV con todos los campos entre comillas
            # quoting=1 es equivalente a csv.QUOTE_ALL
            # lineterminator='\n' asegura saltos de línea consistentes
            df_clean.to_csv(file_path, index=False, encoding='utf-8', quoting=1, lineterminator='\n')
            print(f"✓ Archivo guardado: {file_path} ({len(df_clean)} registros)")
            return True
            
        except Exception as e:
            print(f"✗ Error guardando CSV para {table}: {e}")
            return False
    else:
        print(f"✗ No hay datos para guardar en {table}")
        return False

def main():
    # Parsear argumentos
    args = parse_arguments()
    
    # Definir tablas a extraer
    entity_tables = ["transactionaccountingline","transactionshippingaddress","transactionline"]
    transactions_tables = []
    
    try:
        # Obtener credenciales
        print("Obteniendo credenciales...")
        creds = get_secret_credentials(args.secret_name, args.region_name)
        
        # Obtener clave privada
        print("Descargando clave privada...")
        private_key = get_private_key_from_s3(args.input_s3_bucket, args.s3_key_path)
        
        # Obtener access token
        print("Obteniendo access token...")
        access_token = get_access_token(
            creds["ACCOUNT_ID"],
            creds["CLIENT_ID"], 
            creds["CERTIFICATE_ID"],
            private_key
        )
        
        tables_to_process, final_entity_tables = determine_tables_to_process(
            entity_tables, 
            transactions_tables, 
            creds["ACCOUNT_ID"], 
            access_token
        )
        
        if not tables_to_process:
            print("No hay tablas para procesar. Verificar configuración.")
            return 1
            
        start_job = time.time()
        successful_tables = 0
        
        # Procesar cada tabla
        for table in tables_to_process:
            try:
                print(f"\n--- Procesando tabla: {table} ---")
                # Extraer solo los primeros 5 registros
                df_table = extract_table_data(
                    table, 
                    creds["ACCOUNT_ID"], 
                    access_token, 
                    final_entity_tables,
                    max_records=20
                )
                
                cant = len(df_table.index)
                print(f"Cantidad de registros de la tabla {table}: {cant}")
                
                # Guardar en CSV local
                if save_to_local_csv(df_table, table, output_folder="data_prueba"):
                    successful_tables += 1
                
                del df_table
                
            except Exception as e:
                print(f"Error procesando tabla {table}: {e}")
                continue
        
        end_job = time.time()
        print(f"\n=== Migración completada ===")
        print(f"Tablas procesadas exitosamente: {successful_tables}/{len(tables_to_process)}")
        print(f"Tiempo total: {end_job - start_job:.2f} segundos")
        print(f"Archivos guardados en la carpeta: data_prueba/")
        
    except Exception as e:
        print(f"Error en la ejecución: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())