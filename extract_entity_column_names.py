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

def get_table_columns(table, account_id, access_token):
    """Obtener los nombres de las columnas de una tabla usando SuiteQL"""
    record_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"
    
    # Consulta simple para obtener un registro y extraer las columnas
    sql = {"q": f"SELECT * FROM {table}"}
    
    try:
        print(f"Obteniendo columnas de la tabla: {table}")
        
        res = requests.post(record_url, headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Prefer": "transient"
        }, json=sql, params={"limit": 1})
        
        if res.status_code == 200:
            data = res.json()
            items = data.get("items", [])
            
            if items and len(items) > 0:
                # Obtener las columnas del primer registro
                columns = sorted(list(items[0].keys()))
                print(f"  ✓ Tabla {table}: {len(columns)} columnas encontradas")
                return columns
            else:
                print(f"  ⚠ Tabla {table}: No hay registros, intentando con columnas de links...")
                # Si no hay items, intentar obtener columnas de los links si están disponibles
                links = data.get("links", [])
                if links:
                    # Algunas APIs devuelven información de schema en los links
                    return []
                return []
        else:
            print(f"  ✗ Error obteniendo columnas de {table}: {res.status_code} {res.text}")
            return []
            
    except Exception as e:
        print(f"  ✗ Excepción obteniendo columnas de {table}: {e}")
        return []

def extract_all_columns_schema(entity_tables, account_id, access_token, output_file="table_columns_schema.json"):
    """Extraer el schema de columnas para todas las tablas y guardarlo en JSON"""
    print("\n=== EXTRAYENDO SCHEMA DE COLUMNAS ===")
    
    schema = {}
    
    for table in entity_tables:
        columns = get_table_columns(table, account_id, access_token)
        if columns:
            schema[table] = columns
        else:
            schema[table] = []
        
        # Pequeña pausa para no saturar la API
        time.sleep(0.5)
    
    # Guardar en archivo JSON local
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(schema, f, indent=2, ensure_ascii=False)
        print(f"\n✓ Schema guardado exitosamente en: {output_file}")
        print(f"  Total de tablas: {len(schema)}")
        print(f"  Total de columnas: {sum(len(cols) for cols in schema.values())}")
        return schema
    except Exception as e:
        print(f"\n✗ Error guardando schema: {e}")
        return schema

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

def extract_table_data(table, account_id, access_token, entity_tables):
    """Extraer datos de una tabla específica"""
    record_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"
    df_table = pd.DataFrame()
    limit = 1000
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
    
    while True:
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
                
                df_page = pd.DataFrame(records)
                df_table = pd.concat([df_table, df_page], ignore_index=True)
            
            if data.get("hasMore", False):
                offset += limit
            else:
                print(f"No hay más páginas en la tabla {table}.")
                break
                
        elif res.status_code == 401 or "INVALID_LOGIN" in res.text:
            print("Token expirado, se necesita renovar...")
            raise Exception("Token expired - need to regenerate")
        else:
            print(f"Error SuiteQL: {res.status_code} {res.text}")
            break

    end_table = time.time()
    print(f"Tiempo de procesamiento de la tabla {table}: {end_table - start_table:.2f} seg")
    
    return df_table

def save_to_s3(df, table, output_bucket):
    """Alternativa usando s3fs en lugar de pandas+pyarrow directo"""
    import s3fs
    
    if len(df) > 0:
        try:
            fs = s3fs.S3FileSystem()
            s3_path = f"{output_bucket}/{table}/{table}.parquet"
            print(f"Guardando con s3fs: s3://{s3_path}")
            
            with fs.open(s3_path, 'wb') as f:
                df.to_parquet(f, engine='pyarrow', compression='snappy', index=False)
            return True
        except Exception as e:
            print(f"Error con s3fs: {e}")
            return False
    return False

def main():
    # Parsear argumentos
    args = parse_arguments()
    
    # Definir tablas a extraer - MODIFICA ESTA LISTA SEGÚN TUS NECESIDADES
    entity_tables = ["transactionaccountingline", "transactionshippingaddress", "transactionline"]
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
        
        # NUEVO: Extraer el schema de columnas primero
        schema = extract_all_columns_schema(
            entity_tables, 
            creds["ACCOUNT_ID"], 
            access_token,
            output_file="table_columns_schema.json"
        )
        
        print("\n" + "="*50)
        print("SCHEMA EXTRAÍDO:")
        print("="*50)
        for table, columns in schema.items():
            print(f"\n{table}: {len(columns)} columnas")
            if columns:
                print(f"  Columnas: {', '.join(columns[:5])}{'...' if len(columns) > 5 else ''}")
        
        # Continuar con el resto del proceso si se desea...
        # (El código original de extracción de datos puede seguir aquí)
        
        print("\n✓ Proceso completado exitosamente")
        print(f"✓ Archivo JSON generado: table_columns_schema.json")
        
    except Exception as e:
        print(f"Error en la ejecución: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())