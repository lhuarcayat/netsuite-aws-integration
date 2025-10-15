import os
import boto3
import json
import time
import jwt
import requests
import argparse
import sys
from datetime import datetime

with open('config/parameters.json', 'r') as f:
    config = json.load(f)

# Construir argumentos
sys.argv.extend([
    '--input-s3-bucket', config['input_s3_bucket'],
    '--s3-key-path', config['s3_key_path'],
    '--secret-name', config['secret_name'],
    '--region-name', config['region_name']
])

def parse_arguments():
    """Parsear argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(description='NetSuite Schema Extractor using Metadata Catalog')
    parser.add_argument('--input-s3-bucket', required=True, help='S3 bucket name for input')
    parser.add_argument('--s3-key-path', required=True, help='S3 key path for private key')
    parser.add_argument('--secret-name', required=True, help='AWS Secrets Manager secret name')
    parser.add_argument('--region-name', default='us-east-1', help='AWS region name')
    parser.add_argument('--output-file', default='netsuite_schema.json', help='Output JSON file name')
    parser.add_argument('--tables', nargs='*', help='Specific tables to extract schema for')
    
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

def get_all_available_tables(account_id, access_token):
    """Obtener lista de todas las tablas disponibles desde el metadata catalog"""
    metadata_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/record/v1/metadata-catalog"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    
    print("Obteniendo lista de tablas desde metadata catalog...")
    res = requests.get(metadata_url, headers=headers)
    
    if res.status_code == 200:
        data = res.json()
        available_tables = []
        
        for item in data.get('items', []):
            table_name = item.get('name')
            if table_name:
                available_tables.append(table_name)
        
        print(f"Se encontraron {len(available_tables)} tablas disponibles")
        return available_tables
    else:
        print(f"Error obteniendo tablas del metadata catalog: {res.status_code} {res.text}")
        return []

def get_table_schema_from_metadata(table_name, account_id, access_token):
    """Obtener el esquema de una tabla específica usando el metadata catalog"""
    metadata_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/record/v1/metadata-catalog/{table_name}"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/schema+json"  # Solicitar formato JSON Schema
    }
    
    print(f"Obteniendo metadata para tabla: {table_name}")
    res = requests.get(metadata_url, headers=headers)
    
    schema_info = {
        "table_name": table_name,
        "columns": {},
        "total_columns": 0,
        "metadata_source": "REST metadata-catalog"
    }
    
    if res.status_code == 200:
        try:
            metadata = res.json()
            
            # Extraer información de columnas del JSON Schema
            properties = metadata.get('properties', {})
            
            for field_name, field_info in properties.items():
                column_info = {
                    "data_type": field_info.get('type', 'unknown'),
                    "format": field_info.get('format'),
                    "description": field_info.get('description'),
                    "required": field_name in metadata.get('required', []),
                    "nullable": field_info.get('nullable', True),
                    "maxLength": field_info.get('maxLength'),
                    "enum_values": field_info.get('enum')
                }
                
                # Limpiar campos nulos del diccionario
                column_info = {k: v for k, v in column_info.items() if v is not None}
                schema_info["columns"][field_name] = column_info
            
            schema_info["total_columns"] = len(schema_info["columns"])
            print(f"✓ Esquema obtenido para {table_name}: {schema_info['total_columns']} columnas")
            
        except json.JSONDecodeError as e:
            print(f"✗ Error decodificando JSON para {table_name}: {e}")
            schema_info["error"] = f"JSON decode error: {str(e)}"
            
    elif res.status_code == 404:
        print(f"✗ Tabla {table_name} no encontrada en metadata catalog")
        schema_info["error"] = "Table not found in metadata catalog"
        
    elif res.status_code == 401 or "INVALID_LOGIN" in res.text:
        print("Token expirado, se necesita renovar...")
        raise Exception("Token expired - need to regenerate")
        
    else:
        print(f"✗ Error obteniendo metadata para {table_name}: {res.status_code}")
        schema_info["error"] = f"HTTP {res.status_code}: {res.text[:200]}"
    
    return schema_info

def get_custom_tables_suiteql(account_id, access_token):
    """Obtener información adicional de custom tables usando SuiteQL"""
    record_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"
    
    print("Obteniendo custom record types...")
    sql = {"q": "SELECT Name, ScriptID, InternalID, Description FROM CustomRecordType ORDER BY Name"}
    
    res = requests.post(record_url, headers={
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Prefer": "transient"
    }, json=sql)
    
    custom_tables = []
    if res.status_code == 200:
        data = res.json()
        for item in data.get('items', []):
            custom_tables.append({
                "name": item.get('name'),
                "script_id": item.get('scriptid'),
                "internal_id": item.get('internalid'),
                "description": item.get('description')
            })
        print(f"Se encontraron {len(custom_tables)} custom record types")
    else:
        print(f"Error obteniendo custom record types: {res.status_code}")
    
    return custom_tables

def save_schema_to_json(schemas, output_file):
    """Guardar esquemas localmente en archivo JSON"""
    try:
        schema_data = {
            "extraction_date": datetime.now().isoformat(),
            "metadata_source": "NetSuite REST metadata-catalog API",
            "total_tables_processed": len(schemas),
            "successful_extractions": len([s for s in schemas if "error" not in s]),
            "failed_extractions": len([s for s in schemas if "error" in s]),
            #"custom_record_types": custom_tables,
            "tables": schemas
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(schema_data, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Esquema guardado exitosamente en: {output_file}")
        return True
        
    except Exception as e:
        print(f"✗ Error guardando esquema: {e}")
        return False

def main():
    # Parsear argumentos
    args = parse_arguments()
    
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
        
        # Determinar qué tablas procesar
        if args.tables:
            tables_to_process = args.tables
            print(f"Usando tablas especificadas: {tables_to_process}")
        else:
            # Obtener todas las tablas disponibles
            # tables_to_process = get_all_available_tables(creds["ACCOUNT_ID"], access_token)
            # if not tables_to_process:
            #     print("No se pudieron obtener tablas. Usando lista predeterminada.")
            #     tables_to_process = ["item", "customer", "vendor", "account", "transaction"]
            tables_to_process = ["bom", "customer", "customersubsidiaryrelationship", "inventorybalance", "item", "vendor", "vendorsubsidiaryrelationship", "customerpayment", "invoice", "opportunity", "purchaseorder", "salesorder"]
        
        print(f"\n=== PROCESANDO {len(tables_to_process)} TABLAS ===")
        
        start_job = time.time()
        schemas = []
        successful_tables = 0
        
        # Obtener custom tables información adicional
        #custom_tables = get_custom_tables_suiteql(creds["ACCOUNT_ID"], access_token)
        
        # Procesar cada tabla usando metadata catalog
        for i, table in enumerate(tables_to_process, 1):
            try:
                print(f"\n[{i}/{len(tables_to_process)}] Procesando: {table}")
                schema_info = get_table_schema_from_metadata(
                    table, 
                    creds["ACCOUNT_ID"], 
                    access_token
                )
                schemas.append(schema_info)
                
                if "error" not in schema_info:
                    successful_tables += 1
                
            except Exception as e:
                print(f"✗ Error procesando tabla {table}: {e}")
                schemas.append({
                    "table_name": table,
                    "error": str(e),
                    "columns": {},
                    "total_columns": 0
                })
                continue
        
        # Guardar esquemas en JSON local
        if save_schema_to_json(schemas, args.output_file):
            print(f"\n✓ Esquema guardado exitosamente en: {args.output_file}")
        
        end_job = time.time()
        print(f"\n=== EXTRACCIÓN COMPLETADA ===")
        print(f"Tablas procesadas exitosamente: {successful_tables}/{len(tables_to_process)}")
        #print(f"Custom record types encontrados: {len(custom_tables)}")
        print(f"Tiempo total: {end_job - start_job:.2f} segundos")
        
    except Exception as e:
        print(f"Error en la ejecución: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())