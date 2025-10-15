import sys
import boto3
import json
import time
import jwt
import requests
import pandas as pd
from io import StringIO
import argparse
from datetime import datetime

###GLUE###
# from awsglue.utils import getResolvedOptions
# args = getResolvedOptions(sys.argv, [
#     "INPUT_S3_BUCKET_NAME",
#     "S3_KEY_PATH",
#     "OUTPUT_S3_BUCKET_NAME",
#     "SECRET_NAME",
#     "REGION_NAME"
# ])
# S3_INPUT_BUCKET = args["INPUT_S3_BUCKET_NAME"]
# S3_KEY_PATH = args["S3_KEY_PATH"]
# SECRET_NAME = args["SECRET_NAME"]
# REGION_NAME = args["REGION_NAME"]
# S3_OUTPUT_BUCKET = args['OUTPUT_S3_BUCKET_NAME']

# Cargar configuración
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

def save_table_list_to_file(tables, filename="netsuite_tables_list.txt"):
    """
    Guardar la lista de tablas en un archivo de texto con timestamp
    """
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"# NetSuite Tables List\n")
            f.write(f"# Generated: {timestamp}\n")
            f.write(f"# Total tables: {len(tables)}\n")
            f.write(f"# ================================\n\n")
            
            # Escribir tablas ordenadas alfabéticamente
            for i, table in enumerate(sorted(tables), 1):
                f.write(f"{i:4d}. {table}\n")
        
        print(f"\n✓ Lista de tablas guardada en: {filename}")
        print(f"✓ Total de tablas guardadas: {len(tables)}")
        
    except Exception as e:
        print(f"Error guardando archivo: {e}")

def save_table_details_to_file(table_details, filename="netsuite_table_details.json"):
    """
    Guardar los detalles completos de las tablas en formato JSON
    """
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Ordenar por nombre de tabla
        sorted_details = sorted(table_details, key=lambda x: x.get('name', ''))
        
        # Crear estructura de datos para guardar
        output_data = {
            "metadata": {
                "generated_at": timestamp,
                "total_tables": len(table_details),
                "description": "NetSuite Metadata Catalog - Detailed table information with API links"
            },
            "tables": sorted_details
        }
        
        # Guardar en formato JSON
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Detalles de tablas guardados en: {filename}")
        print(f"✓ Total de tablas con detalles: {len(table_details)}")
        
        # También crear una versión de texto legible
        txt_filename = filename.replace('.json', '.txt')
        with open(txt_filename, 'w', encoding='utf-8') as f:
            f.write(f"# NetSuite Table Details\n")
            f.write(f"# Generated: {timestamp}\n")
            f.write(f"# Total tables: {len(table_details)}\n")
            f.write(f"# ================================\n\n")
            
            for i, table_info in enumerate(sorted_details, 1):
                table_name = table_info.get('name', 'Unknown')
                links = table_info.get('links', [])
                
                f.write(f"{i:4d}. {table_name}\n")
                
                if links:
                    f.write(f"      API Links disponibles:\n")
                    for link in links:
                        rel = link.get('rel', 'N/A')
                        media_type = link.get('mediaType', 'N/A')
                        f.write(f"        - {rel}: {media_type}\n")
                f.write(f"\n")
        
        print(f"✓ Versión legible guardada en: {txt_filename}")
        
    except Exception as e:
        print(f"Error guardando detalles de tablas: {e}")


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
            print(f"Tipo de transacción: {type_code} -> {type_name}")
        print(f"Total de tipos de transacciones obtenidos: {len(transaction_mapping)}")
    else:
        print(f"Error obteniendo tipos de transacciones: {res.status_code} {res.text}")
        
    return transaction_mapping

def get_available_tables_via_metadata_catalog(account_id, access_token, save_to_file=True):
    """
    Obtener todas las tablas disponibles usando el Metadata Catalog API.
    Esta función utiliza la API de metadata catalog de NetSuite para obtener 
    información sobre todas las tablas/records disponibles en la instancia.
    """
    print("Obteniendo todas las tablas disponibles...")
    
    # URL del metadata catalog
    metadata_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/record/v1/metadata-catalog"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    
    try:
        response = requests.get(metadata_url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            tables = []
            table_details = []
            
            for item in data.get('items', []):
                table_name = item.get('name')
                if table_name:
                    tables.append(table_name)
                    
                    # Extraer información adicional disponible en los links
                    links = item.get('links', [])
                    table_info = {
                        'name': table_name,
                        'links': []
                    }
                    
                    for link in links:
                        rel = link.get('rel')
                        href = link.get('href')
                        media_type = link.get('mediaType')
                        table_info['links'].append({
                            'rel': rel,
                            'href': href, 
                            'mediaType': media_type
                        })
                    
                    table_details.append(table_info)
                    print(f"Tabla disponible: {table_name}")
            
            print(f"\nTotal de tablas disponibles: {len(tables)}")
            print("="*50)
            
            # Guardar lista de tablas en archivo si se solicita
            if save_to_file:
                save_table_list_to_file(tables,filename="netsuite_tables_list.txt")
                save_table_details_to_file(table_details,filename="netsuite_table_details.txt")
            return sorted(tables), table_details
            
        else:
            print(f"Error obteniendo metadata catalog: {response.status_code} {response.text}")
            return [], []
            
    except Exception as e:
        print(f"Error al consultar metadata catalog: {e}")
        return [], []

def get_detailed_table_schema(account_id, access_token, table_name, format_type="json"):
    """
    Obtener esquema detallado de una tabla específica desde el metadata catalog.
    
    Parámetros:
    - table_name: nombre de la tabla
    - format_type: "json" para JSON Schema o "openapi" para OpenAPI 3.0
    
    El metadata catalog proporciona información detallada sobre:
    - Campos y sus tipos de datos
    - Sublistas (sublists)
    - Subrecords
    - Operaciones disponibles (GET, POST, PUT, DELETE)
    - Validaciones y restricciones
    - Enlaces HATEOAS
    """
    print(f"\nObteniendo esquema detallado para tabla: {table_name}")
    
    # Construir URL para obtener esquema específico
    base_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/record/v1/metadata-catalog/{table_name}"
    
    # Configurar headers según el formato solicitado
    if format_type.lower() == "openapi":
        accept_header = "application/swagger+json"
    else:
        accept_header = "application/schema+json"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": accept_header
    }
    
    try:
        response = requests.get(base_url, headers=headers)
        
        if response.status_code == 200:
            schema_data = response.json()
            
            print(f"✓ Esquema obtenido para {table_name}")
            
            # Extraer información relevante del esquema
            schema_info = {
                'table_name': table_name,
                'format': format_type,
                'schema': schema_data
            }
            
            # Si es JSON Schema, extraer información de campos
            if format_type == "json" and 'properties' in schema_data:
                properties = schema_data.get('properties', {})
                print(f"  - Campos disponibles: {len(properties)}")
                
                # Mostrar algunos campos de ejemplo
                field_names = list(properties.keys())[:5]  # Primeros 5 campos
                if field_names:
                    print(f"  - Campos ejemplo: {', '.join(field_names)}")
            
            # Si es OpenAPI, extraer información de operaciones
            elif format_type == "openapi" and 'paths' in schema_data:
                paths = schema_data.get('paths', {})
                print(f"  - Endpoints disponibles: {len(paths)}")
                
                # Mostrar operaciones disponibles
                operations = []
                for path, methods in paths.items():
                    operations.extend(methods.keys())
                
                unique_operations = list(set(operations))
                print(f"  - Operaciones: {', '.join(unique_operations)}")
            
            return schema_info
            
        else:
            print(f"Error obteniendo esquema para {table_name}: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"Error al obtener esquema para {table_name}: {e}")
        return None
    
def main():
    #Parsear argumentos
    args = parse_arguments()
    
    # Definir tablas a extraer
    #entity_tables = ["customer", "customersubsidiaryrelationship", "vendor","vendorsubsidiaryrelationship"]
    #transactions_tables = ["CustPymt", "CustInvc", "Opprtnty", "PurchOrd", "SalesOrd"]
    #tables = entity_tables + transactions_tables
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
        print("\n1. TIPOS DE TRANSACCIONES")
        print("-"*30)
        get_available_transaction_types(creds["ACCOUNT_ID"], access_token)

        print("\n2. TODAS LAS TABLAS DISPONIBLES")
        print("-"*35)
        get_available_tables_via_metadata_catalog(
            creds["ACCOUNT_ID"], 
            access_token, 
            save_to_file=True
        )
        # example_tables = ['customer', 'item', 'transaction']
        # for table in example_tables[:2]:  # Solo 2 ejemplos para no saturar
        #     get_detailed_table_schema(
        #             creds["ACCOUNT_ID"], 
        #             access_token, 
        #             table, 
        #             format_type="json"
        #         )

    except Exception as e:
        print(f"Error en la ejecución: {e}")
        return 1
    return 0

if __name__ == "__main__":
    exit(main())
