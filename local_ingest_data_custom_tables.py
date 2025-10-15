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

sys.argv.extend([
    '--input-s3-bucket', config['input_s3_bucket'],
    '--s3-key-path', config['s3_key_path'],
    '--output-s3-bucket', config['output_s3_bucket'],
    '--secret-name', config['secret_name'],
    '--region-name', config['region_name']
])

def parse_arguments():
    """Parsear argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(description='NetSuite Item Table Finder')
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
        print("✅ Token obtenido con éxito")
        return access_token
    else:
        print(f"❌ Error al obtener token: {resp.status_code} {resp.text}")
        raise Exception(f"Failed to get access token: {resp.text}")

def parse_metadata_catalog_correctly(account_id, access_token):
    """Parsear correctamente el catálogo de metadatos para encontrar 'item'"""
    print(f"\n{'='*70}")
    print("PARSEANDO CATÁLOGO DE METADATOS CORRECTAMENTE")
    print(f"{'='*70}")
    
    metadata_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/record/v1/metadata-catalog/"
    
    try:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        print(f"🔍 Consultando: {metadata_url}")
        res = requests.get(metadata_url, headers=headers)
        
        if res.status_code == 200:
            data = res.json()
            print(f"✅ Catálogo obtenido exitosamente")
            
            # Imprimir estructura para debug
            print(f"📋 Estructura del catálogo: {list(data.keys())}")
            
            # Buscar en diferentes estructuras posibles
            available_records = []
            item_records = []
            
            # Estructura 1: items array
            if 'items' in data:
                print(f"🔍 Buscando en 'items' array...")
                for item in data['items']:
                    if isinstance(item, dict):
                        # Buscar por nombre
                        name = item.get('name', '').lower()
                        available_records.append(name)
                        if 'item' in name:
                            item_records.append(item.get('name'))
                            print(f"   🎯 ENCONTRADO: {item.get('name')}")
            
            # Estructura 2: types array
            if 'types' in data:
                print(f"🔍 Buscando en 'types' array...")
                for item in data['types']:
                    if isinstance(item, dict):
                        name = item.get('name', '').lower()
                        available_records.append(name)
                        if 'item' in name:
                            item_records.append(item.get('name'))
                            print(f"   🎯 ENCONTRADO: {item.get('name')}")
            
            # Estructura 3: directamente en el root
            for key, value in data.items():
                if 'item' in key.lower():
                    item_records.append(key)
                    print(f"   🎯 ENCONTRADO en root: {key}")
            
            # Mostrar todas las tablas disponibles (primeras 20)
            print(f"\n📊 TABLAS DISPONIBLES (primeras 20):")
            for i, record in enumerate(sorted(set(available_records))[:20]):
                print(f"   {i+1:2d}. {record}")
            
            if len(available_records) > 20:
                print(f"   ... y {len(available_records)-20} más")
            
            if item_records:
                print(f"\n✅ TABLAS RELACIONADAS CON 'ITEM' ENCONTRADAS:")
                for record in item_records:
                    print(f"   🎯 {record}")
            else:
                print(f"\n❌ No se encontraron tablas con 'item' en el nombre")
                
                # Buscar variaciones
                variations = ['product', 'inventory', 'catalog', 'asset']
                print(f"\n🔍 Buscando variaciones...")
                for variation in variations:
                    found = [r for r in available_records if variation in r]
                    if found:
                        print(f"   📋 Tablas con '{variation}': {found}")
            
            return item_records, available_records
            
        else:
            print(f"❌ Error accediendo al catálogo: {res.status_code}")
            print(f"   Respuesta completa: {res.text}")
            return [], []
            
    except Exception as e:
        print(f"❌ Excepción: {e}")
        return [], []

def get_specific_table_metadata(account_id, access_token, table_name):
    """Obtener metadatos específicos de una tabla"""
    print(f"\n--- Metadatos específicos para: {table_name} ---")
    
    metadata_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/record/v1/metadata-catalog/{table_name}"
    
    try:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        res = requests.get(metadata_url, headers=headers)
        
        if res.status_code == 200:
            data = res.json()
            print(f"✅ Metadatos obtenidos para {table_name}")
            
            # Imprimir información relevante
            if 'fields' in data:
                fields = [f['name'] for f in data['fields'][:10]]  # Primeros 10 campos
                print(f"   📋 Campos (primeros 10): {', '.join(fields)}")
            
            return data
        else:
            print(f"❌ Error obteniendo metadatos de {table_name}: {res.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Excepción obteniendo metadatos de {table_name}: {e}")
        return None

def test_basic_queries_first(account_id, access_token):
    """Probar consultas muy básicas primero"""
    record_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"
    
    print(f"\n{'='*70}")
    print("PROBANDO CONSULTAS BÁSICAS PRIMERO")
    print(f"{'='*70}")
    
    # Consultas muy simples para verificar conectividad
    basic_queries = [
        {
            "name": "Test Transaction table",
            "q": "SELECT COUNT(*) as count FROM transaction"
        },
        {
            "name": "Test Customer table", 
            "q": "SELECT COUNT(*) as count FROM customer"
        },
        {
            "name": "Sample Transaction",
            "q": "SELECT TOP 3 id, type FROM transaction"
        },
        {
            "name": "Sample Customer",
            "q": "SELECT TOP 3 id, entityid FROM customer"  
        }
    ]
    
    working_queries = []
    
    for query in basic_queries:
        print(f"\n--- {query['name']} ---")
        try:
            res = requests.post(record_url, headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "Prefer": "transient"
            }, json={"q": query['q']})
            
            if res.status_code == 200:
                data = res.json()
                print(f"✅ ÉXITO: {data.get('count', 0)} registros")
                if data.get('items'):
                    print(f"   📝 Resultado: {data['items'][0]}")
                working_queries.append(query['name'])
            else:
                print(f"❌ Error: {res.status_code}")
                print(f"   Detalle: {res.text[:200]}...")
                
        except Exception as e:
            print(f"❌ Excepción: {e}")
    
    return working_queries

def test_item_with_working_pattern(account_id, access_token, table_name="item"):
    """Probar tabla item usando patrones que funcionaron"""
    record_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"
    
    print(f"\n{'='*70}")
    print(f"PROBANDO TABLA '{table_name}' CON PATRONES QUE FUNCIONARON")
    print(f"{'='*70}")
    
    # Consultas progresivamente más específicas
    item_queries = [
        {
            "name": f"Count {table_name}",
            "q": f"SELECT COUNT(*) as count FROM {table_name}"
        },
        {
            "name": f"Top 3 {table_name}",
            "q": f"SELECT TOP 3 * FROM {table_name}"
        },
        {
            "name": f"Item fields basic",
            "q": f"SELECT TOP 3 id FROM {table_name}"
        },
        {
            "name": f"Item with common fields",
            "q": f"SELECT TOP 3 id, itemid FROM {table_name}"
        }
    ]
    
    success_queries = []
    
    for query in item_queries:
        print(f"\n--- {query['name']} ---")
        try:
            res = requests.post(record_url, headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "Prefer": "transient"
            }, json={"q": query['q']})
            
            if res.status_code == 200:
                data = res.json()
                count = data.get('count', len(data.get('items', [])))
                print(f"✅ ¡ÉXITO! {count} registros encontrados")
                
                if data.get('items'):
                    # Mostrar campos disponibles
                    fields = list(data['items'][0].keys())
                    print(f"   📋 Campos disponibles: {', '.join(fields[:10])}{'...' if len(fields) > 10 else ''}")
                    print(f"   📝 Primer registro: {data['items'][0]}")
                
                success_queries.append(query['name'])
                
                # Si esta consulta funciona, probar una más compleja
                if query['name'] == f"Count {table_name}":
                    print(f"   🎉 ¡Tabla {table_name} encontrada! Probando consulta más específica...")
                    
            else:
                print(f"❌ Error: {res.status_code}")
                error_text = res.text
                if "was not found" in error_text:
                    print(f"   💡 Tabla '{table_name}' no encontrada")
                elif "no válida" in error_text.lower():
                    print(f"   💡 Tipo de búsqueda no válida para '{table_name}'")
                else:
                    print(f"   Detalle: {error_text[:200]}...")
                
        except Exception as e:
            print(f"❌ Excepción: {e}")
    
    return success_queries

def try_rest_record_api_for_item(account_id, access_token):
    """Probar el REST Record API directo para items"""
    print(f"\n{'='*70}")
    print("PROBANDO REST RECORD API DIRECTO")
    print(f"{'='*70}")
    
    # Diferentes endpoints del REST API
    rest_endpoints = [
        f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/record/v1/item?limit=5",
        f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/record/v1/inventoryitem?limit=5",
        f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/record/v1/assemblyitem?limit=5",
        f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/record/v1/nonInventoryItem?limit=5"
    ]
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    working_endpoints = []
    
    for endpoint in rest_endpoints:
        endpoint_name = endpoint.split('/')[-1].split('?')[0]
        print(f"\n--- Probando: {endpoint_name} ---")
        
        try:
            res = requests.get(endpoint, headers=headers)
            
            if res.status_code == 200:
                data = res.json()
                count = data.get('count', len(data.get('items', [])))
                print(f"✅ ¡ÉXITO! Endpoint {endpoint_name} funciona: {count} registros")
                
                if data.get('items'):
                    fields = list(data['items'][0].keys())
                    print(f"   📋 Campos: {', '.join(fields[:5])}...")
                    print(f"   📝 Primer registro: {str(data['items'][0])[:100]}...")
                
                working_endpoints.append(endpoint_name)
                
            else:
                print(f"❌ Error: {res.status_code}")
                if res.status_code == 404:
                    print(f"   💡 Record type '{endpoint_name}' no existe")
                else:
                    print(f"   Detalle: {res.text[:200]}...")
                    
        except Exception as e:
            print(f"❌ Excepción: {e}")
    
    return working_endpoints

def main():
    print(f"{'='*80}")
    print("NETSUIE ITEM TABLE FINDER - VERSIÓN MEJORADA")
    print(f"{'='*80}")
    
    args = parse_arguments()
    
    try:
        # Obtener credenciales y token
        print("\n🔐 Obteniendo credenciales...")
        creds = get_secret_credentials(args.secret_name, args.region_name)
        
        print("📄 Descargando clave privada...")
        private_key = get_private_key_from_s3(args.input_s3_bucket, args.s3_key_path)
        
        print("🔑 Obteniendo access token...")
        access_token = get_access_token(
            creds["ACCOUNT_ID"],
            creds["CLIENT_ID"], 
            creds["CERTIFICATE_ID"],
            private_key
        )
        
        account_id = creds["ACCOUNT_ID"]
        print(f"\n🏢 Trabajando con cuenta: {account_id}")
        
        # 1. Parsear correctamente el catálogo de metadatos
        item_tables, all_tables = parse_metadata_catalog_correctly(account_id, access_token)
        
        # 2. Probar consultas básicas primero
        working_basic = test_basic_queries_first(account_id, access_token)
        
        # 3. Si encontramos tablas item, obtener metadatos específicos
        if item_tables:
            for table in item_tables[:3]:  # Solo primeras 3
                get_specific_table_metadata(account_id, access_token, table)
        
        # 4. Probar tabla 'item' específicamente
        success_item_queries = test_item_with_working_pattern(account_id, access_token, "item")
        
        # 5. Probar REST Record API directo
        working_rest_endpoints = try_rest_record_api_for_item(account_id, access_token)
        
        # RESUMEN FINAL
        print(f"\n{'='*80}")
        print("RESUMEN FINAL - TODAS LAS OPCIONES ENCONTRADAS")
        print(f"{'='*80}")
        
        if success_item_queries:
            print(f"🎉 ¡ÉXITO! La tabla 'item' SÍ funciona con SuiteQL!")
            print(f"✅ Consultas exitosas: {', '.join(success_item_queries)}")
            print(f"\n💡 RECOMENDACIÓN: Usa 'item' (minúsculas) en tu script SuiteQL")
            print(f"   Ejemplo: SELECT * FROM item WHERE isinactive = 'F'")
        
        elif working_rest_endpoints:
            print(f"🎯 REST Record API funciona para: {', '.join(working_rest_endpoints)}")
            print(f"\n💡 ALTERNATIVA: Usa REST Record API en lugar de SuiteQL")
        
        elif item_tables:
            print(f"📋 Tablas relacionadas en catálogo: {', '.join(item_tables)}")
            print(f"\n💡 RECOMENDACIÓN: Prueba estas variaciones en SuiteQL")
        
        else:
            print(f"❌ No se encontraron formas de acceder a datos de items")
            print(f"⚠️  POSIBLES CAUSAS:")
            print(f"   - Permisos insuficientes para ver/consultar items")
            print(f"   - La instancia no tiene datos de items configurados")
            print(f"   - Se requieren permisos adicionales del administrador")
        
        print(f"\n📊 ESTADÍSTICAS FINALES:")
        print(f"   - Consultas básicas exitosas: {len(working_basic)}")
        print(f"   - Consultas item exitosas: {len(success_item_queries)}")
        print(f"   - REST endpoints funcionando: {len(working_rest_endpoints)}")
        print(f"   - Tablas encontradas en catálogo: {len(all_tables)}")
        
    except Exception as e:
        print(f"💥 Error crítico: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())