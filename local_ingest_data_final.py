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

def debug_available_tables(account_id, access_token):
    """Función para verificar qué tablas están disponibles"""
    record_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"
    
    # Consultar el catálogo de metadatos
    catalog_query = {
        "q": "SELECT tablename FROM systables WHERE tablename LIKE '%inventory%' OR tablename LIKE '%bom%' OR tablename LIKE '%item%'"
    }
    
    print("\n=== VERIFICANDO TABLAS DISPONIBLES ===")
    try:
        res = requests.post(record_url, headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Prefer": "transient"
        }, json=catalog_query)
        
        if res.status_code == 200:
            data = res.json()
            print("Tablas encontradas que contienen 'inventory', 'bom' o 'item':")
            for item in data.get('items', []):
                print(f"  - {item['tablename']}")
        else:
            print(f"Error consultando catálogo: {res.status_code} - {res.text}")
            
    except Exception as e:
        print(f"Error en debug: {e}")

def test_inventory_balance_queries(account_id, access_token):
    """Función para probar diferentes consultas de balance de inventario"""
    record_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"
    
    print("\n=== PROBANDO CONSULTAS DE INVENTARIO ===")
    
    # Diferentes opciones para probar
    test_queries = [
        {
            "name": "InventoryBalance_direct", 
            "q": "SELECT * FROM InventoryBalance LIMIT 5"
        },
        {
            "name": "InventoryStatusInventoryBalance", 
            "q": "SELECT * FROM InventoryStatusInventoryBalance LIMIT 5"
        },
        {
            "name": "ItemQuantity", 
            "q": "SELECT * FROM ItemQuantity LIMIT 5"
        },
        {
            "name": "InventoryBalance_with_fields", 
            "q": """SELECT 
                item, BUILTIN.DF(item) as item_name,
                location, BUILTIN.DF(location) as location_name,
                quantityonhand, quantitycommitted, quantityavailable,
                lastmodifieddate
                FROM InventoryBalance 
                WHERE quantityonhand > 0 LIMIT 5"""
        },
        {
            "name": "TransactionLine_inventory_impact", 
            "q": """SELECT 
                TransactionLine.item as item_id,
                BUILTIN.DF(TransactionLine.item) as item_name,
                SUM(TransactionLine.quantity) as quantity_impact,
                COUNT(*) as transaction_count
                FROM TransactionLine
                INNER JOIN Transaction ON Transaction.id = TransactionLine.transaction
                WHERE TransactionLine.isinventoryaffecting = 'T'
                AND Transaction.posting = 'T'
                AND Transaction.voided = 'F'
                GROUP BY TransactionLine.item
                LIMIT 5"""
        }
    ]
    
    for test_query in test_queries:
        print(f"\n--- Probando: {test_query['name']} ---")
        try:
            res = requests.post(record_url, headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "Prefer": "transient"
            }, json={"q": test_query['q']})
            
            if res.status_code == 200:
                data = res.json()
                count = data.get('count', 0)
                print(f"✅ ÉXITO: {count} registros encontrados")
                if data.get('items') and count > 0:
                    print(f"   Campos disponibles: {list(data['items'][0].keys())}")
                    # Mostrar primer registro como ejemplo
                    print(f"   Ejemplo: {data['items'][0]}")
            else:
                print(f"❌ ERROR: {res.status_code} - {res.text}")
                
        except Exception as e:
            print(f"❌ EXCEPCIÓN: {e}")

def get_table_query(table, entity_tables):
    """Generar consulta SQL específica para cada tabla"""
    
    table_lower = table.lower()
    
    if table_lower == 'item':
        # Para la tabla Item, usar consulta directa con filtros importantes
        return {
            "q": """SELECT 
                id, itemid, displayname, itemtype, subtype, 
                islotitem, isserialitem, isinactive, cost, averagecost,
                createddate, lastmodifieddate, quantityonhand, quantitycommitted,
                quantityavailable, reorderpoint, preferredlocation
                FROM item 
                WHERE isinactive = 'F'"""
        }
    
    elif table_lower == 'bom':
        # Para BOM, obtener los componentes de las revisiones activas
        return {
            "q": """SELECT 
                BomRevisionComponentMember.id as component_id,
                BUILTIN.DF(BomRevisionComponentMember.BomRevision) as bom_revision,
                BUILTIN.DF(BomRevisionComponentMember.item) as component_item,
                BomRevisionComponentMember.bomquantity as quantity,
                BUILTIN.DF(BomRevisionComponentMember.unit) as unit,
                BUILTIN.DF(BomAssembly.assembly) as parent_assembly,
                Bom.name as bom_name,
                BomRevision.name as revision_name,
                BomRevision.effectivestartdate,
                BomRevision.effectiveenddate
                FROM BomRevisionComponentMember
                INNER JOIN BomRevision ON BomRevision.id = BomRevisionComponentMember.BomRevision
                INNER JOIN Bom ON Bom.id = BomRevision.billofmaterials
                INNER JOIN BomAssembly ON BomAssembly.billofmaterials = Bom.id
                WHERE (
                    (BomRevision.effectivestartdate <= SYSDATE AND BomRevision.effectiveenddate IS NULL) OR
                    (BomRevision.effectivestartdate <= SYSDATE AND BomRevision.effectiveenddate >= SYSDATE)
                )"""
        }
    
    elif table_lower in ['inventorybalance', 'inventory_balance']:
        # Para balance de inventario - primera opción: tabla directa
        return {
            "q": """SELECT 
                item as item_id,
                BUILTIN.DF(item) as item_name,
                location as location_id,
                BUILTIN.DF(location) as location_name,
                quantityonhand,
                quantitycommitted,
                quantityavailable,
                quantityonorder,
                lastmodifieddate
                FROM InventoryBalance
                WHERE quantityonhand <> 0 OR quantitycommitted <> 0 OR quantityavailable <> 0"""
        }
    
    elif table_lower == 'inventorybalance_alternative':
        # Alternativa usando TransactionLine para calcular balances
        return {
            "q": """SELECT 
                TransactionLine.item as item_id,
                BUILTIN.DF(TransactionLine.item) as item_name,
                COALESCE(TransactionLine.location, 0) as location_id,
                BUILTIN.DF(COALESCE(TransactionLine.location, 0)) as location_name,
                SUM(TransactionLine.quantity) as cumulative_quantity,
                COUNT(*) as transaction_count,
                MAX(Transaction.trandate) as last_transaction_date
                FROM TransactionLine
                INNER JOIN Transaction ON Transaction.id = TransactionLine.transaction
                WHERE TransactionLine.isinventoryaffecting = 'T'
                AND Transaction.posting = 'T'
                AND Transaction.voided = 'F'
                GROUP BY TransactionLine.item, TransactionLine.location
                HAVING SUM(TransactionLine.quantity) <> 0
                ORDER BY TransactionLine.item"""
        }
    
    # Para otras tablas de entidades (mantener lógica original)
    elif table in entity_tables:
        return {"q": f"SELECT * FROM {table}"}
    
    # Para tablas de transacciones (mantener lógica original)
    else:
        transaction_fields = [
            "id","tranId","tranDate","entity","status","memo",
            "createdDate","lastModifiedDate","source","currency","location"
        ]
        select_fields = ",".join(transaction_fields)
        return {"q": f"SELECT {select_fields} FROM transaction WHERE type='{table}'"}

def extract_table_data_improved(table, account_id, access_token, entity_tables):
    """Versión mejorada de extract_table_data con consultas específicas"""
    record_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"
    df_table = pd.DataFrame()
    limit = 1000
    offset = 0
    
    # Obtener consulta específica para la tabla
    sql = get_table_query(table, entity_tables)
    
    print(f"\n--- Consultando tabla: {table} ---")
    print(f"SQL Query: {sql['q'][:200]}...")  # Solo mostrar primeros 200 caracteres
    
    start_table = time.time()
    
    # Para InventoryBalance, si falla, intentar con alternativa
    if table.lower() in ['inventorybalance', 'inventory_balance']:
        try:
            # Intentar primera consulta
            test_res = requests.post(record_url, headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "Prefer": "transient"
            }, json=sql, params={"limit": 1})
            
            if test_res.status_code != 200:
                print(f"La tabla InventoryBalance no está disponible. Usando alternativa...")
                sql = get_table_query('inventorybalance_alternative', entity_tables)
                print(f"SQL Alternativo: {sql['q'][:200]}...")
                
        except Exception as e:
            print(f"Error probando InventoryBalance, usando alternativa: {e}")
            sql = get_table_query('inventorybalance_alternative', entity_tables)
    
    while True:
        print(f"Consultando tabla {table} | limit={limit}&offset={offset}")
        
        try:
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
                else:
                    print(f"No se encontraron registros en la tabla {table}.")
                    break
                
                if data.get("hasMore", False):
                    offset += limit
                else:
                    print(f"No hay más páginas en la tabla {table}.")
                    break
                    
            elif res.status_code == 401 or "INVALID_LOGIN" in res.text:
                print("Token expirado, se necesita renovar...")
                raise Exception("Token expired - need to regenerate")
            else:
                print(f"Error SuiteQL: {res.status_code}")
                print(f"Error detallado: {res.text}")
                
                # Si es InventoryBalance y falla, intentar alternativa
                if table.lower() in ['inventorybalance', 'inventory_balance'] and 'inventorybalance_alternative' not in sql['q']:
                    print("Intentando consulta alternativa para InventoryBalance...")
                    sql = get_table_query('inventorybalance_alternative', entity_tables)
                    continue
                else:
                    break

        except Exception as e:
            print(f"Excepción durante consulta: {e}")
            break

    end_table = time.time()
    print(f"Tiempo de procesamiento de la tabla {table}: {end_table - start_table:.2f} seg")
    print(f"Total de registros obtenidos: {len(df_table)}")
    
    return df_table

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

def determine_tables_to_process(entity_tables, transactions_tables, account_id, access_token):
    """Determinar las tablas a procesar"""
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
        print("Lista de transaction tables vacía")
        final_transaction_tables = []
    
    # Combinar todas las tablas
    all_tables = final_entity_tables + final_transaction_tables
    
    print(f"\n=== RESUMEN DE TABLAS A PROCESAR ===")
    print(f"Entity tables: {len(final_entity_tables)} tablas")
    print(f"Transaction tables: {len(final_transaction_tables)} tablas") 
    print(f"Total: {len(all_tables)} tablas")
    print("=" * 50)
    
    return all_tables, final_entity_tables

def save_to_s3(df, table, output_bucket):
    """Guardar DataFrame a S3 en formato Parquet"""
    import s3fs
    
    if len(df) > 0:
        try:
            fs = s3fs.S3FileSystem()
            s3_path = f"{output_bucket}/{table}/{table}.parquet"
            print(f"Guardando con s3fs: s3://{s3_path}")
            
            with fs.open(s3_path, 'wb') as f:
                df.to_parquet(f, engine='pyarrow', compression='snappy', index=False)
            
            print(f"✅ Archivo guardado exitosamente: s3://{s3_path}")
            return True
        except Exception as e:
            print(f"❌ Error guardando en S3: {e}")
            return False
    else:
        print(f"⚠️  DataFrame vacío para tabla {table}, no se guardó archivo")
        return False

def main():
    """Función principal actualizada"""
    # Parsear argumentos
    args = parse_arguments()
    
    # CONFIGURACIÓN ACTUALIZADA DE TABLAS
    entity_tables = ["discountitem"]  # Solo Item como entity table
    special_tables = []  # Tablas que requieren consultas especiales
    
    try:
        # Obtener credenciales
        print("=== INICIANDO EXTRACCIÓN DE DATOS NETSUITE ===")
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
        
        # DIAGNÓSTICO: Verificar tablas disponibles
        debug_available_tables(creds["ACCOUNT_ID"], access_token)
        
        # DIAGNÓSTICO: Probar consultas de inventario
        test_inventory_balance_queries(creds["ACCOUNT_ID"], access_token)
        
        # Determinar tablas a procesar
        all_tables = entity_tables + special_tables
        
        if not all_tables:
            print("No hay tablas para procesar. Verificar configuración.")
            return 1
            
        print(f"\n=== PROCESANDO {len(all_tables)} TABLAS ===")
        start_job = time.time()
        successful_tables = 0
        
        # Procesar cada tabla
        for table in all_tables:
            try:
                print(f"\n{'='*60}")
                print(f"PROCESANDO TABLA: {table.upper()}")
                print(f"{'='*60}")
                
                df_table = extract_table_data_improved(table, creds["ACCOUNT_ID"], access_token, entity_tables)
                
                cant = len(df_table.index)
                print(f"📊 Cantidad total de registros de la tabla {table}: {cant}")
                
                if cant > 0:
                    # Mostrar información sobre los campos obtenidos
                    print(f"📋 Campos obtenidos: {list(df_table.columns)}")
                    
                    # Guardar en S3
                    if save_to_s3(df_table, table, args.output_s3_bucket):
                        successful_tables += 1
                        print(f"✅ Tabla {table} procesada exitosamente")
                    else:
                        print(f"❌ Error guardando tabla {table}")
                else:
                    print(f"⚠️  Tabla {table} no contiene datos")
                
                # Liberar memoria
                del df_table
                
            except Exception as e:
                print(f"❌ Error procesando tabla {table}: {e}")
                print(f"Continuando con la siguiente tabla...")
                continue
        
        end_job = time.time()
        
        # Resumen final
        print(f"\n{'='*60}")
        print(f"MIGRACIÓN COMPLETADA")
        print(f"{'='*60}")
        print(f"✅ Tablas procesadas exitosamente: {successful_tables}/{len(all_tables)}")
        print(f"⏱️  Tiempo total: {end_job - start_job:.2f} segundos")
        print(f"📁 Archivos guardados en S3: s3://{args.output_s3_bucket}/")
        
        if successful_tables == len(all_tables):
            print("🎉 ¡Migración completada con éxito!")
            return 0
        else:
            print("⚠️  Migración completada con algunos errores")
            return 1
        
    except Exception as e:
        print(f"💥 Error crítico en la ejecución: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return 1

if __name__ == "__main__":
    exit(main())