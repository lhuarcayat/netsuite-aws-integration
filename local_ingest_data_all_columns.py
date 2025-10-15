import os
import boto3
import json
import time
import jwt
import requests
import pandas as pd
from io import StringIO
import argparse

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
            print(f"Tipo de transacción: {type_code} -> {type_name}")
        print(f"Total de tipos de transacciones obtenidos: {len(transaction_mapping)}")
    else:
        print(f"Error obteniendo tipos de transacciones: {res.status_code} {res.text}")
        
    return transaction_mapping

def get_all_transaction_columns():
    """Obtener todas las columnas posibles para tablas transaccionales"""
    return [
        "abbrevtype", "accountbasednumber", "actionitem", "actualproductionenddate", 
        "actualproductionstartdate", "actualshipdate", "altsalesrangehigh", "altsalesrangelow",
        "projaltsalesamt", "foreignamountpaid", "foreignamountunpaid", "recurannually",
        "isaomautomated", "approvalstatus", "asofdate", "assignee", "autocalculatelag",
        "visibletocustomer", "bidclosedate", "bidopendate", "billingaccount", "billingaddress",
        "billingstatus", "billofmaterials", "billofmaterialsrevision", "isbudgetapproved",
        "committed", "bulkprocsubmission", "businesseventsprocessinghistory", "buyingreason",
        "buyingtimeframe", "completeddate", "createdby", "currency", "trandate", "closedate",
        "createddate", "lastmodifieddate", "daysopen", "daysoverduesearch", "deferredrevenue",
        "draccount", "transferlocation", "number", "tranid", "duedate", "effectivitybasedon",
        "email", "enddate", "isactualprodenddateenteredmanually", "isactualprodstartdateenteredmanually",
        "entity", "entitytaxregnum", "totalcostestimate", "estgrossprofit", "estgrossprofitpercent",
        "estimatedbudget", "exchangerate", "expectedclosedate", "pickupexpirationdate", "externalid",
        "fax", "isfinchrg", "firmed", "forecasttype", "fxaccount", "fulfillmenttype",
        "userevenuearrangement", "basehandlingtaxamount", "handlingtaxamount", "incoterm",
        "intercostatus", "id", "linkedinventorytransfer", "isbookspecific", "intercoadj",
        "isreversal", "linkedir", "revision", "journaltype", "lastmodifiedby", "leadsource",
        "legacytax", "linkedtrackingnumberlist", "location", "manufacturingrouting", "maximumamount",
        "memo", "memdoc", "message", "minimumamount", "recurmonthly", "nextapprover", "nextbilldate",
        "nexus", "pricingtiers", "onetime", "opportunity", "ordreceived", "ordertype", "outsourced",
        "outsourcingcharge", "intercotransaction", "partner", "foreignpaymentamountunused",
        "foreignpaymentamountused", "paymenthold", "paymentmethod", "paymentoption", "payrollbatch",
        "ordpicked", "pickedupdate", "picktype", "ispickupemailnotificationsent", "pickuphold",
        "otherrefnum", "posting", "postingperiod", "printedpickingticket", "priority", "probability",
        "projectedtotal", "promotioncombinations", "linkedpo", "purchaseorderinstructions",
        "recurquarterly", "rangehigh", "rangelow", "recognizedrevenue", "recordtype", "recurringbill",
        "releaseddate", "revrecenddate", "revrecschedule", "revrecstartdate", "revcommittingstatus",
        "reversaldate", "reversal", "revreconrevcommitment", "salesreadiness", "employee",
        "schedulingmethod", "shipcomplete", "shipdate", "shippingaddress", "baseshippingtaxamount",
        "shippingtaxamount", "source", "sourcetransaction", "startdate", "status", "entitystatus",
        "subsidiarytaxregnum", "taxdetailsoverride", "taxpointdate", "taxpointdateoverride",
        "taxregoverride", "basetaxtotal", "taxtotal", "terms", "title", "tosubsidiary",
        "basetotalaftertaxes", "totalaftertaxes", "foreigntotal", "trackingnumberlist",
        "trandisplayname", "tranisvsoebundle", "transactionnumber", "type", "typebaseddocumentnumber",
        "outsourcingchargeunitprice", "useitemcostastransfercost", "vendor", "void", "voided",
        "wavetype", "website", "recurweekly", "weightedtotal", "winlossreason", "iswip"
    ]

def filter_columns_with_data(df, table_name):
    """Filtrar columnas que tienen al menos un valor no nulo"""
    if df.empty:
        return df
    
    print(f"Analizando columnas para {table_name}...")
    print(f"Columnas iniciales: {len(df.columns)}")
    
    # Identificar columnas con al menos un valor no nulo
    columns_with_data = []
    columns_removed = []
    
    for col in df.columns:
        # Contar valores no nulos y no vacíos
        non_null_count = df[col].notna().sum()
        non_empty_count = (df[col].astype(str).str.strip() != '').sum()
        
        if non_null_count > 0 and non_empty_count > 0:
            columns_with_data.append(col)
            completion_pct = (non_null_count / len(df)) * 100
            print(f"  ✓ '{col}': {non_null_count} valores ({completion_pct:.1f}%)")
        else:
            columns_removed.append(col)
    
    print(f"Columnas con datos: {len(columns_with_data)}")
    print(f"Columnas removidas (sin datos): {len(columns_removed)}")
    
    if columns_removed:
        print(f"Columnas removidas: {', '.join(columns_removed[:10])}")
        if len(columns_removed) > 10:
            print(f"... y {len(columns_removed) - 10} más")
    
    # Filtrar DataFrame
    df_filtered = df[columns_with_data].copy()
    return df_filtered

def discover_available_columns(table, account_id, access_token):
    """Descubrir qué columnas están realmente disponibles para un tipo de transacción"""
    record_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"
    all_columns = get_all_transaction_columns()
    available_columns = []
    
    print(f"Descubriendo columnas disponibles para {table}...")
    
    # Probar columnas en lotes de 15 para encontrar las que existen
    batch_size = 15
    for i in range(0, len(all_columns), batch_size):
        batch = all_columns[i:i+batch_size]
        
        try:
            test_fields = ",".join(batch)
            test_sql = {"q": f"SELECT {test_fields} FROM transaction WHERE type='{table}'"}
            
            res = requests.post(record_url, headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "Prefer": "transient"
            }, json=test_sql, params={"limit": 5})
            
            if res.status_code == 200:
                # Si el batch funciona, todas las columnas del batch existen
                available_columns.extend(batch)
                print(f"  ✓ Batch {i//batch_size + 1}: {len(batch)} columnas válidas")
            else:
                # Si el batch falla, probar columnas individualmente
                print(f"  ⚠ Batch {i//batch_size + 1} falló, probando individualmente...")
                for col in batch:
                    try:
                        single_test = {"q": f"SELECT {col} FROM transaction WHERE type='{table}'"}
                        single_res = requests.post(record_url, headers={
                            "Authorization": f"Bearer {access_token}",
                            "Content-Type": "application/json",
                            "Prefer": "transient"
                        }, json=single_test, params={"limit": 5})
                        
                        if single_res.status_code == 200:
                            available_columns.append(col)
                            print(f"    ✓ '{col}' disponible")
                        else:
                            print(f"    ❌ '{col}' no disponible")
                    except:
                        print(f"    ❌ '{col}' error al probar")
                        
        except Exception as e:
            print(f"  Error probando batch {i//batch_size + 1}: {e}")
            continue
    
    print(f"Total columnas disponibles encontradas: {len(available_columns)}")
    return available_columns

def extract_table_data(table, account_id, access_token, entity_tables):
    """Extraer datos de una tabla específica"""
    record_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"
    df_table = pd.DataFrame()
    limit = 1000
    offset = 0
    
    # Construir query según el tipo de tabla
    if table in entity_tables:
        sql = {"q": f"SELECT * FROM {table}"}
        print(f"Query para entity table: SELECT * FROM {table}")
    else:
        # Descubrir columnas disponibles primero
        available_columns = discover_available_columns(table, account_id, access_token)
        if not available_columns:
            print(f"No se encontraron columnas disponibles para {table}")
            return pd.DataFrame()
        
        select_fields = ",".join(available_columns)
        sql = {"q": f"SELECT {select_fields} FROM transaction WHERE type='{table}'"}
        print(f"Query para transaction table con {len(available_columns)} columnas disponibles")
        print(f"Primeras 10 columnas: {available_columns[:10]}")
    
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
                
                # Mostrar información sobre las columnas obtenidas (solo primera página)
                if offset == 0:
                    print(f"Columnas obtenidas para {table}: {len(df_page.columns)}")
                
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
    
    # Filtrar columnas sin datos solo para tablas transaccionales
    if table not in entity_tables and not df_table.empty:
        df_table = filter_columns_with_data(df_table, table)
    
    # Mostrar estadísticas finales
    if not df_table.empty:
        print(f"Resumen final de {table}:")
        print(f"  - Total registros: {len(df_table)}")
        print(f"  - Columnas finales: {len(df_table.columns)}")
    else:
        print(f"⚠️  No se obtuvieron datos para la tabla {table}")
    
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
    
    # Definir tablas a extraer
    entity_tables = ["customer", "customersubsidiaryrelationship", "vendor","vendorsubsidiaryrelationship"]
    transactions_tables = ["CustPymt", "CustInvc", "Opprtnty", "PurchOrd", "SalesOrd"]
    tables = entity_tables + transactions_tables
    
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
        
        get_available_transaction_types(creds["ACCOUNT_ID"], access_token)
        start_job = time.time()
        successful_tables = 0
        
        # Procesar cada tabla
        for table in tables:
            try:
                print(f"\n--- Procesando tabla: {table} ---")
                df_table = extract_table_data(table, creds["ACCOUNT_ID"], access_token, entity_tables)
                
                cant = len(df_table.index)
                print(f"Cantidad de registros de la tabla {table}: {cant}")
                
                if save_to_s3(df_table, table, args.output_s3_bucket):
                    successful_tables += 1
                
                del df_table
                
            except Exception as e:
                print(f"Error procesando tabla {table}: {e}")
                continue
        
        end_job = time.time()
        print(f"\n=== Migración completada ===")
        print(f"Tablas procesadas exitosamente: {successful_tables}/{len(tables)}")
        print(f"Tiempo total: {end_job - start_job:.2f} segundos")
        
    except Exception as e:
        print(f"Error en la ejecución: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())