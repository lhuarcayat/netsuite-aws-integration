import sys
import boto3
import json
import time
import jwt
import requests
import pandas as pd
from io import StringIO
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, [
    "INPUT_S3_BUCKET_NAME",
    "S3_KEY_PATH",
    "OUTPUT_S3_BUCKET_NAME",
    "SECRET_NAME",
    "REGION_NAME"
])

S3_INPUT_BUCKET = args["INPUT_S3_BUCKET_NAME"]
S3_KEY_PATH = args["S3_KEY_PATH"]
SECRET_NAME = args["SECRET_NAME"]
REGION_NAME = args["REGION_NAME"]
S3_OUTPUT_BUCKET = args['OUTPUT_S3_BUCKET_NAME']

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
        # print("Access Token:", access_token)
        return access_token
    else:
        print("Error al obtener token:", resp.status_code, resp.text)

# -------------------------------
#  CONSUMIR EL API
# -------------------------------
entity_tables = ["customer","customersubsidiaryrelationship","vendor"]
transactions_tables = ["AdvIjrnl","AsmBld","AsmUnBld","BinTrnfr","BinWksht","BPO","CashRfnd","CashSale","Check","CardChrg","CardRfnd",
                      "CustCrM","CustDep","CustPymt","CustRfnd","Depst","DepAppl","Estimate","ExpRept","FulfReq","InvTrn","CustInvc",
                      "ItemShip","ItemRcpt","Journal","Opprtnty","PeJrnl","PrchCntrct","PurchOrd","PurchRqst","RtnAuth","SalesOrd",
                      "TrnfrOrd","VendBill","VendCred","VendPymt","VPrep","VPrepApp","WorkOrd"]
tables = entity_tables + transactions_tables
# api_url = f"https://{ACCOUNT_ID}.suitetalk.api.netsuite.com/services/rest/record/v1"
# catalog_url = f"https://{ACCOUNT_ID}.suitetalk.api.netsuite.com/services/rest/record/v1/metadata-catalog"
record_url = f"https://{ACCOUNT_ID}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"

start_job = time.time()
access_token = get_access_token()
for table in tables:
    df_table = pd.DataFrame()
    limit = 1000
    offset = 0
    
    if table in entity_tables:
        sql = {"q": f"SELECT * FROM {table}"}
    else:
        transaction_fields = [
            "id","tranId","tranDate","entity","status","memo","createdDate","lastModifiedDate","source","currency","location"
        ]
        select_fields = ",".join(transaction_fields)
        sql = {"q": f"SELECT {select_fields} FROM transaction WHERE type='{table}'"}
    
    print(sql)
    start_table = time.time()
    while True:
        print(f"Consultando tabla {table} | limit={limit}&offset={offset}")
        res = requests.post(record_url,headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Prefer": "transient"
        },
        json=sql,
        params={"limit": limit,"offset": offset})

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
                print(f"ya no hay más paginas en la tabla {table}.")
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
    # print(df_table.head())
    if cant > 0:
        print('Guardar el DataFrame en formato Parquet en S3')
        s3_path = f"s3://{S3_OUTPUT_BUCKET}/{table}/{table}.parquet"
        df_table.to_parquet(s3_path, engine='pyarrow', compression='snappy', index=False)
        print(f"Archivos registrado correctamente en S3 en la ruta: {s3_path}")
    
    end_table = time.time()
    print(f"Tiempo de procesamiento de la tabla {table}: {end_table - start_table}.seg")
    del(df_table)

end_job = time.time()
print(f"Migración de datos desde Netsuite completada con exito!: {end_job - start_job}.seg")