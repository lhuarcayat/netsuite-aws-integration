import sys
import boto3
import json
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, max as spark_max
from pyspark.sql.types import *
#from pyspark.sql.types import StructType
from botocore.exceptions import ClientError

# -------------------------------
# INICIALIZAR SPARK Y GLUE CONTEXT
# -------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# -------------------------------
# CONFIGURACIÓN DYNAMODB PARA SILVER
# -------------------------------
class SilverWatermarkManager:
    def __init__(self, table_name, region_name):
        self.dynamodb = boto3.resource('dynamodb', region_name=region_name)
        self.table = self.dynamodb.Table(table_name)
        
    def get_watermark(self, table_name):
        """Obtener último watermark para una tabla Silver"""
        try:
            response = self.table.get_item(Key={'table_name': table_name})
            if 'Item' in response:
                return {
                    'last_bronze_execution_date': response['Item']['last_bronze_execution_date'],
                    'silver_execution_date': response['Item']['silver_execution_date'],
                    'total_records': response['Item'].get('total_records', 0)
                }
            return None
        except ClientError as e:
            print(f"Error obteniendo watermark Silver para {table_name}: {e}")
            return None
    
    def update_watermark(self, table_name, last_bronze_execution_date, silver_execution_date, record_count):
        """Actualizar watermark para una tabla Silver"""
        try:
            self.table.put_item(
                Item={
                    'table_name': table_name,
                    'last_bronze_execution_date': last_bronze_execution_date,
                    'silver_execution_date': silver_execution_date,
                    'total_records': record_count,
                    'updated_at': datetime.utcnow().isoformat()
                }
            )
            print(f"✅ Watermark Silver actualizado para {table_name}: Bronze={last_bronze_execution_date}")
        except ClientError as e:
            print(f"❌ Error actualizando watermark Silver para {table_name}: {e}")

# -------------------------------
# FUNCIONES DE TRANSFORMACIÓN SILVER
# -------------------------------
def apply_silver_transformations(df):
    """Aplicar transformaciones Silver a un Spark DataFrame"""
    initial_count = df.count()
    print(f"📊 Aplicando transformaciones Silver. Registros iniciales: {initial_count}")
    
    # 1. Eliminar columna 'links' si existe
    if 'links' in df.columns:
        df = df.drop('links')
        print("🗑️ Columna 'links' eliminada")
    
    # 2. Convertir lastmodifieddate de DD/MM/YYYY a YYYY-MM-DD datetime
    if 'lastmodifieddate' in df.columns:
        try:
            # Convertir de DD/MM/YYYY string a date YYYY-MM-DD
            df = df.withColumn('lastmodifieddate', 
                              to_date(col('lastmodifieddate'), 'dd/MM/yyyy'))
            
            # Contar registros válidos
            valid_dates = df.filter(col('lastmodifieddate').isNotNull()).count()
            print(f"📅 Convertido lastmodifieddate a date. Registros válidos: {valid_dates}")
            
            # Mostrar rango de fechas para verificación
            if valid_dates > 0:
                date_stats = df.filter(col('lastmodifieddate').isNotNull()).agg(
                    F.min('lastmodifieddate').alias('min_date'),
                    spark_max('lastmodifieddate').alias('max_date')
                ).collect()[0]
                
                print(f"📅 Rango de fechas: {date_stats['min_date']} - {date_stats['max_date']}")
                
        except Exception as e:
            print(f"⚠️ Error convirtiendo lastmodifieddate: {e}")
    
    # 3. Deduplicación - eliminar registros exactamente iguales
    df_deduplicated = df.dropDuplicates()
    final_count = df_deduplicated.count()
    duplicates_removed = initial_count - final_count
    
    if duplicates_removed > 0:
        print(f"🔄 Eliminados {duplicates_removed} registros duplicados")
    else:
        print("✅ No se encontraron registros duplicados")
    
    print(f"📊 Transformaciones completadas. Registros finales: {final_count}")
    return df_deduplicated

def get_bronze_partitions(s3_client, bucket_name, table_name):
    """Obtener lista de particiones execution_date disponibles en Bronze"""
    try:
        prefix = f"{table_name}/"
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix,
            Delimiter='/'
        )
        
        partitions = []
        for common_prefix in response.get('CommonPrefixes', []):
            # Extraer execution_date de path como "customer/execution_date=2025-09-23/"
            partition_path = common_prefix['Prefix']
            if 'execution_date=' in partition_path:
                execution_date = partition_path.split('execution_date=')[1].rstrip('/')
                partitions.append(execution_date)
        
        partitions.sort()
        print(f"📂 Particiones Bronze disponibles para {table_name}: {partitions}")
        return partitions
        
    except Exception as e:
        print(f"❌ Error listando particiones Bronze para {table_name}: {e}")
        return []

def read_bronze_partition(bucket_name, table_name, execution_date):
    """Leer todos los archivos parquet de una partición Bronze usando Spark"""
    try:
        partition_path = f"s3://{bucket_name}/{table_name}/execution_date={execution_date}/"
        
        print(f"📁 Leyendo partición Bronze: {partition_path}")
        
        # Leer todos los parquet files de la partición
        df_partition = spark.read.parquet(partition_path)
        
        record_count = df_partition.count()
        print(f"📊 Registros en partición {execution_date}: {record_count}")
        
        return df_partition
        
    except Exception as e:
        print(f"❌ Error leyendo partición Bronze {execution_date} para {table_name}: {e}")
        # Retornar DataFrame vacío en caso de error
        return spark.createDataFrame([], StructType([]))

def get_latest_silver_file(s3_client, bucket_name, table_name):
    """Obtener el archivo Silver más reciente para una tabla"""
    try:
        prefix = f"{table_name}/"
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            print(f"ℹ️ No existe carpeta Silver para {table_name}")
            return None
        
        # Filtrar solo archivos .parquet y ordenar por fecha de modificación
        parquet_files = []
        for obj in response['Contents']:
            if obj['Key'].endswith('.parquet'):
                parquet_files.append({
                    'key': obj['Key'],
                    'last_modified': obj['LastModified']
                })
        
        if not parquet_files:
            print(f"ℹ️ No hay archivos .parquet en Silver para {table_name}")
            return None
        
        # Obtener el archivo más reciente
        latest_file = max(parquet_files, key=lambda x: x['last_modified'])
        print(f"📄 Archivo Silver más reciente para {table_name}: {latest_file['key']}")
        return latest_file['key']
        
    except Exception as e:
        print(f"❌ Error buscando archivos Silver para {table_name}: {e}")
        return None

def read_existing_silver(bucket_name, table_name):
    """Leer tabla Silver existente si existe usando Spark"""
    try:
        s3_client = boto3.client('s3', region_name=REGION_NAME)
        latest_file_key = get_latest_silver_file(s3_client, bucket_name, table_name)
        
        if not latest_file_key:
            print(f"ℹ️ No existe Silver previo para {table_name}, creando desde cero")
            return spark.createDataFrame([], StructType([]))
        
        silver_path = f"s3://{bucket_name}/{latest_file_key}"
        print(f"📖 Leyendo Silver existente: {silver_path}")
        
        df_existing = spark.read.parquet(silver_path)
        record_count = df_existing.count()
        print(f"📊 Registros en Silver existente: {record_count}")
        return df_existing
            
    except Exception as e:
        print(f"❌ Error leyendo Silver existente para {table_name}: {e}")
        return spark.createDataFrame([], StructType([]))

def safe_union_dataframes(df1, df2):
    """Union segura de DataFrames con esquemas potencialmente diferentes"""
    try:
        # Si alguno está vacío, retornar el otro
        if df1.count() == 0:
            return df2
        if df2.count() == 0:
            return df1
        
        # Obtener columnas comunes
        cols1 = set(df1.columns)
        cols2 = set(df2.columns)
        common_cols = sorted(list(cols1.intersection(cols2)))
        
        if not common_cols:
            print("⚠️ No hay columnas comunes para union")
            return df2  # Retornar el nuevo
        
        print(f"🔗 Uniendo DataFrames con {len(common_cols)} columnas comunes")
        
        # Seleccionar solo columnas comunes y hacer union
        df1_selected = df1.select(*common_cols)
        df2_selected = df2.select(*common_cols)
        
        return df1_selected.union(df2_selected)
        
    except Exception as e:
        print(f"❌ Error en union de DataFrames: {e}")
        return df2  # En caso de error, retornar solo el nuevo

def generate_filename_with_timestamp(table_name, execution_date):
    """Generar nombre de archivo con timestamp"""
    timestamp = datetime.utcnow().strftime('%H%M%S')
    return f"{table_name}_{execution_date}_{timestamp}.parquet"

def save_silver_data(df, bucket_name, table_name, execution_date):
    """Guardar DataFrame en Silver con la nueva estructura de carpetas y nombres"""
    try:
        # Generar nombre del archivo con timestamp
        filename = generate_filename_with_timestamp(table_name, execution_date)
        
        # Construir path completo: bucket/tabla/archivo.parquet
        silver_s3_path = f"s3://{bucket_name}/{table_name}/{filename}"
        
        print(f"💾 Guardando Silver en: {silver_s3_path}")
        
        # Guardar el archivo
        df.coalesce(1).write.mode('overwrite').option('compression', 'snappy').parquet(silver_s3_path)
        
        print(f"✅ Silver guardado exitosamente: {silver_s3_path}")
        return True
        
    except Exception as e:
        print(f"❌ Error guardando Silver: {e}")
        return False

# -------------------------------
# PARSEAR ARGUMENTOS
# -------------------------------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "BRONZE_S3_BUCKET_NAME",
    "SILVER_S3_BUCKET_NAME", 
    "REGION_NAME",
    "SILVER_WATERMARK_TABLE"
])

job.init(args["JOB_NAME"], args)

BRONZE_S3_BUCKET = args["BRONZE_S3_BUCKET_NAME"]
SILVER_S3_BUCKET = args["SILVER_S3_BUCKET_NAME"]
REGION_NAME = args["REGION_NAME"]
SILVER_WATERMARK_TABLE = args["SILVER_WATERMARK_TABLE"]

# Fecha de ejecución Silver
SILVER_EXECUTION_DATE = datetime.utcnow().strftime('%Y-%m-%d')

# Inicializar watermark manager y S3 client
watermark_manager = SilverWatermarkManager(SILVER_WATERMARK_TABLE, REGION_NAME)
s3_client = boto3.client('s3', region_name=REGION_NAME)

# Importar funciones adicionales de Spark
from pyspark.sql import functions as F

# -------------------------------
# CONFIGURACIÓN DE TABLAS
# -------------------------------
entity_tables = ["customer", "customersubsidiaryrelationship", "vendor", "vendorsubsidiaryrelationship"]
transactions_tables = ["CustPymt", "CustInvc", "Opprtnty", "PurchOrd", "SalesOrd"]
tables = entity_tables + transactions_tables

print("=== TRANSFORMACIÓN SILVER SPARK ===")
print(f"📅 Fecha de ejecución Silver: {SILVER_EXECUTION_DATE}")
print(f"📂 Bronze bucket: {BRONZE_S3_BUCKET}")
print(f"📂 Silver bucket: {SILVER_S3_BUCKET}")
print(f"🗃️ Tabla DynamoDB watermarks: {SILVER_WATERMARK_TABLE}")
print("=" * 50)

# -------------------------------
# PROCESAR TABLAS SILVER CON CDC
# -------------------------------
successful_tables = 0
start_job = datetime.utcnow()

for table in tables:
    print(f"\n🔄 --- Procesando tabla Silver: {table} ---")
    
    try:
        # 1. Obtener watermark Silver existente
        watermark_info = watermark_manager.get_watermark(table)
        is_first_run = watermark_info is None
        
        # 2. Obtener particiones Bronze disponibles
        available_partitions = get_bronze_partitions(s3_client, BRONZE_S3_BUCKET, table)
        
        if not available_partitions:
            print(f"⚠️ No hay particiones Bronze para {table}")
            continue
        
        # 3. Determinar qué particiones procesar
        if is_first_run:
            print(f"🆕 Primera ejecución Silver para {table}")
            partitions_to_process = available_partitions
        else:
            last_processed = watermark_info['last_bronze_execution_date']
            print(f"📄 Última partición Bronze procesada: {last_processed}")
            
            # Procesar particiones >= última procesada
            partitions_to_process = [p for p in available_partitions if p >= last_processed]
        
        print(f"📋 Particiones a procesar: {partitions_to_process}")
        
        if not partitions_to_process:
            print(f"ℹ️ No hay particiones nuevas para {table}")
            continue
        
        # 4. Leer datos nuevos de Bronze
        df_new_bronze = None
        
        for partition_date in partitions_to_process:
            print(f"📖 Procesando partición Bronze: {partition_date}")
            df_partition = read_bronze_partition(BRONZE_S3_BUCKET, table, partition_date)
            
            if df_partition.count() > 0:
                if df_new_bronze is None:
                    df_new_bronze = df_partition
                else:
                    df_new_bronze = df_new_bronze.union(df_partition)
        
        if df_new_bronze is None or df_new_bronze.count() == 0:
            print(f"ℹ️ No hay datos nuevos en Bronze para {table}")
            continue
        
        new_records_count = df_new_bronze.count()
        print(f"📊 Total registros nuevos de Bronze: {new_records_count}")
        
        # 5. Aplicar transformaciones Silver a datos nuevos
        df_new_transformed = apply_silver_transformations(df_new_bronze)
        
        # 6. Leer Silver existente
        df_existing_silver = read_existing_silver(SILVER_S3_BUCKET, table)
        
        # 7. Union y deduplicación final
        if df_existing_silver.count() == 0:
            df_final = df_new_transformed
            print("📝 Creando Silver desde cero")
        else:
            print("🔄 Uniendo con Silver existente y deduplicando")
            df_combined = safe_union_dataframes(df_existing_silver, df_new_transformed)
            df_final = df_combined.dropDuplicates()
            
            combined_count = df_combined.count()
            final_count = df_final.count()
            duplicates_removed = combined_count - final_count
            
            if duplicates_removed > 0:
                print(f"🔄 Eliminados {duplicates_removed} duplicados en unión final")
        
        total_final_records = df_final.count()
        print(f"📊 Registros finales en Silver: {total_final_records}")
        
        # 8. Guardar Silver con nueva estructura
        if total_final_records > 0:
            success = save_silver_data(df_final, SILVER_S3_BUCKET, table, SILVER_EXECUTION_DATE)
            
            if success:
                # 9. Actualizar watermark
                latest_partition = max(partitions_to_process)
                watermark_manager.update_watermark(
                    table,
                    latest_partition,
                    SILVER_EXECUTION_DATE,
                    total_final_records
                )
                
                successful_tables += 1
            else:
                print(f"❌ Error guardando Silver para {table}")
        else:
            print(f"⚠️ No hay datos finales para guardar en Silver")
    
    except Exception as e:
        print(f"❌ Error procesando tabla Silver {table}: {e}")
    
    finally:
        # Limpiar cache de Spark
        if 'df_new_bronze' in locals() and df_new_bronze is not None:
            df_new_bronze.unpersist()
        if 'df_new_transformed' in locals():
            df_new_transformed.unpersist()
        if 'df_existing_silver' in locals():
            df_existing_silver.unpersist()
        if 'df_final' in locals():
            df_final.unpersist()

end_job = datetime.utcnow()
duration = (end_job - start_job).total_seconds()

print(f"\n=== RESUMEN EJECUCIÓN SILVER SPARK ===")
print(f"📅 Fecha de ejecución: {SILVER_EXECUTION_DATE}")
print(f"✅ Tablas procesadas exitosamente: {successful_tables}/{len(tables)}")
print(f"⏱️ Tiempo total: {duration:.2f} segundos")
print("=" * 50)

# Finalizar job
job.commit()