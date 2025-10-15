import sys
import boto3
import json
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, max as spark_max, lit, when, regexp_replace
from pyspark.sql.types import *
from botocore.exceptions import ClientError

# -------------------------------
# INICIALIZAR SPARK Y GLUE CONTEXT
# -------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
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
# FUNCIÓN PARA CARGAR CONFIGURACIÓN DE TIPOS DE DATOS
# -------------------------------
def load_data_types_config(s3_client, bucket_name, file_key):
    """Cargar configuración de tipos de datos desde S3"""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        config_content = response['Body'].read().decode('utf-8')
        config_data = json.loads(config_content)
        print(f"📄 Configuración de tipos de datos cargada exitosamente")
        return config_data
    except Exception as e:
        print(f"❌ Error cargando configuración de tipos de datos: {e}")
        return None

def get_table_schema(config_data, table_name):
    """Obtener esquema de tipos para una tabla específica"""
    if not config_data:
        return None
    
    for table_config in config_data:
        if table_config.get('table_name') == table_name:
            return table_config.get('columns', {})
    
    print(f"⚠️ No se encontró configuración para la tabla: {table_name}")
    return None

# -------------------------------
# FUNCIONES DE TRANSFORMACIÓN SILVER MEJORADAS
# -------------------------------
def apply_data_type_transformations(df, table_name, table_schema):
    """Aplicar transformaciones de tipos de datos basadas en la configuración"""
    if not table_schema:
        print(f"⚠️ No hay esquema definido para {table_name}, omitiendo transformaciones de tipos")
        return df
    
    print(f"🔄 Aplicando transformaciones de tipos para {table_name}")
    
    # Obtener columnas existentes en el DataFrame
    existing_columns = set(df.columns)
    transformed_columns_count = 0
    
    for column_name, column_config in table_schema.items():
        if column_name not in existing_columns:
            continue
            
        data_type = column_config.get('data_type')
        if not data_type:
            continue
            
        try:
            # Aplicar transformación según el tipo de dato
            if data_type == 'int':
                df = df.withColumn(column_name, 
                    when(col(column_name).isNull(), lit(None))
                    .otherwise(
                        when(col(column_name).rlike(r'^\s*$'), lit(None))
                        .otherwise(col(column_name).cast(IntegerType()))
                    )
                )
                
            elif data_type == 'double':
                df = df.withColumn(column_name,
                    when(col(column_name).isNull(), lit(None))
                    .otherwise(
                        when(col(column_name).rlike(r'^\s*$'), lit(None))
                        .otherwise(col(column_name).cast(DoubleType()))
                    )
                )
                
            elif data_type == 'boolean':
                df = df.withColumn(column_name,
                    when(col(column_name).isNull(), lit(None))
                    .otherwise(
                        when(col(column_name).rlike(r'^\s*$'), lit(None))
                        .otherwise(
                            when(col(column_name).isin(['true', 'True', 'TRUE', '1', 'yes', 'Yes', 'YES']), lit(True))
                            .when(col(column_name).isin(['false', 'False', 'FALSE', '0', 'no', 'No', 'NO']), lit(False))
                            .otherwise(col(column_name).cast(BooleanType()))
                        )
                    )
                )
                
            elif data_type == 'string':
                df = df.withColumn(column_name,
                    when(col(column_name).isNull(), lit(None))
                    .otherwise(col(column_name).cast(StringType()))
                )
                
            elif data_type == 'date':
                # Solo aplicar transformación de fecha si no es lastmodifieddate (ya procesada)
                if column_name != 'lastmodifieddate':
                    df = df.withColumn(column_name,
                        when(col(column_name).isNull(), lit(None))
                        .otherwise(
                            when(col(column_name).rlike(r'^\s*$'), lit(None))
                            .otherwise(
                                # Intentar múltiples formatos de fecha
                                when(col(column_name).rlike(r'^\d{2}/\d{2}/\d{4}$'), 
                                     to_date(col(column_name), 'dd/MM/yyyy'))
                                .when(col(column_name).rlike(r'^\d{4}-\d{2}-\d{2}$'),
                                     to_date(col(column_name), 'yyyy-MM-dd'))
                                .when(col(column_name).rlike(r'^\d{2}-\d{2}-\d{4}$'),
                                     to_date(col(column_name), 'dd-MM-yyyy'))
                                .otherwise(to_date(col(column_name)))
                            )
                        )
                    )
            
            transformed_columns_count += 1
            
        except Exception as e:
            print(f"⚠️ Error transformando columna {column_name} a {data_type}: {e}")
            # Continuar con las demás columnas en caso de error
            continue
    
    print(f"✅ Transformadas {transformed_columns_count} columnas de {table_name}")
    return df

def apply_silver_transformations(df, table_name, config_data):
    """Aplicar transformaciones Silver a un Spark DataFrame"""
    initial_count = df.count()
    print(f"📊 Aplicando transformaciones Silver. Registros iniciales: {initial_count}")
    
    # 1. Eliminar columna 'links' si existe
    if 'links' in df.columns:
        df = df.drop('links')
        print("🗑️ Columna 'links' eliminada")
    
    # 2. Convertir lastmodifieddate de DD/MM/YYYY a YYYY-MM-DD datetime (lógica original)
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
    
    # 3. NUEVA FUNCIONALIDAD: Aplicar transformaciones de tipos según configuración
    table_schema = get_table_schema(config_data, table_name)
    if table_schema:
        df = apply_data_type_transformations(df, table_name, table_schema)
    else:
        print(f"⚠️ No se encontró configuración de esquema para {table_name}")
    
    # 4. Deduplicación - eliminar registros exactamente iguales
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
        
        print(f"📖 Leyendo partición Bronze: {partition_path}")
        
        # Leer todos los parquet files de la partición
        df_partition = spark.read.parquet(partition_path)
        
        record_count = df_partition.count()
        print(f"📊 Registros en partición {execution_date}: {record_count}")
        
        return df_partition
        
    except Exception as e:
        print(f"❌ Error leyendo partición Bronze {execution_date} para {table_name}: {e}")
        # Retornar DataFrame vacío en caso de error
        return spark.createDataFrame([], StructType([]))

def clean_existing_silver_files(s3_client, bucket_name, table_name):
    """Eliminar todos los archivos Silver existentes para una tabla antes de crear el nuevo"""
    try:
        prefix = f"{table_name}/"
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            print(f"ℹ️ No hay archivos existentes en Silver para {table_name}")
            return
        
        # Identificar archivos a eliminar (.parquet y archivos relacionados)
        files_to_delete = []
        for obj in response['Contents']:
            key = obj['Key']
            # Eliminar archivos .parquet, .crc, _SUCCESS, etc.
            if (key.endswith('.parquet') or 
                key.endswith('.crc') or 
                '_SUCCESS' in key or
                key.endswith('.metadata')):
                files_to_delete.append({'Key': key})
        
        if files_to_delete:
            print(f"🗑️ Eliminando {len(files_to_delete)} archivos Silver existentes para {table_name}")
            
            # Eliminar archivos en lotes (máximo 1000 por lote)
            for i in range(0, len(files_to_delete), 1000):
                batch = files_to_delete[i:i+1000]
                s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': batch}
                )
            
            print(f"✅ Archivos Silver anteriores eliminados para {table_name}")
        else:
            print(f"ℹ️ No hay archivos que eliminar para {table_name}")
            
    except Exception as e:
        print(f"❌ Error eliminando archivos Silver existentes para {table_name}: {e}")

def read_existing_silver(bucket_name, table_name):
    """Leer tabla Silver existente si existe usando Spark"""
    try:
        # Construir path de la tabla Silver (directorio completo)
        silver_table_path = f"s3://{bucket_name}/{table_name}/"
        print(f"📖 Intentando leer Silver existente: {silver_table_path}")
        
        # Intentar leer todos los archivos parquet del directorio
        df_existing = spark.read.parquet(silver_table_path)
        record_count = df_existing.count()
        print(f"📊 Registros en Silver existente: {record_count}")
        return df_existing
            
    except Exception as e:
        print(f"ℹ️ No existe Silver previo para {table_name} o error leyendo: {e}")
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

def save_silver_data(df, bucket_name, table_name, execution_date):
    """
    SOLUCIÓN SIMPLE: Dejar que Spark maneje todo automáticamente
    """
    try:
        silver_s3_path = f"s3://{bucket_name}/{table_name}/"
        
        print(f"💾 Guardando Silver en directorio: {silver_s3_path}")
        
        df.coalesce(1).write.mode('overwrite').option('compression', 'snappy').parquet(silver_s3_path)
        
        print(f"✅ Silver guardado exitosamente")
        print(f"📁 Estructura: bucket/tabla/part-XXXXX.parquet + _SUCCESS")
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

# Configuración para el archivo de tipos de datos
CONFIG_S3_BUCKET = "arcl-dev-00-glue"  # Bucket donde está el archivo de configuración
CONFIG_FILE_KEY = "scripts/data_types_all_tables.json"

# Fecha de ejecución Silver
SILVER_EXECUTION_DATE = datetime.utcnow().strftime('%Y-%m-%d')

# Inicializar watermark manager y S3 client
watermark_manager = SilverWatermarkManager(SILVER_WATERMARK_TABLE, REGION_NAME)
s3_client = boto3.client('s3', region_name=REGION_NAME)

# Importar funciones adicionales de Spark
from pyspark.sql import functions as F

# Cargar configuración de tipos de datos
print("📄 Cargando configuración de tipos de datos...")
config_data = load_data_types_config(s3_client, CONFIG_S3_BUCKET, CONFIG_FILE_KEY)

# -------------------------------
# CONFIGURACIÓN DE TABLAS
# -------------------------------
#entity_tables = ["bom", "customer", "customersubsidiaryrelationship", "inventorybalance", "item", "vendor", "vendorsubsidiaryrelationship"]
transactions_tables = ["CustPymt", "CustInvc", "Opprtnty", "PurchOrd", "SalesOrd"]
#entity_tables = ["transactionline", "transactionshippingaddress","transactionaccountingline"]
entity_tables = ["transactionshippingaddress","transactionaccountingline","bom", "customer", "customersubsidiaryrelationship", "inventorybalance", "item", "vendor", "vendorsubsidiaryrelationship"]
tables = entity_tables + transactions_tables

print("=== TRANSFORMACIÓN SILVER SPARK ===")
print(f"📅 Fecha de ejecución Silver: {SILVER_EXECUTION_DATE}")
print(f"📂 Bronze bucket: {BRONZE_S3_BUCKET}")
print(f"📂 Silver bucket: {SILVER_S3_BUCKET}")
print(f"🗃️ Tabla DynamoDB watermarks: {SILVER_WATERMARK_TABLE}")
print(f"📄 Archivo configuración: s3://{CONFIG_S3_BUCKET}/{CONFIG_FILE_KEY}")
print("=" * 50)

# -------------------------------
# PROCESAR TABLAS SILVER CON CDC
# -------------------------------
successful_tables = 0
start_job = datetime.utcnow()

for table in tables:
    print(f"\n📄 --- Procesando tabla Silver: {table} ---")
    
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
            print(f"🔄 Última partición Bronze procesada: {last_processed}")
            
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
        
        # 5. Aplicar transformaciones Silver a datos nuevos (NUEVA VERSIÓN CON CONFIG)
        df_new_transformed = apply_silver_transformations(df_new_bronze, table, config_data)
        
        # 6. Leer Silver existente
        df_existing_silver = read_existing_silver(SILVER_S3_BUCKET, table)
        
        # 7. Union y deduplicación final
        if df_existing_silver.count() == 0:
            df_final = df_new_transformed
            print("📁 Creando Silver desde cero")
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
        
        # 8. Guardar Silver con archivo único
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