import json

# Cargar configuración
with open('config/parameters.json', 'r') as f:
    config = json.load(f)

# Construir argumentos
import sys
sys.argv.extend([
    '--input-s3-bucket', config['input_s3_bucket'],
    '--s3-key-path', config['s3_key_path'],
    '--output-s3-bucket', config['output_s3_bucket'],
    '--secret-name', config['secret_name'],
    '--region-name', config['region_name']
])

# Ejecutar script principal
#from netsuite_extractor import main
from local_ingest_data_all_columns import main
main()