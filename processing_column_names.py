import pandas as pd
import json
from typing import Dict, List

def convert_csv_to_table_columns_json(csv_file: str, output_file: str = 'table_columns.json') -> Dict[str, List[str]]:
    """
    Convierte un CSV con formato 'table_name,column_name' a un JSON 
    donde las claves son nombres de tablas y los valores son listas de columnas.
    
    Args:
        csv_file (str): Ruta al archivo CSV de entrada
        output_file (str): Ruta al archivo JSON de salida
    
    Returns:
        Dict[str, List[str]]: Diccionario con tablas y sus columnas
    """
    
    table_columns = {}
    
    try:
        # Leer el archivo CSV línea por línea para manejar el formato especial
        with open(csv_file, 'r', encoding='utf-8') as file:
            lines = file.readlines()
        
        # Filtrar líneas que no sean headers y que tengan contenido
        data_lines = [line.strip() for line in lines 
                     if line.strip() and 'table_name","column_name' not in line]
        
        print(f"Procesando {len(data_lines)} líneas de datos...")
        
        for i, line in enumerate(data_lines):
            # Limpiar comillas externas
            clean_line = line.strip().strip('"')
            
            # Encontrar la primera coma para separar table_name y column_name
            first_comma_index = clean_line.find(',')
            
            if first_comma_index > -1:
                # Extraer table_name y column_name
                table_name = clean_line[:first_comma_index].strip().strip('"')
                column_name = clean_line[first_comma_index + 1:].strip().strip('"')
                
                # Validar que ambos campos existan y no sean vacíos
                if table_name and column_name and table_name != 'table_name':
                    # Inicializar lista de columnas si la tabla no existe
                    if table_name not in table_columns:
                        table_columns[table_name] = []
                    
                    # Agregar columna si no existe ya (evitar duplicados)
                    if column_name not in table_columns[table_name]:
                        table_columns[table_name].append(column_name)
            
            # Mostrar progreso cada 100 líneas
            if (i + 1) % 100 == 0:
                print(f"Procesadas {i + 1} líneas...")
        
        # Ordenar las columnas de cada tabla para consistencia
        for table in table_columns:
            table_columns[table].sort()
        
        # Guardar en archivo JSON
        with open(output_file, 'w', encoding='utf-8') as json_file:
            json.dump(table_columns, json_file, indent=2, ensure_ascii=False)
        
        # Mostrar resumen
        print(f"\n✅ Conversión completada!")
        print(f"📊 Total de tablas: {len(table_columns)}")
        print(f"💾 Archivo guardado como: {output_file}")
        print("\n📋 Resumen de tablas:")
        
        for table_name, columns in sorted(table_columns.items()):
            print(f"  • {table_name}: {len(columns)} columnas")
        
        return table_columns
        
    except FileNotFoundError:
        print(f"❌ Error: El archivo '{csv_file}' no se encontró.")
        return {}
    except Exception as e:
        print(f"❌ Error procesando el archivo: {str(e)}")
        return {}

def show_sample_data(table_columns: Dict[str, List[str]], max_tables: int = 3, max_columns: int = 5):
    """
    Muestra una muestra de los datos convertidos.
    
    Args:
        table_columns (Dict[str, List[str]]): Diccionario de tablas y columnas
        max_tables (int): Número máximo de tablas a mostrar
        max_columns (int): Número máximo de columnas a mostrar por tabla
    """
    print(f"\n🔍 Muestra de datos (primeras {max_tables} tablas):")
    
    for i, (table_name, columns) in enumerate(sorted(table_columns.items())):
        if i >= max_tables:
            break
            
        print(f"\n📁 {table_name}:")
        displayed_columns = columns[:max_columns]
        print(f"   {', '.join(displayed_columns)}")
        
        if len(columns) > max_columns:
            print(f"   ... y {len(columns) - max_columns} columnas más")

if __name__ == "__main__":
    # Opción 1: Versión completa con validaciones y reportes
    print("🚀 Iniciando conversión de CSV a JSON...")
    result = convert_csv_to_table_columns_json("column_names.csv", "table_columns.json")
    
    if result:
        show_sample_data(result)
        print(f"\n✨ ¡Listo! Archivo JSON creado exitosamente.")
    else:
        print("❌ La conversión falló. Revisa el archivo CSV y vuelve a intentar.")


