import pandas as pd
import numpy as np
import json
import re
from pathlib import Path
import os
from datetime import datetime

class CSVDataTypeAnalyzer:
    def __init__(self):
        # Patrones para detectar fechas
        self.date_patterns = [
            r'^\d{1,2}/\d{1,2}/\d{4}$',  # DD/MM/YYYY o MM/DD/YYYY
            r'^\d{4}/\d{1,2}/\d{1,2}$',  # YYYY/MM/DD
            r'^\d{1,2}-\d{1,2}-\d{4}$',  # DD-MM-YYYY o MM-DD-YYYY
            r'^\d{4}-\d{1,2}-\d{1,2}$',  # YYYY-MM-DD
        ]
        
        # Patrones para detectar booleanos
        self.boolean_values = {
            'true', 'false', 'yes', 'no', 'y', 'n', 
            '1', '0', 'si', 'no', 'verdadero', 'falso',
            'T', 'F', 'TRUE', 'FALSE', 'YES', 'NO'
        }
    
    def is_date(self, value):
        """Verifica si un valor es una fecha en los formatos especificados."""
        if pd.isna(value) or value == '':
            return False
            
        str_value = str(value).strip()
        
        for pattern in self.date_patterns:
            if re.match(pattern, str_value):
                try:
                    # Intentar parsear la fecha para validar
                    if '/' in str_value:
                        parts = str_value.split('/')
                        if len(parts[0]) == 4:  # YYYY/MM/DD
                            datetime.strptime(str_value, '%Y/%m/%d')
                        else:  # DD/MM/YYYY
                            datetime.strptime(str_value, '%d/%m/%Y')
                    elif '-' in str_value:
                        parts = str_value.split('-')
                        if len(parts[0]) == 4:  # YYYY-MM-DD
                            datetime.strptime(str_value, '%Y-%m-%d')
                        else:  # DD-MM-YYYY
                            datetime.strptime(str_value, '%d-%m-%Y')
                    return True
                except ValueError:
                    continue
        return False
    
    def is_boolean(self, value):
        """Verifica si un valor es booleano."""
        if pd.isna(value) or value == '':
            return False
        return str(value).strip().lower() in self.boolean_values
    
    def is_numeric(self, value):
        """Verifica si un valor es numérico y devuelve el tipo."""
        if pd.isna(value) or value == '':
            return None
        
        try:
            str_value = str(value).strip()
            
            # Intentar convertir a float (maneja notación científica)
            float_val = float(str_value)
            
            # Si es infinito o NaN, no es un número válido
            if np.isinf(float_val) or np.isnan(float_val):
                return None
            
            # Verificar si es un entero
            if float_val.is_integer():
                # Verificar que el string original no tenga punto decimal explícito
                # Ejemplos: "1.0" -> double, "1" -> int, "1.0E2" -> double
                if '.' in str_value or 'e' in str_value.lower():
                    return 'double'
                else:
                    return 'int'
            else:
                return 'double'
                
        except (ValueError, TypeError, OverflowError):
            return None
    
    def is_integer(self, value):
        """Verifica si un valor es un entero."""
        result = self.is_numeric(value)
        return result == 'int'
    
    def is_double(self, value):
        """Verifica si un valor es un número decimal."""
        result = self.is_numeric(value)
        return result == 'double'
    
    def analyze_column(self, series, column_name):
        """Analiza una columna y determina su tipo de dato."""
        # Filtrar valores nulos y vacíos para el análisis
        non_null_values = series.dropna()
        non_empty_values = non_null_values[non_null_values.astype(str).str.strip() != '']
        
        if len(non_empty_values) == 0:
            return "string"
        
        # Tomar una muestra para el análisis (máximo 1000 valores para eficiencia)
        sample_size = min(1000, len(non_empty_values))
        sample_values = non_empty_values.sample(n=sample_size, random_state=42) if len(non_empty_values) > sample_size else non_empty_values
        
        # Contadores para cada tipo
        date_count = 0
        boolean_count = 0
        integer_count = 0
        double_count = 0
        string_count = 0
        
        # Debug: mostrar algunos valores de ejemplo
        print(f"    Analizando columna '{column_name}' - Muestra de valores:")
        for i, value in enumerate(sample_values.head(5)):
            numeric_type = self.is_numeric(value)
            print(f"      '{value}' -> tipo numérico: {numeric_type}")
        
        for value in sample_values:
            if self.is_date(value):
                date_count += 1
            elif self.is_boolean(value):
                boolean_count += 1
            elif self.is_integer(value):
                integer_count += 1
            elif self.is_double(value):
                double_count += 1
            else:
                string_count += 1
        
        total_count = len(sample_values)
        
        # Debug: mostrar contadores
        print(f"    Contadores - Date: {date_count}, Bool: {boolean_count}, Int: {integer_count}, Double: {double_count}, String: {string_count}")
        
        # Determinar el tipo basado en el porcentaje más alto
        # Se requiere al menos 80% de consistencia para tipos específicos
        threshold = 0.8
        
        if date_count / total_count >= threshold:
            return "date"
        elif boolean_count / total_count >= threshold:
            return "boolean"
        elif integer_count / total_count >= threshold:
            return "int"
        elif double_count / total_count >= threshold:
            return "double"
        else:
            return "string"
    
    def analyze_csv(self, file_path):
        """Analiza un archivo CSV y devuelve la información de tipos de datos."""
        try:
            # Leer el CSV
            df = pd.read_csv(file_path, low_memory=False)
            
            # Obtener el nombre de la tabla (nombre del archivo sin extensión)
            table_name = Path(file_path).stem
            
            # Analizar cada columna
            columns_info = {}
            
            print(f"Analizando archivo: {file_path}")
            print(f"Columnas encontradas: {len(df.columns)}")
            
            for column in df.columns:
                print(f"\n  Procesando columna: {column}")
                data_type = self.analyze_column(df[column], column)
                columns_info[column] = {
                    "data_type": data_type
                }
                print(f"    Resultado: {column} -> {data_type}")
            
            return {
                "table_name": table_name,
                "columns": columns_info
            }
            
        except Exception as e:
            print(f"Error al procesar {file_path}: {str(e)}")
            return None
    
    def analyze_multiple_csvs(self, directory_path=None, file_paths=None):
        """Analiza múltiples archivos CSV."""
        results = []
        
        if directory_path:
            # Analizar todos los CSVs en un directorio
            csv_files = list(Path(directory_path).glob("*.csv"))
            file_paths = [str(f) for f in csv_files]
        
        if not file_paths:
            print("No se encontraron archivos CSV para analizar.")
            return []
        
        for file_path in file_paths:
            result = self.analyze_csv(file_path)
            if result:
                results.append(result)
        
        return results
    
    def save_results_to_json(self, results, output_file="data_prueba/csv_analysis_results.json"):
        """Guarda los resultados en un archivo JSON."""
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\nResultados guardados en: {output_file}")
    
    def test_numeric_detection(self):
        """Método para probar la detección de valores numéricos."""
        test_values = [
            "-324690.68",
            "-3.465763813E7",
            "2.108152167E7",
            "0.0",
            "0",
            "-123",
            "456",
            "1.5",
            "-2.7",
            "1.0",
            "not_a_number",
            "true",
            "2023-01-01"
        ]
        
        print("Probando detección de tipos numéricos:")
        print("-" * 50)
        
        for value in test_values:
            numeric_type = self.is_numeric(value)
            is_int = self.is_integer(value)
            is_double = self.is_double(value)
            
            print(f"'{value}' -> numérico: {numeric_type}, int: {is_int}, double: {is_double}")

# Función principal para usar el analizador
def main():
    analyzer = CSVDataTypeAnalyzer()
    
    # Probar la detección de tipos numéricos
    print("=== PRUEBA DE DETECCIÓN NUMÉRICA ===")
    analyzer.test_numeric_detection()
    print("\n" + "="*50 + "\n")
    
    # Opción 1: Analizar un solo archivo
    # single_file = "fcf3b31f57814a4a863430656490547c.csv"  # Cambiar por tu archivo
    # if os.path.exists(single_file):
    #     result = analyzer.analyze_csv(single_file)
    #     if result:
    #         print(f"\nResultado para {single_file}:")
    #         print(json.dumps(result, indent=2, ensure_ascii=False))
            
    #         # Guardar resultado
    #         analyzer.save_results_to_json([result], f"{Path(single_file).stem}_analysis.json")
    
    # Opción 2: Analizar todos los CSVs en un directorio
    directory = "data_prueba"  # Cambiar por tu directorio
    if os.path.exists(directory):
        results = analyzer.analyze_multiple_csvs(directory_path=directory)
        if results:
            analyzer.save_results_to_json(results)
    else:
        print(f"Directorio '{directory}' no encontrado. Ajusta la ruta en el código.")
    
    # Opción 3: Analizar archivos específicos
    # file_list = ["archivo1.csv", "archivo2.csv", "archivo3.csv"]
    # results = analyzer.analyze_multiple_csvs(file_paths=file_list)

if __name__ == "__main__":
    main()