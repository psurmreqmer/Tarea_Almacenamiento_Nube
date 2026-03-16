import time
import os
from dotenv import load_dotenv
from conexion import conectar_aws

'''
¿Qué almacenaría?

Datasets históricos: Archivos CSV o Parquet con el rendimiento académico de años anteriores.

Logs de actividad: Registro de inscripciones de los alumnos a los talleres.

Resultados de consultas: El historial de análisis sobre qué categorías de talleres (Tecnología, etc.) tienen más demanda.
'''

load_dotenv()
session = conectar_aws()
athena = session.client('athena')

BUCKET_NAME = "estudiantes-proyectoaws"
DATABASE = "db_estudiantes"
OUTPUT_S3 = f"s3://{BUCKET_NAME}/athena-resultados/"

def imprimir_resultados(query_id):
    try:
        results = athena.get_query_results(QueryExecutionId=query_id)
        print("-" * 50)
        for row in results['ResultSet']['Rows']:
            # Extraemos los valores de cada celda
            celdas = [col.get('VarCharValue', 'NULL') for col in row['Data']]
            print(" | ".join(celdas))
        print("-" * 50)
    except Exception as e:
        print(f"No se pudieron mostrar los datos: {e}")

def ejecutar_query(query, descripcion, mostrar_datos=False):
    print(f"\nEjecutando: {descripcion}...")
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': OUTPUT_S3}
    )
    query_id = response['QueryExecutionId']
    
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)
    
    if status == 'SUCCEEDED':
        print(f"Éxito.")
        if mostrar_datos:
            imprimir_resultados(query_id)
    else:
        print(f"Falló: {status}")

def punto_7_athena_csv():
    try:
        # 1. Crear la Base de Datos
        ejecutar_query(f"CREATE DATABASE IF NOT EXISTS {DATABASE}", "Creación de Base de Datos")

        # 2. Crear la Tabla
        query_tabla = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.talleres (
            id_taller INT,
            nombre STRING,
            cupos INT,
            categoria STRING
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        LOCATION 's3://{BUCKET_NAME}/proyectos/datos_2026/'
        TBLPROPERTIES ("skip.header.line.count"="1");
        """
        ejecutar_query(query_tabla, "Creación de la tabla 'talleres'")

        # 3. CONSULTAS
        
        query_1 = 'SELECT * FROM "talleres" WHERE "categoria" = \'Tecnologia\';'
        ejecutar_query(query_1, "Consulta 1: Talleres de Tecnología", mostrar_datos=True)

        query_2 = 'SELECT "categoria", COUNT(*) as total FROM "talleres" GROUP BY "categoria";'
        ejecutar_query(query_2, "Consulta 2: Conteo por categoría", mostrar_datos=True)

        query_3 = 'SELECT "nombre", "cupos" FROM "talleres" ORDER BY "cupos" DESC LIMIT 1;'
        ejecutar_query(query_3, "Consulta 3: Taller con más cupos", mostrar_datos=True)

    except Exception as e:
        print(f"Error en Athena: {e}")

if __name__ == "__main__":
    if session:
        punto_7_athena_csv()