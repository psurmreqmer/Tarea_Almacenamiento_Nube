import time
import os
import json
from dotenv import load_dotenv
from conexion import conectar_aws

# 1. Cargar configuración
load_dotenv()
session = conectar_aws()
s3 = session.client('s3')
athena = session.client('athena')

BUCKET_NAME = "estudiantes-proyectoaws"
DATABASE_JSON = "db_alumnos_json"
OUTPUT_S3 = f"s3://{BUCKET_NAME}/athena-resultados/"

def imprimir_resultados(query_id):
    try:
        results = athena.get_query_results(QueryExecutionId=query_id)
        print("-" * 60)
        for row in results['ResultSet']['Rows']:
            celdas = [col.get('VarCharValue', 'NULL') for col in row['Data']]
            print(" | ".join(celdas))
        print("-" * 60)
    except Exception as e:
        print(f"Error al mostrar datos: {e}")

def ejecutar_query(query, descripcion, mostrar_datos=False):
    print(f"\n🚀 Ejecutando: {descripcion}...")
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': 'default'},
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
        error_msg = athena.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status'].get('StateChangeReason')
        print(f"❌ Falló: {status}. Motivo: {error_msg}")

def preparar_datos_s3():
    # Datos de ejemplo
    datos = [
        {"id": 1, "nombre": "Martin", "curso": "Cloud", "nota": 9.5, "becado": True},
        {"id": 2, "nombre": "Elena", "curso": "IA", "nota": 8.0, "becado": False},
        {"id": 3, "nombre": "Lucas", "curso": "Cloud", "nota": 7.2, "becado": True},
        {"id": 4, "nombre": "Sara", "curso": "Python", "nota": 10.0, "becado": True}
    ]
    
    # Formato JSON
    cuerpo_json = "\n".join([json.dumps(d) for d in datos])
    
    print(f"Subiendo archivo JSON a S3...")
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key="datos_alumnos/alumnos.json",
        Body=cuerpo_json
    )

def punto_8_athena_json():
    try:
        # A. Preparar el archivo en S3
        preparar_datos_s3()

        # B. Crear la Base de Datos
        ejecutar_query(f"CREATE DATABASE IF NOT EXISTS {DATABASE_JSON}", f"Crear DB {DATABASE_JSON}")

        # C. Crear la Tabla JSON
        query_tabla = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_JSON}.alumnos (
            id int,
            nombre string,
            curso string,
            nota double,
            becado boolean
        )
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        LOCATION 's3://{BUCKET_NAME}/datos_alumnos/';
        """
        ejecutar_query(query_tabla, "Creación de tabla Alumnos (JSON)")

        # D. Queries
        
        # Query 1: Alumnos con nota mayor o igual a 8
        q1 = f'SELECT nombre, nota, curso FROM {DATABASE_JSON}.alumnos WHERE nota >= 8.0;'
        ejecutar_query(q1, "Consulta 1: Alumnos sobresalientes", mostrar_datos=True)

        # Query 2: Contar cuántos alumnos hay por curso
        q2 = f'SELECT curso, COUNT(*) as total FROM {DATABASE_JSON}.alumnos GROUP BY curso;'
        ejecutar_query(q2, "Consulta 2: Conteo por curso", mostrar_datos=True)

        # Query 3: Ver solo los alumnos becados
        q3 = f'SELECT * FROM {DATABASE_JSON}.alumnos WHERE becado = true;'
        ejecutar_query(q3, "Consulta 3: Listado de becados", mostrar_datos=True)

    except Exception as e:
        print(f"Error general: {e}")

if __name__ == "__main__":
    if session:
        punto_8_athena_json()