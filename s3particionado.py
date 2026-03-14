import time
import os
from dotenv import load_dotenv
from conexion import conectar_aws

load_dotenv()
session = conectar_aws()
s3 = session.client('s3')
athena = session.client('athena')

BUCKET_NAME = "estudiantes-proyectoaws"
DATABASE = "db_estudiantes"
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

def preparar_datos_particionados():
    # Mes 03 (Marzo)
    data_marzo = "id_alumno,estado\n1,presente\n2,ausente\n3,presente"
    # Mes 04 (Abril)
    data_abril = "id_alumno,estado\n1,presente\n2,presente\n3,ausente"

    print("Subiendo datos particionados a S3...")
    # Subimos a carpetas con formato llave=valor
    s3.put_object(Bucket=BUCKET_NAME, Key="asistencias/mes=03/marzo.csv", Body=data_marzo)
    s3.put_object(Bucket=BUCKET_NAME, Key="asistencias/mes=04/abril.csv", Body=data_abril)

def punto_9_athena_particionado():
    try:
        # 1. Subir los archivos en sus carpetas de partición
        preparar_datos_particionados()

        # 2. Crear la tabla particionada
        query_tabla = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.asistencias (
            id_alumno INT,
            estado STRING
        )
        PARTITIONED BY (mes STRING)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        LOCATION 's3://{BUCKET_NAME}/asistencias/'
        TBLPROPERTIES ("skip.header.line.count"="1");
        """
        ejecutar_query(query_tabla, "Creación de Tabla Particionada")

        # 3. Cargar las particiones
        ejecutar_query(f"MSCK REPAIR TABLE {DATABASE}.asistencias", "Reparando/Cargando Particiones")

        # 4. Queries
        # Consulta: Ver asistencias solo del mes de Marzo
        query_final = f'SELECT * FROM "asistencias" WHERE "mes" = \'03\';'
        ejecutar_query(query_final, "Consulta sobre Partición (Mes 03)", mostrar_datos=True)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if session:
        punto_9_athena_particionado()