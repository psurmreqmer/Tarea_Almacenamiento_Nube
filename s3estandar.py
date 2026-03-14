import os
import io
import pandas as pd
from dotenv import load_dotenv
from conexion import conectar_aws

load_dotenv()
session = conectar_aws()
s3 = session.client('s3')

BUCKET_NAME = "estudiantes-proyectoaws" 

def punto_1_s3_estandar():
    try:
        print(f"Creando bucket: {BUCKET_NAME}...")
        s3.create_bucket(Bucket=BUCKET_NAME)

        # Datos: ID, Nombre del Taller, Cupos Disponibles, Categoría
        data = {
            'id_taller': [1, 2, 3, 4],
            'nombre': ['Introduccion a Python', 'CV Pro y Empleo', 'Becas Erasmus', 'IA para Estudiantes'],
            'cupos': [20, 10, 50, 25],
            'categoria': ['Tecnologia', 'Empleo', 'Internacional', 'Tecnologia']
        }
        df = pd.DataFrame(data)
        
        # Convertir el DataFrame a formato CSV en memoria
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        path_s3 = "proyectos/datos_2026/talleres.csv"
        
        print(f"Subiendo objeto a: {path_s3}...")
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=path_s3,
            Body=csv_buffer.getvalue()
        )

        print("Recuperando objeto del bucket para verificar...")
        response = s3.get_object(Bucket=BUCKET_NAME, Key=path_s3)
        
        # Leer el contenido para confirmar
        contenido = response['Body'].read().decode('utf-8')
        print("\nObjeto obtenido con éxito. Contenido:")
        print(contenido[:100] + "...")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if session:
        punto_1_s3_estandar()