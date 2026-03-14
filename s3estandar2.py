import os
import io
from dotenv import load_dotenv
from conexion import conectar_aws

load_dotenv()
session = conectar_aws()
s3 = session.client('s3')

BUCKET_NAME_IA = "estudiantes-poco-frecuente" 

def punto_2_s3_ia():
    try:
        region = os.getenv("REGION")
        print(f"🪣 Creando bucket: {BUCKET_NAME_IA} en {region}...")
        
        if region == "us-east-1":
            s3.create_bucket(Bucket=BUCKET_NAME_IA)
        else:
            s3.create_bucket(
                Bucket=BUCKET_NAME_IA,
                CreateBucketConfiguration={'LocationConstraint': region}
            )

        # Definimos la ruta y el contenido
        path_s3 = "archivo_ia/info_estudiantes_ia.txt"
        contenido = "Este es un archivo de acceso poco frecuente (IA) sobre becas antiguas."

        print(f"📤 Subiendo objeto a la capa STANDARD_IA...")
        s3.put_object(
            Bucket=BUCKET_NAME_IA,
            Key=path_s3,
            Body=contenido,
            StorageClass='STANDARD_IA'
        )

        # --- OBTENER EL OBJETO ---
        print("Recuperando objeto del bucket para verificar...")
        response = s3.get_object(Bucket=BUCKET_NAME_IA, Key=path_s3)
        
        # Leemos el contenido y verificamos la clase
        contenido_recuperado = response['Body'].read().decode('utf-8')
        clase_almacenamiento = response.get('StorageClass', 'STANDARD')

        print(f"\nObjeto obtenido con éxito.")
        print(f"Capa de almacenamiento confirmada: {clase_almacenamiento}")
        print(f"Contenido: {contenido_recuperado}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if session:
        punto_2_s3_ia()