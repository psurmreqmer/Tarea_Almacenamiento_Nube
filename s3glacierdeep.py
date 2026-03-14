import os
from dotenv import load_dotenv
from conexion import conectar_aws

load_dotenv()
session = conectar_aws()
s3 = session.client('s3')

BUCKET_NAME_DEEP = "estudiantes-deep-archive-glacier" 

def punto_5_s3_deep_archive():
    try:
        region = os.getenv("REGION")
        print(f"🪣 Creando bucket Deep Archive en {region}...")
        
        if region == "us-east-1":
            s3.create_bucket(Bucket=BUCKET_NAME_DEEP)
        else:
            s3.create_bucket(
                Bucket=BUCKET_NAME_DEEP,
                CreateBucketConfiguration={'LocationConstraint': region}
            )

        path_s3 = "legal/registros_fundacion_2005.txt"
        contenido = "Documentos originales de la fundacion de la asociacion de estudiantes. Año 2005."

        print(f"Subiendo objeto a la capa DEEP_ARCHIVE...")
        s3.put_object(
            Bucket=BUCKET_NAME_DEEP,
            Key=path_s3,
            Body=contenido,
            StorageClass='DEEP_ARCHIVE'
        )

        # --- OBTENER EL OBJETO (Metadatos) ---
        # Usamos head_object para evitar el error InvalidObjectState
        print("📥 Recuperando metadatos del archivo profundo...")
        response = s3.head_object(Bucket=BUCKET_NAME_DEEP, Key=path_s3)
        
        clase = response.get('StorageClass')
        tamano = response.get('ContentLength')
        fecha = response.get('LastModified')

        # --- MOSTRAR DATOS ---
        print("\n" + "="*50)
        print("DATOS DEL OBJETO EN DEEP ARCHIVE")
        print("="*50)
        print(f"Ruta S3:   s3://{BUCKET_NAME_DEEP}/{path_s3}")
        print(f"Clase:     {clase}")
        print(f"Tamaño:    {tamano} bytes")
        print(f"Modificado: {fecha}")
        print("-" * 50)
        print("Objeto archivado correctamente en la capa mas economica.")
        print("Nota: La recuperacion de este archivo tarda de 12 a 48 horas.")
        print("="*50 + "\n")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if session:
        punto_5_s3_deep_archive()