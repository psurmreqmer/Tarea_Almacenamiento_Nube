import os
from dotenv import load_dotenv
from conexion import conectar_aws

load_dotenv()
session = conectar_aws()
s3 = session.client('s3')

BUCKET_NAME_GLACIER = "estudiantes-glacier-archivo"

def punto_4_s3_glacier():
    try:
        region = os.getenv("REGION")
        print(f"🪣 Creando bucket Glacier: {BUCKET_NAME_GLACIER} en {region}...")
        
        if region == "us-east-1":
            s3.create_bucket(Bucket=BUCKET_NAME_GLACIER)
        else:
            s3.create_bucket(
                Bucket=BUCKET_NAME_GLACIER,
                CreateBucketConfiguration={'LocationConstraint': region}
            )

        path_s3 = "archivo_historico/expedientes_graduados_2015.txt"
        contenido = "Actas y expedientes de alumnos graduados en el año 2015. Almacenamiento a largo plazo."

        print(f"📤 Subiendo objeto a la capa GLACIER...")
        s3.put_object(
            Bucket=BUCKET_NAME_GLACIER,
            Key=path_s3,
            Body=contenido,
            StorageClass='GLACIER'
        )

        # --- OBTENER EL OBJETO (Metadatos) ---
        print("Recuperando metadatos del archivo (HeadObject)...")
        
        response = s3.head_object(Bucket=BUCKET_NAME_GLACIER, Key=path_s3)
        
        clase = response.get('StorageClass')
        tamano = response.get('ContentLength')
        fecha = response.get('LastModified')
        tipo = response.get('ContentType')

        # mostrar datos
        print("\n" + "="*50)
        print("DATOS TÉCNICOS DEL OBJETO (GLACIER)")
        print("="*50)
        print(f"Ruta S3:   s3://{BUCKET_NAME_GLACIER}/{path_s3}")
        print(f"Clase:     {clase}")
        print(f"Tamaño:    {tamano} bytes")
        print(f"Modificado: {fecha}")
        print("-" * 50)
        print("Objeto localizado con éxito en el archivo.")
        print("="*50 + "\n")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if session:
        punto_4_s3_glacier()