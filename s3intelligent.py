import os
from dotenv import load_dotenv
from conexion import conectar_aws


load_dotenv()
session = conectar_aws()
s3 = session.client('s3')

BUCKET_NAME_INT = "estudiantes-intelligent-tiering" 

def punto_3_s3_intelligent():
    try:
        region = os.getenv("REGION")
        print(f"🪣 Creando bucket: {BUCKET_NAME_INT} en {region}...")
        
        if region == "us-east-1":
            s3.create_bucket(Bucket=BUCKET_NAME_INT)
        else:
            s3.create_bucket(
                Bucket=BUCKET_NAME_INT,
                CreateBucketConfiguration={'LocationConstraint': region}
            )

        path_s3 = "fotos_alumnos/guia_estudiante.txt"
        contenido = "Guía completa para el alumno. Almacenada en Intelligent-Tiering para optimizar costes automáticamente."

        print(f"📤 Subiendo objeto a la capa INTELLIGENT_TIERING...")
        s3.put_object(
            Bucket=BUCKET_NAME_INT,
            Key=path_s3,
            Body=contenido,
            StorageClass='INTELLIGENT_TIERING'
        )

        # Obtener objetos y mostrar datos
        print("Recuperando objeto para previsualización...")
        response = s3.get_object(Bucket=BUCKET_NAME_INT, Key=path_s3)
        
        contenido_recuperado = response['Body'].read().decode('utf-8')
        
        # Extracción de metadatos
        clase = response.get('StorageClass', 'STANDARD (Default)')
        tamano = response.get('ContentLength')
        fecha = response.get('LastModified')
        tipo = response.get('ContentType')

        # --- DISEÑO DE LA PREVISUALIZACIÓN ---
        print("\n" + "="*50)
        print("DATOS DEL OBJETO RECUPERADO")
        print("="*50)
        print(f"Ruta S3:   s3://{BUCKET_NAME_INT}/{path_s3}")
        print(f"Clase:     {clase}")
        print(f"Tipo:      {tipo}")
        print(f"Tamaño:    {tamano} bytes")
        print(f"Modificado: {fecha}")
        print("-" * 50)
        print("CONTENIDO DEL ARCHIVO:")
        print(f'"{contenido_recuperado}"')
        print("="*50 + "\n")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if session:
        punto_3_s3_intelligent()