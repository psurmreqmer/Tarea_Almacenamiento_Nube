import os
from dotenv import load_dotenv
from conexion import conectar_aws

load_dotenv()

def obtener_s3_client():
    session = conectar_aws()
    return session.client('s3')

def habilitar_versionado(bucket_name):
    s3 = obtener_s3_client()
    s3.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={'Status': 'Enabled'}
    )
    print(f"✅ Versionado habilitado en {bucket_name}")

def subir_y_obtener(bucket, key, body, storage_class='STANDARD'):
    s3 = obtener_s3_client()
    # Subir
    s3.put_object(Bucket=bucket, Key=key, Body=body, StorageClass=storage_class)
    # Obtener (Get)
    response = s3.get_object(Bucket=bucket, Key=key)
    print(f"✅ Objeto {key} subido a {storage_class} y recuperado ({response['ContentLength']} bytes)")
    return response