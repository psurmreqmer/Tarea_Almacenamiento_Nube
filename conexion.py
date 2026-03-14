import boto3
from dotenv import load_dotenv
import os

# 1. Cargar configuración
load_dotenv()

def conectar_aws():
    """Establece la conexión inicial y devuelve una sesión de boto3."""
    try:
        session = boto3.session.Session(
            aws_access_key_id=os.getenv("ACCESS_KEY"),
            aws_secret_access_key=os.getenv("SECRET_KEY"),
            aws_session_token=os.getenv("SESSION_TOKEN"),
            region_name=os.getenv("REGION"),
            SubnetId=os.getenv("SUBNET_ID")
        )
        
        # Validamos la conexión haciendo una llamada simple (puedes usar sts)
        sts = session.client('sts')
        sts.get_caller_identity()
        
        print("Conexión con AWS exitosa")
        return session
    
    except Exception as e:
        print(f"Fallo en conexión: {e}")
        return None

#conectar_aws()