import boto3
from dotenv import load_dotenv
import os

# Cargar variables de entorno
load_dotenv()

ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
SESSION_TOKEN = os.getenv("SESSION_TOKEN")
REGION = os.getenv("REGION")
SECURITY_GROUP = os.getenv("SECURITY_GROUP")
AMI = os.getenv("AMI")
INSTANCE_TYPE = os.getenv("INSTANCIA")
KEY = os.getenv("INSTANCIA")
PAR_CLAVES = os.getenv("PAR_CLAVES")
SUBNET_ID = os.getenv("SUBNET_ID")


PAR_CLAVES

session = boto3.session.Session(
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    aws_session_token=SESSION_TOKEN,
    region_name=REGION
)

# Crear cliente EC2
ec2_client = session.client('ec2')
print("SECURITY_GROUP:", SECURITY_GROUP)

try:
    # Crear la instancia
    response = ec2_client.run_instances(
    ImageId=AMI,         
    InstanceType=INSTANCE_TYPE,  
    KeyName=PAR_CLAVES,               # ✅ nombre correcto del key pair
    SecurityGroupIds=[SECURITY_GROUP],  
    SubnetId=SUBNET_ID,               
    MinCount=1,            
    MaxCount=1              
)


    # Obtener ID de la instancia creada
    instance_id = response['Instances'][0]['InstanceId']
    print(f"✅ Instancia EC2 creada con ID: {instance_id}")

except Exception as e:
    print("❌ Error al crear la instancia EC2")
    print(e)
