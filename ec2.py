import boto3
import os
from conexion import conectar_aws

'''
¿Qué almacenaría?

En este almacenamiento guardaría los expedientes académicos de los alumnos y los PDFs de las ofertas de trabajo
'''

session = conectar_aws()

def crear_instancia_ec2(session):
    if not session: return None
    ec2_client = session.client('ec2')
    try:
        response = ec2_client.run_instances(
            ImageId=os.getenv("AMI"),
            InstanceType=os.getenv("INSTANCIA"),
            KeyName=os.getenv("PAR_CLAVES"),
            SecurityGroupIds=[os.getenv("SECURITY_GROUP")],
            SubnetId=os.getenv("SUBNET_ID"),
            MinCount=1, MaxCount=1
        )
        instance_id = response['Instances'][0]['InstanceId']
        print(f"Instancia EC2 creada: {instance_id}")
        
        # Esperamos a que esté "running" para poder pararla después
        print("Esperando a que la instancia esté activa...")
        waiter = ec2_client.get_waiter('instance_running')
        waiter.wait(InstanceIds=[instance_id])
        return instance_id
    except Exception as e:
        print(f"Error al crear EC2: {e}")
        return None


def parar_instancia_ec2(session, instance_id):
    ec2_client = session.client('ec2')
    try:
        print(f"Parando instancia {instance_id}...")
        ec2_client.stop_instances(InstanceIds=[instance_id])
        
        # Esperamos a que se detenga del todo
        waiter = ec2_client.get_waiter('instance_stopped')
        waiter.wait(InstanceIds=[instance_id])
        print("Instancia detenida correctamente.")
    except Exception as e:
        print(f"Error al parar EC2: {e}")

def eliminar_instancia_ec2(session, instance_id):
    ec2_client = session.client('ec2')
    try:
        print(f"Eliminando instancia {instance_id}...")
        ec2_client.terminate_instances(InstanceIds=[instance_id])
        
        # Esperamos a que se elimine
        waiter = ec2_client.get_waiter('instance_terminated')
        waiter.wait(InstanceIds=[instance_id])
        print("Instancia eliminada por completo.")
    except Exception as e:
        print(f"Error al eliminar EC2: {e}")
        
session = conectar_aws()


if session:
    # 1. Crear
    id = crear_instancia_ec2(session)
        # 2. Parar
    parar_instancia_ec2(session, id)
        
        # 3. Eliminar
    eliminar_instancia_ec2(session, id)