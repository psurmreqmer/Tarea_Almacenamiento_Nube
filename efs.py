import os
import time
from dotenv import load_dotenv
from conexion import conectar_aws

'''
¿Qué almacenaría?

Documentación compartida: Guías de talleres, PDFs de ofertas de trabajo y materiales de estudio que deben ser accesibles simultáneamente
 por varias instancias (servidores) de la plataforma.

Datos de rendimiento compartidos: Informes consolidados de los alumnos que diferentes departamentos (orientación, administración) 
necesitan consultar al mismo tiempo.
'''
load_dotenv()
session = conectar_aws()

def gestionar_efs(session):
    ec2 = session.client("ec2")
    efs = session.client("efs")

    # Rutas de tus archivos
    clave = "C:/Users/marti/Desktop/awsprueba/llave.pem"
    txt_local = "prueba.txt" 
    
    #  Lanzar Instancia
    print("Lanzando instancia para EFS...")
    instancia = ec2.run_instances(
        ImageId=os.getenv("AMI"),
        InstanceType=os.getenv("INSTANCIA"),
        MinCount=1,
        MaxCount=1,
        KeyName=os.getenv("PAR_CLAVES"),
        SecurityGroupIds=[os.getenv("SECURITY_GROUP")],
        SubnetId=os.getenv("SUBNET_ID")
    )
    
    instance_id = instancia["Instances"][0]["InstanceId"]
    ec2.get_waiter("instance_running").wait(InstanceIds=[instance_id])
    
    # Obtener IP para el SSH
    desc = ec2.describe_instances(InstanceIds=[instance_id])
    public_ip = desc["Reservations"][0]["Instances"][0].get("PublicIpAddress")
    print(f"Instancia lista: {instance_id} en IP: {public_ip}")

    #Crear el Sistema de Archivos EFS
    print("Creando EFS...")
    efs_response = efs.create_file_system(
        PerformanceMode="generalPurpose",
        Tags=[{'Key': 'Name', 'Value': 'EFS-proyecto'}]
    )
    efs_id = efs_response["FileSystemId"]

    # Esperar a que el EFS esté disponible
    while True:
        fs = efs.describe_file_systems(FileSystemId=efs_id)
        if fs["FileSystems"][0]["LifeCycleState"] == "available":
            break
        time.sleep(5)
    print(f"EFS {efs_id} disponible.")

    # 4. Crear Mount Target
    print(f"Creando Mount Target en {os.getenv('SUBNET_ID')}...")
    efs.create_mount_target(
        FileSystemId=efs_id, 
        SubnetId=os.getenv("SUBNET_ID"), 
        SecurityGroups=[os.getenv("SECURITY_GROUP")]
    )

    # Esperar a que el punto de montaje esté listo
    while True:
        mts = efs.describe_mount_targets(FileSystemId=efs_id)["MountTargets"]
        if mts and all(mt["LifeCycleState"] == "available" for mt in mts):
            break
        time.sleep(5)
    print("Punto de montaje listo.")

    # 5. Configuración dentro de la máquina (SSH)
    print("Esperando 60s para que el SSH esté operativo...")
    time.sleep(60)

    print("Instalando utilidades y montando EFS...")
    #Instala, crea carpeta, monta y da permisos
    cmd_setup = (
        f'ssh -o StrictHostKeyChecking=no -i {clave} ec2-user@{public_ip} '
        f'"sudo yum install -y amazon-efs-utils && '
        f'sudo mkdir -p /mnt/efs && '
        f'sudo mount -t efs {efs_id}:/ /mnt/efs && '
        f'sudo chmod 777 /mnt/efs"'
    )
    os.system(cmd_setup)

    # copiar archivo
    print(f"Subiendo {txt_local} al EFS...")
    os.system(f'scp -o StrictHostKeyChecking=no -i {clave} {txt_local} ec2-user@{public_ip}:/tmp/{txt_local}')
    os.system(f'ssh -o StrictHostKeyChecking=no -i {clave} ec2-user@{public_ip} "mv /tmp/{txt_local} /mnt/efs/{txt_local}"')

    print(f"\nArchivo disponible en /mnt/efs/{txt_local}")
    return efs_id

gestionar_efs(session)