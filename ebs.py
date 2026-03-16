import os
from dotenv import load_dotenv
from conexion import conectar_aws
import time

'''
¿Qué almacenaría?
Bases de datos locales de la aplicación que gestiona los talleres.

Logs del servidor de ofertas de trabajo que requieren baja latencia.

Archivos temporales del sistema operativo de la instancia que ayuda a los alumnos.
'''


# cargar variables del .env
load_dotenv()


session = conectar_aws()

def gestionar_ebs_con_archivo(session):

    client = session.client('ec2')

    clave = "C:/Users/marti/Desktop/awsprueba/llave.pem"
    txt = "prueba.txt"

    # 1. Crear instancia EC2
    instancia = client.run_instances(
        ImageId=os.getenv("AMI"),
        InstanceType=os.getenv("INSTANCIA"),
        MinCount=1,
        MaxCount=1,
        KeyName=os.getenv("PAR_CLAVES"),
        SecurityGroupIds=[os.getenv("SECURITY_GROUP")],
        SubnetId=os.getenv("SUBNET_ID"),

        TagSpecifications=[{
            'ResourceType': 'instance',
            'Tags': [{'Key': 'Name', 'Value': 'ec2-ebs'}]
        }]
    )

    instance_id = instancia['Instances'][0]['InstanceId']
    zona_disponibilidad = instancia['Instances'][0]['Placement']['AvailabilityZone']

    print(f"Instancia creada: {instance_id}")

    client.get_waiter('instance_running').wait(InstanceIds=[instance_id])
    print("Instancia en ejecución")

    # 2. Crear volumen EBS
    volumen = client.create_volume(
        AvailabilityZone=zona_disponibilidad,
        Size=1,
        VolumeType='gp3',

        TagSpecifications=[{
            'ResourceType': 'volume',
            'Tags': [{'Key': 'Name', 'Value': 'volumenPrueba'}]
        }]
    )

    volume_id = volumen['VolumeId']
    print(f"Volumen creado: {volume_id}")

    client.get_waiter('volume_available').wait(VolumeIds=[volume_id])

    # 3. Asociar volumen al EC2
    client.attach_volume(
        Device="/dev/sdf",
        InstanceId=instance_id,
        VolumeId=volume_id
    )

    print("Volumen asociado al EC2")

    client.get_waiter('volume_in_use').wait(VolumeIds=[volume_id])

    # 4. Obtener IP pública
    desc = client.describe_instances(InstanceIds=[instance_id])
    public_ip = desc['Reservations'][0]['Instances'][0]['PublicIpAddress']

    print(f"IP pública: {public_ip}")


    # 5. Formatear y montar EBS
    os.system(f'ssh -o StrictHostKeyChecking=no -i {clave} ec2-user@{public_ip} "sudo mkfs -t ext4 /dev/sdf"')

    os.system(f'ssh -o StrictHostKeyChecking=no -i {clave} ec2-user@{public_ip} "sudo mkdir -p /mnt/volumen"')

    os.system(f'ssh -o StrictHostKeyChecking=no -i {clave} ec2-user@{public_ip} "sudo mount /dev/sdf /mnt/volumen"')


    # 6. Copiar archivo al volumen
    os.system(f'scp -i {clave} {txt} ec2-user@{public_ip}:/tmp/{txt}')

    os.system(f'ssh -i {clave} ec2-user@{public_ip} "sudo mv /tmp/{txt} /mnt/volumen/{txt}"')

    print("Archivo prueba.txt copiado al volumen EBS en la instancia.")

    return instance_id, volume_id



if session:
    gestionar_ebs_con_archivo(session)
else:
    print("No se pudo conectar a AWS")