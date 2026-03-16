import os
from dotenv import load_dotenv
from conexion import conectar_aws

''' 
¿Qué almacenaría?

Expedientes académicos: Para recuperar versiones anteriores si hay errores en la carga de notas de los alumnos.

Bases de convocatorias de becas: Como haces en el código, para mantener el historial de cambios en las cuantías o requisitos.

Listados de asistencia a talleres: Para auditar cambios accidentales en los datos de rendimiento.
'''

load_dotenv()
session = conectar_aws()
s3 = session.client('s3')

BUCKET_NAME = "estudiantes-proyectoaws" 

def punto_6_control_versiones():
    try:
        # --- HABILITAR EL CONTROL DE VERSIONES ---
        print(f"Habilitando control de versiones en: {BUCKET_NAME}...")
        s3.put_bucket_versioning(
            Bucket=BUCKET_NAME,
            VersioningConfiguration={'Status': 'Enabled'}
        )
        print("Versionado habilitado con éxito.")

        # Nombre del archivo
        key = "becas/urgentes.txt"

        # --- Subir versión original ---
        print("\nSubiendo Versión 1 (Beca inicial)...")
        s3.put_object(
            Bucket=BUCKET_NAME, 
            Key=key, 
            Body="Beca Ayuda Transporte: 100€"
        )

        # --- Subir versión modificada ---
        # Subimos el archivo con el mismo nombre pero distinto contenido
        print("Subiendo Versión 2 (Beca actualizada)...")
        s3.put_object(
            Bucket=BUCKET_NAME, 
            Key=key, 
            Body="Beca Ayuda Transporte: 250€ (Importe actualizado por la directiva)"
        )

        # --- D. MOSTRAR LAS VERSIONES DISPONIBLES ---
        print("\nListando historial de versiones del objeto:")
        versiones = s3.list_object_versions(Bucket=BUCKET_NAME, Prefix=key)

        print("-" * 60)
        print(f"{'VERSION ID':<35} | {'¿ES ACTUAL?':<10}")
        print("-" * 60)

        for v in versiones.get('Versions', []):
            es_actual = "SÍ" if v['IsLatest'] else "NO (Histórica)"
            v_id = v['VersionId']
            print(f"{v_id:<35} | {es_actual:<10}")

        # --- E. MOSTRAR EL CONTENIDO DE LAS DOS VERSIONES ---
        print("\nPREVISUALIZACIÓN DE AMBAS VERSIONES:")
        
        for v in versiones.get('Versions', []):
            res = s3.get_object(Bucket=BUCKET_NAME, Key=key, VersionId=v['VersionId'])
            contenido = res['Body'].read().decode('utf-8')
            estado = "ACTUAL" if v['IsLatest'] else "ANTIGUA"
            print(f"[{estado}]: {contenido}")
        
        print("-" * 60)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if session:
        punto_6_control_versiones()