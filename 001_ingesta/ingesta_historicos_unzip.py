# Descomprime los zip históricos V4 CLOUD

import subprocess
from google.cloud import storage

# Configura las credenciales y el proyecto de GCP
client = storage.Client()

# Bucket de origen y destino
bucket_origen = 'bcrudo_historicoszip'
bucket_destino = 'bcrudo_historicosunzip'

# Obtén los blobs (archivos) del bucket de origen
bucket_origen = client.get_bucket(bucket_origen)
bucket_destino = client.get_bucket(bucket_destino)
blobs = bucket_origen.list_blobs()

# Itera sobre los blobs y descomprime cada uno
for blob in blobs:
    # Ruta completa del archivo comprimido
    archivo_comprimido = f'gs://{bucket_origen.name}/{blob.name}'

    # Ruta de destino para el archivo descomprimido
    directorio_descomprimido = blob.name.replace(".zip", "")
    archivo_descomprimido = f'gs://{bucket_destino.name}/{directorio_descomprimido}/'

    # Verifica si el archivo descomprimido ya existe en el bucket de destino
    prefix = directorio_descomprimido + "/"
    existe_en_destino = False
    for blob_destino in bucket_destino.list_blobs(prefix=prefix):
        existe_en_destino = True
        break

    if not existe_en_destino:
        # Descomprime el archivo utilizando gsutil
        comando = f'gsutil cp {archivo_comprimido} . && unzip -oq {blob.name} -d {directorio_descomprimido} && gsutil cp -r {directorio_descomprimido} {archivo_descomprimido}'
        subprocess.run(comando, shell=True, check=True)
        print(f'Archivo {blob.name} descomprimido en {archivo_descomprimido}')
    else:
        print(f'El archivo {blob.name} ya está descomprimido en {archivo_descomprimido}')
