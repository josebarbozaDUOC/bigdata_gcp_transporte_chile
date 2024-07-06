# Descarga los datos de recorridos diarios V1 cloud shell
import urllib.request
import json
from google.cloud import storage
from datetime import datetime
import os

# Inicializar cliente de Google Cloud Storage
client = storage.Client()

# Nombre del bucket y la carpeta basada en la fecha de hoy
bucket_name = 'bcrudo_diarios-v2'
fecha_hoy = datetime.now().strftime('%Y_%m_%d')
carpeta = f"{fecha_hoy}/"

# Obtener el bucket
bucket = client.bucket(bucket_name)

# URL de las APIs
url_all = "https://www.red.cl/restservice_v2/rest/getservicios/all"
url_reco = "https://www.red.cl/restservice_v2/rest/conocerecorrido?codsint="

# Obtener la lista de recorridos
with urllib.request.urlopen(url_all) as response:
    if response.status == 200:
        recorridos = json.loads(response.read())

        # Crear carpeta en el bucket si no existe
        if not bucket.blob(carpeta).exists():
            new_blob = bucket.blob(carpeta)
            new_blob.upload_from_string('', content_type='application/x-www-form-urlencoded;charset=UTF-8')
        
        for recorrido in recorridos:
            url_tempo = url_reco + recorrido
            with urllib.request.urlopen(url_tempo) as response_tempo:
                if response_tempo.status == 200:
                    data = json.loads(response_tempo.read())
                    archivo_nombre = f"{carpeta}{recorrido}.json"
                    blob = bucket.blob(archivo_nombre)
                    
                    # Verificar si el archivo ya existe
                    if not blob.exists():
                        blob.upload_from_string(json.dumps(data, indent=4), content_type='application/json')
                        print(f"Archivo {archivo_nombre} subido correctamente.")
                    else:
                        print(f"Archivo {archivo_nombre} ya existe en el bucket, omitiendo descarga.")
                else:
                    print(f"Error al obtener los datos de la API para el recorrido {recorrido}.")
    else:
        print(f"Error al obtener los datos de la API desde {url_all}.")
