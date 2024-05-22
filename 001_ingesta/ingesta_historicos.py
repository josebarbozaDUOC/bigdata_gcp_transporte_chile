# Descarga de zip históricos V3 CLOUD

from google.cloud import storage
import requests
import io

# Autenticación con Google Cloud Storage
client = storage.Client()

# Nombre del bucket de destino en Google Cloud Storage
bucket_name = "bcrudo_historicoszip"

# Obtener el bucket
bucket = client.bucket(bucket_name)

# URL de la API
url = "https://us-central1-duoc-bigdata-sc-2023-01-01.cloudfunctions.net/datos_transporte_et"

# GET a la API
response = requests.get(url)

# Try-Except
if response.status_code == 200:
    # Obtener los datos en formato JSON
    data = response.json()
    # Acceder a la lista de recursos
    resources = data["result"]["resources"]

    # Iterar sobre la lista de recursos
    for resource in resources:
        # Obtener la URL del recurso
        resource_url = resource["url"]
        resource_created = resource["created"]
        # Nombre del archivo
        filename = resource_url.split("/")[-1]

        # Verificar si el archivo ya existe en el bucket
        blob = bucket.blob(filename)
        if not blob.exists():
            # Obtener el contenido del archivo
            file_content_response = requests.get(resource_url)
            if file_content_response.status_code == 200:
                # Subir el contenido del archivo al bucket de GCS
                file_content = file_content_response.content
                blob.upload_from_file(io.BytesIO(file_content))
                print(f"Archivo subido: {filename}")
                print(f"Fecha de creación: {resource_created}")
            else:
                print(f"No se pudo obtener el contenido del archivo desde {resource_url}")
        else:
            print(f"El archivo {filename} ya existe en el bucket, se omitirá su descarga.")
else:
    print("Error al obtener los datos de la API")
