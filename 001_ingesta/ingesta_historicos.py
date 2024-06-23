# ingesta historicos pubsub: zip y descomprime (no async) v5
import functions_framework
from google.cloud import storage
from google.cloud import pubsub_v1
import requests
import logging
import io
import os
import zipfile
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Configura el logging
logging.basicConfig(level=logging.INFO)

# Configuración
PROJECT_ID = os.getenv('PROJECT_ID', 'transporte001')
PUB_TEMA = os.getenv('PUB_TEMA', 'Termino_ingesta_historicos_zip')
BUCKET_NAME_ZIP = os.getenv('BUCKET_NAME', 'bcrudo_historicoszip')
BUCKET_NAME_UNZIP = 'bcrudo_historicosunzip'
API_URL = os.getenv('API_URL', 'https://us-central1-duoc-bigdata-sc-2023-01-01.cloudfunctions.net/datos_transporte_et')

# Configurar reintentos para requests
retry_strategy = Retry(
    total=3,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.mount("http://", adapter)

def procesar_zip(bucket_zip, bucket_unzip, filename, zip_content):
    # Subir el ZIP al bucket de ZIPs
    blob_zip = bucket_zip.blob(filename)
    blob_zip.upload_from_string(zip_content)
    logging.info({"message": "Archivo ZIP subido", "filename": filename})

    # Descomprimir y subir al bucket de archivos descomprimidos
    with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_ref:
        for zip_info in zip_ref.infolist():
            if zip_info.filename[-1] == '/':  # Es un directorio
                continue
            file_content = zip_ref.read(zip_info.filename)
            unzipped_blob = bucket_unzip.blob(f"{filename[:-4]}/{zip_info.filename}")
            unzipped_blob.upload_from_string(file_content)
    logging.info({"message": "Archivo descomprimido y subido", "filename": filename})

@functions_framework.cloud_event
def ingesta_historicos_zip(cloud_event):
    try:
        # Autenticación con Google Cloud Storage
        client = storage.Client()
        
        # Obtener los buckets
        bucket_zip = client.bucket(BUCKET_NAME_ZIP)
        bucket_unzip = client.bucket(BUCKET_NAME_UNZIP)
        
        # GET a la API
        response = http.get(API_URL)
        response.raise_for_status()
        
        # Obtener los datos en formato JSON
        data = response.json()
        
        # Acceder a la lista de recursos
        resources = data["result"]["resources"]
        
        # Iterar sobre la lista de recursos
        for resource in resources:
            resource_url = resource["url"]
            resource_created = resource["created"]
            filename = resource_url.split("/")[-1]
            
            blob_zip = bucket_zip.blob(filename)
            if not blob_zip.exists():
                # Descargar el archivo ZIP
                file_content_response = http.get(resource_url)
                file_content_response.raise_for_status()
                zip_content = file_content_response.content
                
                # Procesar el ZIP (subir y descomprimir)
                procesar_zip(bucket_zip, bucket_unzip, filename, zip_content)
            else:
                logging.info({"message": "Archivo ZIP ya existe", "filename": filename})
                
                # Verificar si ya está descomprimido
                prefix = filename[:-4] + "/"
                blobs_unzipped = list(bucket_unzip.list_blobs(prefix=prefix))
                if not blobs_unzipped:
                    # Si no está descomprimido, lo descargamos y procesamos
                    zip_content = blob_zip.download_as_bytes()
                    procesar_zip(bucket_zip, bucket_unzip, filename, zip_content)
                else:
                    logging.info({"message": "Archivo ya descomprimido", "filename": filename})
        
        logging.info({"message": "Proceso de descarga y descompresión completado"})

        # Enviar mensaje a Pub/Sub
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, PUB_TEMA)
        
        message = 'Término de ingesta y descompresión de históricos zip'
        future = publisher.publish(topic_path, message.encode('utf-8'))
        logging.info({"message": "Mensaje enviado a Pub/Sub", "message_id": future.result()})

    except Exception as e:
        logging.error({"message": "Error en el proceso", "error": str(e)})
        raise

    return 'Procesamiento completado'
