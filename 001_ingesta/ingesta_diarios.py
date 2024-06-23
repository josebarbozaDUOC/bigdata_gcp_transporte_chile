# Ingesta de datos diarios utilizando cloud function, activada por pubsub de scheduler
# al finalizar la ingesta, realiza un pub, para activar la siguiente fase
import functions_framework
from google.cloud import storage
from google.cloud import pubsub_v1
from datetime import datetime
import requests
import json
import logging
import os
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Configura el logging
logging.basicConfig(level=logging.INFO)

# Configuración
PROJECT_ID = os.getenv('PROJECT_ID', 'transporte001')
PUB_TEMA = os.getenv('PUB_TEMA', 'Termino_ingesta_diarios')
BUCKET_NAME = os.getenv('BUCKET_NAME', 'bcrudo_diarios')
API_URL_ALL = os.getenv('API_URL_ALL', 'https://www.red.cl/restservice_v2/rest/getservicios/all')
API_URL_RECO = os.getenv('API_URL_RECO', 'https://www.red.cl/restservice_v2/rest/conocerecorrido?codsint=')

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

@functions_framework.cloud_event
def ingesta_diarios(cloud_event):
    try:
        # Inicializar cliente de Google Cloud Storage
        client = storage.Client()

        # Nombre del bucket y la carpeta basada en la fecha de hoy
        fecha_hoy = datetime.now().strftime('%Y_%m_%d')
        carpeta = f"{fecha_hoy}/"

        # Obtener el bucket
        bucket = client.bucket(BUCKET_NAME)

        # Verificar si ya se procesó
        estado_blob = bucket.blob(f"{carpeta}estado.txt")
        if estado_blob.exists():
            logging.info({"message": "Datos ya procesados para hoy, omitiendo ingesta", "date": fecha_hoy})
            return

        # Obtener la lista de archivos existentes en el bucket
        blobs = bucket.list_blobs(prefix=carpeta)
        archivos_existentes = {blob.name for blob in blobs}

        # Obtener la lista de recorridos
        response = http.get(API_URL_ALL)
        response.raise_for_status()
        recorridos = response.json()

        # Crear carpeta en el bucket si no existe
        if not bucket.blob(carpeta).exists():
            new_blob = bucket.blob(carpeta)
            new_blob.upload_from_string('', content_type='application/x-www-form-urlencoded;charset=UTF-8')

        for recorrido in recorridos:
            archivo_nombre = f"{carpeta}{recorrido}.json"
            if archivo_nombre in archivos_existentes:
                logging.info({"message": "Archivo ya existe en el bucket, omitiendo descarga", "filename": archivo_nombre})
                continue

            try:
                url_tempo = API_URL_RECO + recorrido
                response_tempo = http.get(url_tempo)
                response_tempo.raise_for_status()
                data = response_tempo.json()

                blob = bucket.blob(archivo_nombre)
                blob.upload_from_string(json.dumps(data, indent=4), content_type='application/json')
                logging.info({"message": "Archivo subido correctamente", "filename": archivo_nombre})

                # Actualizar la lista de archivos existentes
                archivos_existentes.add(archivo_nombre)

            except requests.RequestException as e:
                logging.error({"message": "Error al obtener los datos de la API para el recorrido", "recorrido": recorrido, "error": str(e)})

        # Marcar como completado
        estado_blob.upload_from_string("completado", content_type='text/plain')

        logging.info({"message": "Proceso de ingesta de datos diarios completado"})

        # Enviar mensaje a Pub/Sub
        try:
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(PROJECT_ID, PUB_TEMA)
            
            message = 'Término de ingesta de diarios'
            future = publisher.publish(topic_path, message.encode('utf-8'))
            logging.info({"message": "Mensaje enviado a Pub/Sub", "message_id": future.result()})
        except Exception as e:
            logging.error({"message": "Error al enviar mensaje a Pub/Sub", "error": str(e)})
    except requests.RequestException as e:
        logging.error({"message": "Error al obtener los datos de la API desde", "url": API_URL_ALL, "error": str(e)})
    except Exception as e:
        logging.error({"message": "Error inesperado", "error": str(e)})
