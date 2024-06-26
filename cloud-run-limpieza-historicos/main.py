import os
import json
import base64
import logging
from flask import Flask, request
from google.cloud import storage
import pandas as pd
import io

# Configuración global
BUCKET_ORIGEN = 'bcrudo_historicosunzip'
BUCKET_DESTINO = 'bclean_historicos'
CLIENT = storage.Client()

app = Flask(__name__)

@app.route('/health-check', methods=['GET'])
def health_check():
    """Función simple para el healthcheck."""
    return 'OK', 200

@app.route('/', methods=['POST'])
def pubsub_push():
    """Maneja mensajes entrantes de Pub/Sub."""
    envelope = request.get_json()
    if not envelope:
        msg = 'No Pub/Sub message received'
        logging.error(msg)
        return f'Bad Request: {msg}', 400

    if not isinstance(envelope, dict) or 'message' not in envelope:
        msg = 'Invalid Pub/Sub message format'
        logging.error(msg)
        return f'Bad Request: {msg}', 400

    pubsub_message = envelope['message']

    if isinstance(pubsub_message, dict) and 'data' in pubsub_message:
        data = pubsub_message['data']
        message = json.loads(base64.b64decode(data).decode('utf-8').strip())
        logging.info(f'Received message: {message}')
        # Procesa el JSON recibido
        procesar_archivos(message)
        
    return ('', 204)

def procesar_archivos(event):
    """Procesa archivos del bucket de origen y los guarda en el bucket de destino."""
    try:
        logging.basicConfig(level=logging.INFO)
        
        bucket_origen = CLIENT.bucket(BUCKET_ORIGEN)
        bucket_destino = CLIENT.bucket(BUCKET_DESTINO)
        
        file_names = [
            'agency.txt', 'calendar.txt', 'calendar_dates.txt', 'feed_info.txt',
            'frequencies.txt', 'routes.txt', 'shapes.txt', 'stop_times.txt',
            'stops.txt', 'trips.txt'
        ]
        
        for file_name in file_names:
            output_file_name = file_name.replace('.txt', '.csv')
            
            # Verificar si el archivo CSV ya existe
            if archivos_csv_existen(bucket_destino, output_file_name):
                logging.info(f'El archivo CSV {output_file_name} ya existe. Omitiendo procesamiento.')
                continue
            
            # DataFrame por defecto vacío
            combined_df = pd.DataFrame()

            # Iterar sobre las carpetas en el bucket de origen
            for blob in bucket_origen.list_blobs():
                if blob.name.endswith(f'/{file_name}'):
                    df = read_gcs_file(bucket_origen, blob.name)
                    combined_df = armonizar_y_unir_df(combined_df, df)
            
            if not combined_df.empty:
                # Eliminar columnas completamente nulas o con todos nulos
                combined_df = combined_df.dropna(axis=1, how='all')

                # Guardar el DataFrame combinado en el bucket de destino
                guardar_csv(bucket_destino, output_file_name, combined_df)
                logging.info(f'Procesamiento completado para {file_name}')
            else:
                logging.warning(f'No se encontraron archivos para {file_name}')
    except Exception as e:
        logging.error(f"Error en la función procesar_archivos: {str(e)}")
        raise  # Re-lanza la excepción para que Cloud Run la maneje

def archivos_csv_existen(bucket, file_name):
    """Verifica si un archivo CSV ya existe en el bucket de destino."""
    try:
        blob = bucket.blob(file_name)
        return blob.exists()
    except Exception as e:
        logging.error(f"Error comprobando existencia de {file_name}: {str(e)}")
        return False

def read_gcs_file(bucket, blob_name):
    """Lee un archivo desde GCS y lo carga en un DataFrame de pandas."""
    try:
        blob = bucket.blob(blob_name)
        data = blob.download_as_text()
        return pd.read_csv(io.StringIO(data))
    except Exception as e:
        logging.error(f"Error leyendo archivo {blob_name}: {str(e)}")
        return pd.DataFrame()

def guardar_csv(bucket, file_name, df):
    """Guarda un DataFrame como archivo CSV en GCS."""
    try:
        if not df.empty:
            blob = bucket.blob(file_name)
            blob.upload_from_string(df.to_csv(index=False), 'text/csv')
            logging.info(f'Archivo guardado: {file_name}')
        else:
            logging.warning(f'No se guardó {file_name} porque el DataFrame está vacío')
    except Exception as e:
        logging.error(f"Error guardando archivo {file_name}: {str(e)}")

def armonizar_y_unir_df(combined_df, df):
    """Armoniza y une DataFrames columna por columna."""
    try:
        for col in df.columns:
            if col in combined_df.columns:
                # Si la columna ya existe, añade los datos debajo
                combined_df = combined_df.append(df[[col]], ignore_index=True)
            else:
                # Si la columna no existe, añádela al DataFrame combinado con NaNs
                combined_df[col] = pd.NA
                combined_df = combined_df.append(df[[col]], ignore_index=True)
        
        # Asegurar que todas las filas anteriores tienen valores NaN en las nuevas columnas
        combined_df = combined_df.fillna(pd.NA)

        return combined_df
    except Exception as e:
        logging.error(f"Error armonizando y uniendo DataFrames: {str(e)}")
        return combined_df

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
