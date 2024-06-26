import os
import json
import base64
import logging
from flask import Flask, request
from google.cloud import storage
import pandas as pd
from datetime import datetime

# Configuración global
BUCKET_ORIGEN = 'bcrudo_diarios'
BUCKET_DESTINO = 'bclean_diarios'
CLIENT = storage.Client()

app = Flask(__name__)

@app.route('/health-check', methods=['GET'])
def health_check():
    """Función simple para el healthcheck."""
    return 'OK', 200

@app.route('/', methods=['POST'])
def pubsub_push():
    envelope = request.get_json()
    if not envelope:
        msg = 'No Pub/Sub message received'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400

    if not isinstance(envelope, dict) or 'message' not in envelope:
        msg = 'Invalid Pub/Sub message format'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400

    pubsub_message = envelope['message']

    if isinstance(pubsub_message, dict) and 'data' in pubsub_message:
        data = pubsub_message['data']
        message = json.loads(base64.b64decode(data).decode('utf-8').strip())
        print(f'Received message: {message}')
        # Aquí puedes procesar el JSON recibido
        procesar_archivos(message)
        
    return ('', 204)

def procesar_archivos(event):
    try:
        logging.basicConfig(level=logging.INFO)
        
        today = datetime.now().strftime('%Y_%m_%d')
        bucket_origen = CLIENT.bucket(BUCKET_ORIGEN)
        bucket_destino = CLIENT.bucket(BUCKET_DESTINO)
        
        blobs = list(bucket_origen.list_blobs(prefix=f'{today}/'))
        logging.info(f'Número de archivos a procesar: {len(blobs)}')
        
        for blob in blobs:
            if not blob.name.endswith('.json'):
                logging.info(f'Omitiendo archivo no JSON: {blob.name}')
                continue
            
            route_id = os.path.splitext(os.path.basename(blob.name))[0]
            ruta_destino = f'{today}/{route_id}/'
            
            # Verificar si los archivos CSV ya existen
            if archivos_csv_existen(bucket_destino, ruta_destino):
                logging.info(f'Los archivos CSV para {route_id} ya existen. Omitiendo procesamiento.')
                continue
            
            try:
                data = json.loads(blob.download_as_string())
                
                shapes, stops, routes = procesar_json(data, route_id)
                
                guardar_csv(bucket_destino, ruta_destino, 'shapes.csv', shapes)
                guardar_csv(bucket_destino, ruta_destino, 'stops.csv', stops)
                guardar_csv(bucket_destino, ruta_destino, 'routes.csv', routes)
                
                logging.info(f'Procesamiento completado para {blob.name}')
            except Exception as e:
                logging.error(f'Error procesando {blob.name}: {str(e)}')
        return 'Procesamiento completado'
    except Exception as e:
        logging.error(f"Error en la función: {str(e)}")
        raise  # Re-lanza la excepción para que Cloud Run la maneje

def procesar_json(data, route_id):
    df_negocio = pd.json_normalize(data.get('negocio', {}))
    df_ida = pd.json_normalize(data.get('ida', {}))
    df_regreso = pd.json_normalize(data.get('regreso', {}))
    
    path_id_ida = f"{route_id}I"
    path_id_regreso = f"{route_id}R"
    
    shapes = pd.concat([
        procesar_direccion(data.get('ida', {}), 'ida', path_id_ida),
        procesar_direccion(data.get('regreso', {}), 'regreso', path_id_regreso)
    ])
    
    stops = pd.concat([
        procesar_paraderos(data.get('ida', {}), 'ida', route_id, path_id_ida),
        procesar_paraderos(data.get('regreso', {}), 'regreso', route_id, path_id_regreso)
    ])
    
    routes = crear_routes(df_negocio, route_id, df_ida, df_regreso)
    
    return shapes, stops, routes

def procesar_direccion(data, direccion, path_id):
    if not data:
        return pd.DataFrame()
    
    path = pd.json_normalize(data.get('path', []))
    if not path.empty:
        path.columns = ['pt_lat', 'pt_lon']
        path['id'] = path_id
        path['pt_sequence'] = range(1, len(path) + 1)
    
    return path

def procesar_paraderos(data, direccion, route_id, shape_id):
    paraderos = pd.json_normalize(data.get('paraderos', []))
    if paraderos.empty:
        return pd.DataFrame()
    
    paraderos = paraderos.rename(columns={
        'stop.stopId': 'stop_id',
        'stop.stopCoordenadaX': 'stop_lat',
        'stop.stopCoordenadaY': 'stop_lon',
        'stop.cod': 'stop_cod',
        'stop.num': 'stop_num',
        'stop.name': 'stop_name',
        'stop.comuna': 'stop_comuna',
        'stop.type': 'stop_type',
        'stop.eje': 'stop_eje',
        'stop.distancia': 'stop_distancia'
    })
    paraderos = paraderos.drop(['servicios', 'pos', 'codSimt'], axis=1, errors='ignore')
    paraderos['shape_id'] = shape_id
    paraderos['route_id'] = route_id
    paraderos[['stop_lat', 'stop_lon']] = paraderos[['stop_lat', 'stop_lon']].astype('float64')
    
    return paraderos

def crear_routes(df_negocio, route_id, df_ida, df_regreso):
    if df_negocio.empty:
        return pd.DataFrame()
    
    routes = df_negocio.rename(columns={'nombre': 'name'})
    routes = routes.add_prefix('agency_')
    routes['agency_timezone'] = 'America/Santiago'
    
    ida_destino = df_ida.get('destino', '').iloc[0] if not df_ida.empty else ''
    regreso_destino = df_regreso.get('destino', '').iloc[0] if not df_regreso.empty else ''
    routes['route_long_name'] = f"{ida_destino} - {regreso_destino}".strip(' - ')
    
    routes['route_id'] = route_id
    return routes

def archivos_csv_existen(bucket, ruta):
    """Verifica si los archivos CSV ya existen en el bucket de destino."""
    archivos_esperados = ['shapes.csv', 'stops.csv', 'routes.csv']
    for archivo in archivos_esperados:
        blob = bucket.blob(f'{ruta}{archivo}')
        if not blob.exists():
            return False
    return True

def guardar_csv(bucket, ruta, nombre_archivo, df):
    if not df.empty:
        blob = bucket.blob(f'{ruta}{nombre_archivo}')
        blob.upload_from_string(df.to_csv(index=False), 'text/csv')
        logging.info(f'Archivo guardado: {ruta}{nombre_archivo}')
    else:
        logging.warning(f'No se guardó {nombre_archivo} porque el DataFrame está vacío')

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
