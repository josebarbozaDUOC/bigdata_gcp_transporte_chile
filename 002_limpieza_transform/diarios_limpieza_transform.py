# CÓDIGO PARA TRATAR UN JSON COMPLEJO (DATOS DIARIOS) V3 CLOUD
import pandas as pd
import json
from google.cloud import storage
from datetime import datetime
import os
import sys

# Configuración del cliente de Google Cloud Storage
storage_client = storage.Client()

def process_json(bucket_name, source_blob_name, destination_bucket_name):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    json_data = blob.download_as_string()
    data = json.loads(json_data)
    
    # Extraer el nombre del archivo sin la extensión para usarlo como route_id
    route_id = os.path.splitext(os.path.basename(source_blob_name))[0]
    path_id_ida = str(route_id) + 'I'
    path_id_regreso = str(route_id) + 'R'
    agency_timezone = 'America/Santiago'

    # Procesamiento de datos
    df_negocio = pd.json_normalize(data['negocio'])
    df_ida = pd.json_normalize(data['ida'])
    horarios_ida = pd.json_normalize(data['ida'], 'horarios', errors='ignore')
    path_ida = pd.json_normalize(data['ida'], 'path', errors='ignore')
    paraderos_ida = pd.json_normalize(data, record_path=['ida', 'paraderos'])
    df_regreso = pd.json_normalize(data['regreso'])
    horarios_regreso = pd.json_normalize(data['regreso'], 'horarios', errors='ignore')
    path_regreso = pd.json_normalize(data['regreso'], 'path', errors='ignore')
    paraderos_regreso = pd.json_normalize(data, record_path=['regreso', 'paraderos'])

    # Limpieza y transformación de datos
    columnas_a_eliminar = ['horarios', 'path', 'paraderos']
    df_ida = df_ida.drop(columnas_a_eliminar, axis=1)
    df_ida.rename(columns=lambda x: 'ida_' + x, inplace=True)
    horarios_ida = horarios_ida.drop_duplicates()
    path_ida.rename(columns={0: 'pt_lat', 1: 'pt_lon'}, inplace=True)
    path_ida.insert(0, 'id', [path_id_ida] * len(path_ida))
    path_ida['pt_sequence'] = range(1, len(path_ida) + 1)
    columnas_renombrar = {col: col.replace('stop.', '') for col in paraderos_ida.columns if col.startswith('stop.')}
    paraderos_ida.rename(columns=columnas_renombrar, inplace=True)
    paraderos_ida.drop(['servicios', 'pos', 'codSimt'], axis=1, inplace=True)
    paraderos_ida.rename(columns={'stopId': 'stop_id', 'stopCoordenadaX': 'stop_lat', 'stopCoordenadaY': 'stop_lon'}, inplace=True)
    paraderos_ida.insert(0, 'shape_id', [path_id_ida] * len(paraderos_ida))
    paraderos_ida.insert(0, 'route_id', [route_id] * len(paraderos_ida))
    paraderos_ida[['stop_lat', 'stop_lon']] = paraderos_ida[['stop_lat', 'stop_lon']].astype('float64')
    
    df_regreso = df_regreso.drop(columnas_a_eliminar, axis=1)
    df_regreso.rename(columns=lambda x: 'regreso_' + x, inplace=True)
    horarios_regreso = horarios_regreso.drop_duplicates()
    path_regreso.rename(columns={0: 'pt_lat', 1: 'pt_lon'}, inplace=True)
    path_regreso.insert(0, 'id', [path_id_regreso] * len(path_regreso))
    path_regreso['pt_sequence'] = range(1, len(path_regreso) + 1)
    columnas_renombrar = {col: col.replace('stop.', '') for col in paraderos_regreso.columns if col.startswith('stop.')}
    paraderos_regreso.rename(columns=columnas_renombrar, inplace=True)
    paraderos_regreso.drop(['servicios', 'pos', 'codSimt'], axis=1, inplace=True)
    paraderos_regreso.rename(columns={'stopId': 'stop_id', 'stopCoordenadaX': 'stop_lat', 'stopCoordenadaY': 'stop_lon'}, inplace=True)
    paraderos_regreso.insert(0, 'shape_id', [path_id_regreso] * len(paraderos_regreso))
    paraderos_regreso.insert(0, 'route_id', [route_id] * len(paraderos_regreso))
    paraderos_regreso[['stop_lat', 'stop_lon']] = paraderos_regreso[['stop_lat', 'stop_lon']].astype('float64')
    
    # Genera los archivos csv finales
    shapes = pd.concat([path_ida, path_regreso], axis=0)
    shapes.rename(columns=lambda x: 'shape_' + x, inplace=True)
    shapes.insert(0, 'route_id', [route_id] * len(shapes.shape_id))
    
    stops = pd.concat([paraderos_ida, paraderos_regreso], axis=0)
    stops.rename(columns={'cod': 'stop_cod', 'num': 'stop_num', 'name': 'stop_name', 'comuna': 'stop_comuna', 'type': 'stop_type', 'eje': 'stop_eje', 'distancia': 'stop_distancia'}, inplace=True)
    
    routes = df_negocio.copy()
    routes.rename(columns={'nombre': 'name'}, inplace=True)
    routes.rename(columns=lambda x: 'agency_' + x, inplace=True)
    routes.insert(4, 'agency_timezone', [agency_timezone])
    route_long_name = df_ida['ida_destino'] + ' - ' + df_regreso['regreso_destino']
    routes.insert(0, 'route_long_name', route_long_name)
    routes.insert(0, 'route_id', [route_id])
    
    # Guardar los DataFrames como archivos CSV en el bucket de destino
    today = datetime.today().strftime('%Y_%m_%d')
    destination_folder = f'{today}/{os.path.splitext(source_blob_name)[0]}'
    save_to_bucket(destination_bucket_name, destination_folder, 'shapes.csv', shapes)
    save_to_bucket(destination_bucket_name, destination_folder, 'stops.csv', stops)
    save_to_bucket(destination_bucket_name, destination_folder, 'routes.csv', routes)

def save_to_bucket(bucket_name, folder_name, file_name, df):
    """Guarda un DataFrame como archivo CSV en un bucket de GCS."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f'{folder_name}/{file_name}')
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')

def process_all_files(source_bucket_name, destination_bucket_name):
    """Procesa todos los archivos JSON en un bucket."""
    bucket = storage_client.bucket(source_bucket_name)
    blobs = bucket.list_blobs()
    
    for blob in blobs:
        if blob.name.endswith('.json'):
            process_json(source_bucket_name, blob.name, destination_bucket_name)

if __name__ == '__main__':
    source_bucket = sys.argv[1]
    destination_bucket = sys.argv[2]
    process_all_files(source_bucket, destination_bucket)
