# CÓDIGO PARA TRATAR UN JSON COMPLEJO (DATOS DIARIOS) V3 CLOUD
import subprocess
from google.cloud import storage
import pandas as pd
import json
from datetime import datetime
import os

# Configura las credenciales y el proyecto de GCP
client = storage.Client()

# Bucket de origen y destino
bucket_origen_name = 'bcrudo_diarios'
bucket_destino_name = 'bclean_diario'

# Obtén los blobs (archivos) del bucket de origen
bucket_origen = client.get_bucket(bucket_origen_name)
bucket_destino = client.get_bucket(bucket_destino_name)

blobs = bucket_origen.list_blobs()

# Itera sobre los blobs
for blob in blobs:
    # Ruta completa del archivo json
    ruta_json = f'gs://{bucket_origen.name}/{blob.name}'
    
    # Ruta de destino para el archivo a guardar
    nombre_blob = blob.name.replace(".json", "")
    ruta_carpeta_destino = f'gs://{bucket_destino.name}/{nombre_blob}/'
    
    # Verifica si el archivo descomprimido ya existe en el bucket de destino
    prefix = nombre_blob + "/"
    existe_en_destino = False
    
    for blob_destino in bucket_destino.list_blobs(prefix=prefix):
        existe_en_destino = True
        break
    
    if not existe_en_destino:
        # Descargar y procesar el JSON
        json_data = blob.download_as_string()
        data = json.loads(json_data)
        
        # Variables de ejemplo (esto debería adaptarse según tu JSON y tus necesidades)
        route_id = os.path.splitext(os.path.basename(blob.name))[0]
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
        destination_folder = f'{today}/{os.path.splitext(blob.name)[0]}'
        
        shapes_blob = bucket_destino.blob(f'{destination_folder}/shapes.csv')
        shapes_blob.upload_from_string(shapes.to_csv(index=False), 'text/csv')
        
        stops_blob = bucket_destino.blob(f'{destination_folder}/stops.csv')
        stops_blob.upload_from_string(stops.to_csv(index=False), 'text/csv')
        
        routes_blob = bucket_destino.blob(f'{destination_folder}/routes.csv')
        routes_blob.upload_from_string(routes.to_csv(index=False), 'text/csv')
        
        print(f'Archivo {blob.name} procesado y almacenado en {ruta_carpeta_destino}')
    else:
        print(f'El archivo {blob.name} ya está procesado y almacenado en {ruta_carpeta_destino}')
