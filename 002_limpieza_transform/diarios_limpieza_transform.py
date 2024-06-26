# Script antiguo de limpieza de datos diarios
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
bucket_destino_name = 'bclean_diarios'

# Obtén los blobs (archivos) del bucket de origen
bucket_origen = client.bucket(bucket_origen_name)
bucket_destino = client.bucket(bucket_destino_name)

today = datetime.now().strftime('%Y_%m_%d')
#prefix = f'{today}/'
prefix = f'2024_05_26/'

blobs = list(bucket_origen.list_blobs(prefix=prefix))

num_blobs = len(blobs)
print(f'El número de blobs en la subcarpeta es: {num_blobs}')
# Verificando si el número de blobs coincide con los archivos JSON esperados

# Itera sobre los blobs
for blob in blobs:
    blob_name = blob.name
    print(f'entramos al loop con: {blob_name}')
    
    if blob.name.endswith('.json'):
        # Ruta completa del archivo json
        ruta_json = f'gs://{bucket_origen.name}/{blob_name}'
        
        # Ruta de destino para el archivo a guardar
        # bucket / carpeta fecha hoy / nombre archivo / .csv
        nombre_blob = os.path.splitext(os.path.basename(blob_name))[0]
        ruta_carpeta_destino = f'{today}/{nombre_blob}/'

        # Verifica si el archivo con el nombre del blob ya existe en el bucket de destino
        existe_en_destino = False

        for blob_destino in bucket_destino.list_blobs(prefix=ruta_carpeta_destino):
            existe_en_destino = True
            break

        if not existe_en_destino:
            # Descargar y procesar el JSON
            json_data = blob.download_as_string()

            try:
                data = json.loads(json_data)
                print(f'JSON cargado correctamente')

                # Proceso principal
                # variables
                route_id        = os.path.splitext(os.path.basename(blob.name))[0]
                path_id_ida     = str(route_id) + 'I'
                path_id_regreso = str(route_id) + 'R'
                agency_timezone = 'America/Santiago'
                ida_destino     = ''
                regreso_destino = ''

                # inicializar dataframes vacios
                df_negocio = pd.DataFrame()
                df_ida = pd.DataFrame()
                horarios_ida = pd.DataFrame()
                path_ida = pd.DataFrame()
                paraderos_ida = pd.DataFrame()
                df_regreso = pd.DataFrame()
                horarios_regreso = pd.DataFrame()
                path_regreso = pd.DataFrame()
                paraderos_regreso = pd.DataFrame()

                # Procesamiento y comprobación de datos
                if 'negocio' in data and data['negocio']:
                    df_negocio = pd.json_normalize(data['negocio'])
                else: df_negocio = pd.DataFrame()
                if 'ida' in data and data['ida']:
                    df_ida = pd.json_normalize(data['ida'])
                    if 'horarios' in data['ida'] and data['ida']['horarios']:
                        horarios_ida = pd.json_normalize(data['ida'], 'horarios', errors='ignore')
                    else: horarios_ida = pd.DataFrame()
                    if 'path' in data['ida'] and data['ida']['path']:
                        path_ida = pd.json_normalize(data['ida'], 'path', errors='ignore')
                    else: path_ida = pd.DataFrame()
                    if 'paraderos' in data['ida'] and data['ida']['paraderos']:
                        paraderos_ida = pd.json_normalize(data, record_path=['ida', 'paraderos'])
                    else: paraderos_ida = pd.DataFrame()
                else: df_ida = pd.DataFrame()
                if 'regreso' in data and data['regreso']:
                    df_regreso = pd.json_normalize(data['regreso'])
                    if 'horarios' in data['regreso'] and data['regreso']['horarios']:
                        horarios_regreso = pd.json_normalize(data['regreso'], 'horarios', errors='ignore')
                    else: horarios_regreso = pd.DataFrame()
                    if 'path' in data['regreso'] and data['regreso']['path']:
                        path_regreso = pd.json_normalize(data['regreso'], 'path', errors='ignore')
                    else: path_regreso = pd.DataFrame()
                    if 'paraderos' in data['regreso'] and data['regreso']['paraderos']:
                        paraderos_regreso = pd.json_normalize(data, record_path=['regreso', 'paraderos'])
                    else: paraderos_regreso = pd.DataFrame()
                else: df_regreso = pd.DataFrame()

                # Limpieza y transformación de datos
                if 'ida' in data and data['ida']:
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
                    ida_destino = df_ida['ida_destino'].iloc[0]

                if 'regreso' in data and data['regreso']:
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
                    regreso_destino = df_regreso['regreso_destino'].iloc[0]
                
                # Genera los archivos csv finales (combina datasets anteriores, asigna id recorrido)
                shapes = pd.concat([path_ida, path_regreso], axis=0)
                shapes.rename(columns=lambda x: 'shape_' + x, inplace=True)
                shapes.insert(0, 'route_id', [route_id] * len(shapes.shape_id))
                
                stops = pd.concat([paraderos_ida, paraderos_regreso], axis=0)
                stops.rename(columns={'cod': 'stop_cod', 'num': 'stop_num', 'name': 'stop_name', 'comuna': 'stop_comuna', 'type': 'stop_type', 'eje': 'stop_eje', 'distancia': 'stop_distancia'}, inplace=True)
                
                routes = df_negocio.copy()
                routes.rename(columns={'nombre': 'name'}, inplace=True)
                routes.rename(columns=lambda x: 'agency_' + x, inplace=True)
                routes.insert(4, 'agency_timezone', [agency_timezone])
                route_long_name = ida_destino + ' - ' + regreso_destino
                routes.insert(0, 'route_long_name', route_long_name)
                routes.insert(0, 'route_id', [route_id])

                # Guardar los DataFrames como archivos CSV en el bucket de destino
                shapes_csv = shapes.to_csv(index=False)
                shapes_blob_destino = bucket_destino.blob(f'{ruta_carpeta_destino}shapes.csv')
                shapes_blob_destino.upload_from_string(shapes_csv, content_type='text/csv')

                stops_csv = stops.to_csv(index=False)
                stops_blob_destino = bucket_destino.blob(f'{ruta_carpeta_destino}stops.csv')
                stops_blob_destino.upload_from_string(stops_csv, content_type='text/csv')

                routes_csv = routes.to_csv(index=False)
                routes_blob_destino = bucket_destino.blob(f'{ruta_carpeta_destino}routes.csv')
                routes_blob_destino.upload_from_string(routes_csv, content_type='text/csv')

                print(f'{blob_name}: Datos guardados en {ruta_carpeta_destino}shapes.csv')
                print(f'{blob_name}: Datos guardados en {ruta_carpeta_destino}stops.csv')
                print(f'{blob_name}: Datos guardados en {ruta_carpeta_destino}routes.csv')
                
            except json.JSONDecodeError as e:
                print(f'Error al cargar el JSON: {e}')
                continue

        else:
            print(f'El archivo {ruta_carpeta_destino} ya existe en el bucket de destino, omitiendo...')
    else:
        print(f'El archivo {blob_name} no termina en .json, omitiendo...')
