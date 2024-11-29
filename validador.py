import struct
import mysql.connector
from datetime import datetime, timedelta
import pytz
import psycopg2


def extract_fields(response_data,resource_data,spans_data): 
   
     # Conexión a la base de datos MySQL
    #connection = mysql.connector.connect(
    #    host="localhost",  
    #    user="root",  
    #    password="", 
    #    database="studentdb"  
    #)

   #conn_params = {
    #"dbname": "studentdb",
    #"user": "root",
    #"host": "localhost",
    #"port": 5432
    #}

    conn_params = {
        "dbname": "studentdb_p1fi",
        "user": "root",
        "host": "dpg-ct326o1u0jms7392g8bg-a.oregon-postgres.render.com",
        "port": 5432,
        "password": "EWqGSNAsmPLWlC6CH5Zn0SlHlCk19YMg"
        }



    #cursor = connection.cursor()

    # Construir la respuesta con la estructura
    telemetry_sdk_language = resource_data['telemetry.sdk.language']
    telemetry_sdk_name = resource_data['telemetry.sdk.name']
    telemetry_sdk_version = resource_data['telemetry.sdk.version']
    service_name = resource_data['service.name']
    span = spans_data[0]
    print('span',span)  
    
    trace_id = span['trace_id']
    span_id = span['span_id']
    flags = span['flags']
    name = span['name']
    start_time_unix_nano = span['start_time_unix_nano']
    end_time_unix_nano = span['end_time_unix_nano']
    start_time = parse_unix_nano_to_datetime(span['start_time_unix_nano'])
    end_time = parse_unix_nano_to_datetime(span['end_time_unix_nano'])
    request_type = list(span['attributes'].keys())[0]
    request_value = span['attributes'][request_type]
    
    
    duration_nano = end_time_unix_nano - start_time_unix_nano
    duration_ms = duration_nano / 1_000_000
        
    start_time_unix_nano = parse_unix_nano_to_datetime(span['start_time_unix_nano'])
    end_time_unix_nano = parse_unix_nano_to_datetime(span['end_time_unix_nano'])
    #insert_query = "INSERT INTO telemetry_data (telemetry_sdk_language, telemetry_sdk_name, telemetry_sdk_version, service_name, trace_id, span_id, flags, name, start_time_unix_nano, end_time_unix_nano, request_type, request_value, duration_ms) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    
    # Datos insertar
    data = (telemetry_sdk_language,telemetry_sdk_name,telemetry_sdk_version,service_name,trace_id,span_id,flags,name,start_time_unix_nano,end_time_unix_nano,request_type,request_value,duration_ms)
        
    #try:
        #cursor.execute(insert_query, data)
        #connection.commit()
        #cursor.close()
        #connection.close()
        #print("Traza insertada correctamente.")
    
    #except mysql.connector.Error as err:
        #print(f"Error: {err}") 


    try:
        # Conexión
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cursor:
                query = """
                INSERT INTO telemetry_data (
                    telemetry_sdk_language, telemetry_sdk_name, telemetry_sdk_version,
                    service_name, trace_id, span_id, flags, name,
                    start_time_unix_nano, end_time_unix_nano,
                    request_type, request_value, duration_ms
                ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                cursor.execute(query, data)
                conn.commit()
                print("Datos insertados correctamente.")
    except Exception as e:
        print(f"Error al insertar datos: {e}")
    

# Función para convertir nanosegundos a DateTime
def parse_unix_nano_to_datetime(nano):
    # Convertir los nanosegundos a segundos dividiendo entre 1e9 (mil millones)
     # Convertir nanosegundos a segundos
    seconds = nano / 1e9
    
    # Crear el objeto datetime
    utc_time = datetime(1970, 1, 1) + timedelta(seconds=seconds)
    
    # Definir la zona horaria de Colombia
    colombia_tz = pytz.timezone('America/Bogota')
    
    # Convertir el tiempo UTC a la zona horaria de Colombia
    colombia_time = utc_time.replace(tzinfo=pytz.utc).astimezone(colombia_tz)
    
    #return datetime.utcfromtimestamp(nano / 1e9)
    return colombia_time


