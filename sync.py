import gspread
from oauth2client.service_account import ServiceAccountCredentials
import psycopg2
from datetime import datetime
import time

# --- 1. CONFIGURACIÓN ---
GOOGLE_SHEET_NAME = "Base_Capture"
SUPABASE_CONNECTION_STRING = "postgresql://postgres.yfgcitusasicycngjets:$U._fMEHa6%40gqsD@aws-0-sa-east-1.pooler.supabase.com:6543/postgres" 

# --- DICCIONARIOS DE MAPEO DE COLUMNAS ---
COLUMN_MAPPING_CIUDADANOS = {
    "ID_Ciudadano": "id_ciudadano", "Cédula": "cedula", "Nombre y Apellido": "nombre_apellido", "Email": "email",
    "Teléfono": "telefono", "Género": "genero", "Fecha de Nacimiento": "fecha_nacimiento", "Edad": "edad",
    "Estado Civil": "estado_civil", "Autoidentificación Etnica": "etnia", "Nacionalidad": "nacionalidad",
    "Lengua Materna": "lengua_materna", "Nivel": "nivel", "Nivel Educativo": "nivel_educativo",
    "Actividad Económica": "actividad_economica", "¿Sufre usted de alguna discapacidad?": "sufre_discapacidad",
    "% de Discapacidad": "porcentaje_discapacidad", "Posee Carnet de Conadis": "carnet_conadis",
    "¿Sufre usted de alguna enfermedad catastrofica?": "sufre_enfermedad_catastrofica", "Dirección del Domicilio": "direccion_domicilio",
    "Provincia": "provincia", "Cantón": "canton", "Parroquia": "parroquia",
    "¿Pertenece a una organización?": "pertenece_organizacion", "¿Pertenece a una Comunidad?": "pertenece_comunidad",
    "Comunidad": "comunidad", "Sector": "sector", "Barrio": "barrio", "Condición": "condicion",
    "Fecha del Registro": "fecha_registro", "Usuario": "usuario"
}
COLUMN_MAPPING_FAMILIAS = {
    "Id_Familia": "id_familia",
    "Cabeza de Familia": "id_ciudadano_cabeza",
    "Miembro de Familia": "id_ciudadano_miembro",
    "Parentezco": "parentezco",
    "Usuario": "usuario"
}
COLUMN_MAPPING_VIVIENDAS = {
    "ID_Vivienda": "id_vivienda", "Dueño o Responsable(Apellidos)": "id_ciudadano", "Número de Casa": "numero_casa",
    "Foto": "foto", "Tipo vivienda": "tipo_vivienda", "Número de Habitaciones": "numero_habitaciones", "Tenencia": "tenencia",
    "Producción": "produccion", "Superficie Terreno (m2)": "superficie_terreno_m2", "Área de Construccion (m2)": "area_construccion_m2",
    "Fecha de Recidencia": "fecha_residencia", "Ubicación": "ubicacion", "Su vivienda cuenta con Energia Eléctrica": "tiene_energia_electrica",
    "Su vivienda cuenta con Agua Potable": "tiene_agua_potable", "Su vivienda esta equipada con Servicio Higienico": "tiene_servicio_higienico",
    "Su vivienda posee algun sistema de Tratamiento de Aguas residuales": "tiene_tratamiento_aguas",
    "Como gestiona los reciduos solidos": "gestion_residuos", "Cuenta usted con Servicio Telefonico": "tiene_servicio_telefonico",
    "Su vivienda cuenta con conexion a Internet": "tiene_internet", "Usuario": "usuario"
}
COLUMN_MAPPING_PRODUCCION = {
    "ID_Producción": "id_produccion", "ID_Ciudadano": "id_ciudadano", "Unidad Productiva": "unidad_productiva",
    "Tenencia de la Tierra": "tenencia_tierra", "Área total unidad productiva (ha)": "area_total_productiva_ha",
    "Área de Bosque (Ha):": "area_bosque_ha", "Área de Producción (Ha):": "area_produccion_ha", "Tipo producción:": "tipo_produccion",
    "Producto:": "producto", "Área de Cultivo (ha):": "area_cultivo_ha",
    "Cantidad produccion esperada (Tn):": "cantidad_produccion_esperada_tn", "Estado de la producción:": "estado_produccion",
    "Energia para la producción": "energia_produccion", "Tecnificación de La producción": "tecnificacion_produccion",
    "Riego y Drenaje": "riego_drenaje", "A recibdio apoyo de:": "recibio_apoyo", "Nombre del Proyecto:": "nombre_proyecto",
    "Año del proyecto:": "ano_proyecto", "Esta Afiliado al Seguro Campesino": "afiliado_seguro_campesino", "Usuario": "usuario"
}

def clean_value(value, column_name, row_number):
    if not value or (isinstance(value, str) and value.strip() == ''): return None
    if isinstance(value, (int, float)): return value

    if isinstance(value, str):
        value = value.strip()
        date_columns = ["Fecha de Nacimiento", "Fecha del Registro", "Fecha de Recidencia"]
        if column_name in date_columns:
            if value.replace('/','').isdigit():
                try: return datetime.strptime(value, '%d/%m/%Y').strftime('%Y-%m-%d')
                except (ValueError, TypeError): pass
            print(f"\nADVERTENCIA: Fila {row_number}, columna '{column_name}'. Formato de fecha irreconocible ('{value}'). Se guardará como NULO.")
            return None
        
        numeric_columns_with_units = [
            '% de Discapacidad', 'Superficie Terreno (m2)', 'Área de Construccion (m2)', 
            'Área total unidad productiva (ha)', 'Área de Bosque (Ha):', 'Área de Producción (Ha):', 
            'Área de Cultivo (ha):', 'Cantidad produccion esperada (Tn):'
        ]
        if column_name in numeric_columns_with_units:
            try: return float(''.join(filter(lambda x: x.isdigit() or x in ['.', ','], value)).replace(',', '.'))
            except (ValueError, TypeError):
                print(f"\nADVERTENCIA: Fila {row_number}, columna '{column_name}'. Se encontró TEXTO ('{value}') en columna numérica. Se guardará como NULO.")
                return None
        return value
    return None

def sync_table_in_batches(conn, sheet, table_name, column_mapping, id_column_db, batch_size=500):
    print(f"\nIniciando sincronización de '{table_name}' en lotes de {batch_size} filas...")
    try:
        headers = sheet.row_values(1)
        sheet_id_column = [key for key, value in column_mapping.items() if value == id_column_db][0]
        db_columns = list(column_mapping.values())
        insert_sql = f""" INSERT INTO {table_name} ({", ".join(db_columns)}) VALUES ({", ".join(["%s"] * len(db_columns))}) ON CONFLICT ({id_column_db}) DO UPDATE SET {", ".join([f'{col} = EXCLUDED.{col}' for col in db_columns if col != id_column_db])}; """
        
        start_row = 2
        total_rows_processed = 0
        while True:
            print(f"Procesando lote para '{table_name}' desde la fila {start_row}...")
            rows_values = sheet.get(f'A{start_row}:AZ{start_row + batch_size - 1}')
            if not rows_values: break
            
            data_batch = [dict(zip(headers, row)) for row in rows_values if any(row)]
            if not data_batch: break

            with conn.cursor() as cur:
                for i, row_data in enumerate(data_batch):
                    current_row_num = start_row + i
                    if not row_data.get(sheet_id_column):
                        print(f"\nADVERTENCIA: Se omitió la fila {current_row_num} porque su ID ('{sheet_id_column}') está vacío.")
                        continue
                    
                    values = [clean_value(row_data.get(sheet_col), sheet_col, current_row_num) for sheet_col in column_mapping.keys()]
                    cur.execute(insert_sql, tuple(values))
            
            conn.commit()
            total_rows_processed += len(data_batch)
            print(f"Lote hasta la fila {start_row + len(data_batch) - 1} sincronizado. Total: {total_rows_processed} filas.")
            start_row += batch_size
            time.sleep(1)
            
    except Exception as e:
        print(f"\nOcurrió un error en la sincronización de '{table_name}': {e}")
        if conn: conn.rollback()
    
    print(f"Sincronización de '{table_name}' completada.")

def main():
    print("--- INICIANDO PRUEBA DE SINCRONIZACIÓN SOLO PARA 'viviendas' ---")
    conn = None
    try:
        conn = psycopg2.connect(SUPABASE_CONNECTION_STRING)
        print("Conexión a la base de datos exitosa.")

        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open(GOOGLE_SHEET_NAME)
        print("Conexión a Google Sheets exitosa.")
        
        # Sincronizamos solo la tabla de viviendas para encontrar el error
        sync_table_in_batches(conn, spreadsheet.worksheet("t_vivienda"), "viviendas", COLUMN_MAPPING_VIVIENDAS, "id_vivienda")

    except gspread.exceptions.WorksheetNotFound as e:
        print(f"Error FATAL: No se encontró una hoja de cálculo. Revisa el nombre: {e}")
    except Exception as e:
        print(f"\nOcurrió un error en el proceso principal: {e}")
    finally:
        if conn is not None:
            conn.close()
        print("\n--- PRUEBA FINALIZADA ---")


if __name__ == '__main__':
    main()