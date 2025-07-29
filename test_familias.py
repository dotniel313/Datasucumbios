import gspread
from oauth2client.service_account import ServiceAccountCredentials
import psycopg2

# --- CONFIGURACIÓN ---
GOOGLE_SHEET_NAME = "Base_Capture"
SUPABASE_CONNECTION_STRING = "postgresql://postgres:$U._fMEHa6%40gqsD@db.yfgcitusasicycngjets.supabase.co:5432/postgres"

COLUMN_MAPPING_FAMILIAS = {
    "Id_Familia": "id_familia",  # <-- Corregido con 'F' mayúscula
    "Cabeza de Familia": "id_ciudadano_cabeza",
    "Miembro de Familia": "id_ciudadano_miembro",
    "Parentezco": "parentezco",
    "Usuario": "usuario"
}

def sync_familias_table():
    sheet_data = None
    try:
        print("--- Conectando a Google Sheets para leer la hoja 't_ciudadano_familia'... ---")
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open(GOOGLE_SHEET_NAME)
        sheet = spreadsheet.worksheet("t_ciudadano_familia")
        
        all_values = sheet.get_all_values()
        
        if not all_values or len(all_values) < 2:
            print("La hoja 't_ciudadano_familia' está vacía o no tiene datos.")
            return

        headers = all_values[0]
        
        # --- LÍNEA DE DIAGNÓSTICO CLAVE ---
        print("\n--- ¡ATENCIÓN! Encabezados detectados por el script: ---")
        print(headers)
        print("--------------------------------------------------\n")
        # --- FIN DE DIAGNÓSTICO ---
        
        data_rows = all_values[1:]
        sheet_data = [dict(zip(headers, row)) for row in data_rows]
        print(f"Se procesaron {len(sheet_data)} filas de la hoja.")

    except Exception as e:
        print(f"Error FATAL al leer la hoja 't_ciudadano_familia': {e}")
        return

    # Si la lectura fue exitosa, el script terminará aquí.
    # No intentaremos conectar a la base de datos en esta prueba.
    print("Prueba de lectura finalizada. Revisa los encabezados impresos arriba.")


if __name__ == '__main__':
    sync_familias_table()