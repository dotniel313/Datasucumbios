# /--------------------------------------------------------------------
# | @version: 25.8.3 (Final, Completo y Consolidado)
# | @description: Versión final con todas las rutas y correcciones.
# \--------------------------------------------------------------------
import os
import psycopg2
import psycopg2.extras
from flask import Flask, render_template, jsonify
import traceback

app = Flask(__name__)

# --- CONFIGURACIÓN CLAVE ---
# Esto fuerza a Flask a recargar los archivos de plantilla cada vez, evitando la caché.
app.config['TEMPLATES_AUTO_RELOAD'] = True

SUPABASE_CONNECTION_STRING = "postgresql://postgres.yfgcitusasicycngjets:$U._fMEHa6%40gqsD@aws-0-sa-east-1.pooler.supabase.com:6543/postgres"

# =================================================================
# FUNCIONES DE AYUDA (Helpers)
# =================================================================

def get_db_connection():
    return psycopg2.connect(SUPABASE_CONNECTION_STRING)

def execute_query(conn, query, params=None):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]

def get_age_group_sql(column_name):
    return f"""CASE 
        WHEN {column_name} IS NULL THEN 'S/D' WHEN {column_name} <= 9 THEN '0-9' WHEN {column_name} <= 19 THEN '10-19' 
        WHEN {column_name} <= 29 THEN '20-29' WHEN {column_name} <= 39 THEN '30-39' WHEN {column_name} <= 49 THEN '40-49' 
        WHEN {column_name} <= 59 THEN '50-59' ELSE '60+' END"""

def generar_navegacion(parroquia_actual, perfil_activo):
    perfiles = [
        {"url": "territorial", "texto": "Territorial"},
        {"url": "poblacion", "texto": "Población"},
        {"url": "vivienda", "texto": "Vivienda"},
        {"url": "produccion", "texto": "Producción"},
        {"url": "economia", "texto": "Economía"},
        {"url": "salud", "texto": "Salud"},
        {"url": "discapacidad", "texto": "Discapacidad"},
        {"url": "formacion", "texto": "Formación"},

        # --- AÑADE AQUÍ TUS OTROS PERFILES ---
        # Sigue este formato: {"url": "nombre-en-la-url", "texto": "Texto del Botón"}
        # Por ejemplo:
        # {"url": "turismo", "texto": "Turismo"},
        # {"url": "seguridad", "texto": "Seguridad"}
        # -----------------------------------------
    ]
    
    navegacion = []
    for perfil in perfiles:
        navegacion.append({
            "href": f"/{perfil['url']}/{parroquia_actual.replace(' ', '_')}",
            "texto": perfil['texto'],
            "activo": perfil['url'] == perfil_activo
        })
    return navegacion

def generar_navegacion_para_ruta(parroquia, perfil_activo):
    nombre_parroquia_formateado = parroquia.replace("_", " ")
    navegacion_items = generar_navegacion(nombre_parroquia_formateado, perfil_activo)
    return nombre_parroquia_formateado, navegacion_items

# =================================================================
# RUTAS DE PÁGINAS (HTML)
# =================================================================

@app.route("/territorial/<nombre_parroquia>")
def perfil_territorial_page(nombre_parroquia="Aguas_Negras"):
    nombre_formateado, navegacion = generar_navegacion_para_ruta(nombre_parroquia, 'territorial')
    return render_template('perfil_territorial.html', nombre_parroquia=nombre_formateado, navegacion=navegacion)

@app.route("/")
@app.route("/poblacion/<nombre_parroquia>")
def perfil_poblacion_page(nombre_parroquia="Aguas_Negras"):
    nombre_formateado, navegacion = generar_navegacion_para_ruta(nombre_parroquia, 'poblacion')
    return render_template('perfil_poblacion.html', nombre_parroquia=nombre_formateado, navegacion=navegacion)

@app.route("/vivienda/<nombre_parroquia>")
def perfil_vivienda_page(nombre_parroquia):
    nombre_formateado, navegacion = generar_navegacion_para_ruta(nombre_parroquia, 'vivienda')
    return render_template('perfil_vivienda.html', nombre_parroquia=nombre_formateado, navegacion=navegacion)

@app.route("/produccion/<nombre_parroquia>")
def perfil_produccion_page(nombre_parroquia):
    nombre_formateado, navegacion = generar_navegacion_para_ruta(nombre_parroquia, 'produccion')
    return render_template('perfil_produccion.html', nombre_parroquia=nombre_formateado, navegacion=navegacion)

@app.route("/economia/<nombre_parroquia>")
def perfil_economia_page(nombre_parroquia):
    nombre_formateado, navegacion = generar_navegacion_para_ruta(nombre_parroquia, 'economia')
    return render_template('perfil_economia.html', nombre_parroquia=nombre_formateado, navegacion=navegacion)

@app.route('/salud/<nombre_parroquia>')
def perfil_salud_page(nombre_parroquia):
    nombre_formateado, navegacion = generar_navegacion_para_ruta(nombre_parroquia, 'salud')
    return render_template('perfil_salud.html', nombre_parroquia=nombre_formateado, navegacion=navegacion)

@app.route('/discapacidad/<nombre_parroquia>')
def perfil_discapacidad_page(nombre_parroquia):
    nombre_formateado, navegacion = generar_navegacion_para_ruta(nombre_parroquia, 'discapacidad')
    return render_template('perfil_discapacidad.html', nombre_parroquia=nombre_formateado, navegacion=navegacion)

@app.route('/formacion/<nombre_parroquia>')
def perfil_formacion_page(nombre_parroquia):
    nombre_formateado, navegacion = generar_navegacion_para_ruta(nombre_parroquia, 'formacion')
    return render_template('perfil_formacion.html', nombre_parroquia=nombre_formateado, navegacion=navegacion)

# =================================================================
# RUTAS DE API (JSON)
# =================================================================

@app.route('/api/territorio/<parroquia>')
def api_territorio(parroquia):
    conn = get_db_connection()
    try:
        # Consulta SQL corregida: se cambia v.barrio por c.barrio
        query = """
            SELECT
                trim(split_part(trim(v.ubicacion, '()'), ',', 2))::float AS lng,
                split_part(trim(v.ubicacion, '()'), ',', 1)::float AS lat,
                c.barrio,
                c.comunidad
            FROM
                public.viviendas v
            JOIN
                public.ciudadanos c ON c.id_ciudadano = split_part(v.id_ciudadano, ':', 1)
            WHERE
                c.parroquia ILIKE %s
                AND v.ubicacion IS NOT NULL AND v.ubicacion != '' AND v.ubicacion LIKE '%%,%%';
        """
        data = execute_query(conn, query, (parroquia.replace('_', ' '),))
        return jsonify({"viviendas": data})
    except Exception as e:
        print(f"Error en la API de territorio: {e}")
        traceback.print_exc()
        return jsonify({"error": "Error interno del servidor"}), 500
    finally:
        if conn: conn.close()

@app.route("/api/poblacion/<nombre_parroquia>")
def api_poblacion_data(nombre_parroquia):
    conn = get_db_connection()
    try:
        nombre_limpio = nombre_parroquia.replace("_", " ")
        params = (nombre_limpio,)
        where_sql = "WHERE c.parroquia ILIKE %s"
        ciudadanos_query = f"SELECT *, {get_age_group_sql('edad')} as grupo_edad FROM public.ciudadanos c {where_sql}"
        ciudadanos = execute_query(conn, ciudadanos_query, params)
        jefes_query = f"SELECT c.genero, c.estado_civil, c.id_ciudadano FROM public.familias f JOIN public.ciudadanos c ON c.id_ciudadano = split_part(f.id_ciudadano_cabeza, ':', 1) {where_sql}"
        jefes = execute_query(conn, jefes_query, params)
        return jsonify({"ciudadanos": ciudadanos, "jefes": jefes})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": "Error interno del servidor"}), 500
    finally:
        if conn: conn.close()

@app.route("/api/vivienda/<nombre_parroquia>")
def api_vivienda_data(nombre_parroquia):
    conn = get_db_connection()
    try:
        params = (nombre_parroquia.replace("_", " "),)
        viviendas_query = "SELECT v.*, c.comunidad FROM public.viviendas v JOIN public.ciudadanos c ON c.id_ciudadano = split_part(v.id_ciudadano, ':', 1) WHERE c.parroquia ILIKE %s"
        viviendas = execute_query(conn, viviendas_query, params)
        return jsonify({"viviendas": viviendas})
    finally:
        if conn: conn.close()

@app.route("/api/produccion/<nombre_parroquia>")
def api_produccion_data(nombre_parroquia):
    conn = get_db_connection()
    try:
        params = (nombre_parroquia.replace("_", " "),)
        produccion_query = "SELECT p.*, c.comunidad FROM public.produccion p JOIN public.ciudadanos c ON c.id_ciudadano = split_part(p.id_ciudadano, ':', 1) WHERE c.parroquia ILIKE %s"
        unidades = execute_query(conn, produccion_query, params)
        return jsonify({"unidades": unidades})
    finally:
        if conn: conn.close()

@app.route("/api/economia/<nombre_parroquia>")
def api_economia_data(nombre_parroquia):
    conn = get_db_connection()
    try:
        params = (nombre_parroquia.replace("_", " "),)
        ciudadanos_query = "SELECT * FROM public.ciudadanos WHERE parroquia ILIKE %s"
        ciudadanos = execute_query(conn, ciudadanos_query, params)
        return jsonify({"ciudadanos": ciudadanos})
    finally:
        if conn: conn.close()

@app.route('/api/salud/<parroquia>')
def api_salud(parroquia):
    conn = get_db_connection()
    try:
        query = "SELECT sufre_enfermedad_catastrofica AS enfermedad, genero AS sexo, comunidad, edad FROM public.ciudadanos WHERE parroquia ILIKE %s AND sufre_enfermedad_catastrofica IS NOT NULL AND sufre_enfermedad_catastrofica != 'Ninguna'"
        data = execute_query(conn, query, (parroquia.replace('_', ' '),))
        return jsonify({"ciudadanos": data})
    finally:
        if conn: conn.close()

@app.route('/api/discapacidad/<parroquia>')
def api_discapacidad(parroquia):
    conn = get_db_connection()
    try:
        query = "SELECT sufre_discapacidad AS discapacidad, genero AS sexo, comunidad, edad FROM public.ciudadanos WHERE parroquia ILIKE %s AND sufre_discapacidad IS NOT NULL AND sufre_discapacidad != 'No, Ninguna'"
        data = execute_query(conn, query, (parroquia.replace('_', ' '),))
        return jsonify({"ciudadanos": data})
    finally:
        if conn: conn.close()
        
@app.route('/api/formacion/<parroquia>')
def api_formacion(parroquia):
    conn = get_db_connection()
    try:
        query = "SELECT nivel_educativo, comunidad, genero, edad FROM public.ciudadanos WHERE parroquia ILIKE %s"
        data = execute_query(conn, query, (parroquia.replace('_', ' '),))
        return jsonify({"ciudadanos": data})
    finally:
        if conn: conn.close()

# =================================================================
# INICIAR LA APLICACIÓN
# =================================================================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)