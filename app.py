# /--------------------------------------------------------------------
# | @version: 20.1.0 (Completa, Verificada y Funcional)
# | @date: 2025-07-28
# | @description: Versión final con la lógica completa y correcta
# |              para los cuatro perfiles, con funciones de ayuda globales.
# \--------------------------------------------------------------------
import psycopg2
import psycopg2.extras
from flask import Flask, render_template, jsonify, request
import traceback
from collections import Counter, defaultdict

app = Flask(__name__)
SUPABASE_CONNECTION_STRING = "postgresql://postgres.yfgcitusasicycngjets:$U._fMEHa6%40gqsD@aws-0-sa-east-1.pooler.supabase.com:6543/postgres"

# --- FUNCIONES DE AYUDA GLOBALES ---
def get_db_connection():
    return psycopg2.connect(SUPABASE_CONNECTION_STRING)

def get_age_group_sql(column_name):
    return f"""CASE WHEN {column_name} IS NULL THEN 'S/D' WHEN {column_name} <= 9 THEN '0-9' WHEN {column_name} <= 19 THEN '10-19' WHEN {column_name} <= 29 THEN '20-29' WHEN {column_name} <= 39 THEN '30-39' WHEN {column_name} <= 49 THEN '40-49' WHEN {column_name} <= 59 THEN '50-59' ELSE '60+' END"""

def pivot_data(data_list, cat_field, series_field):
    if not data_list: return {'categories': [], 'series': []}
    categories = sorted(list(set(c[cat_field] for c in data_list if c[cat_field])))
    series_names = sorted(list(set(c[series_field] for c in data_list if c[series_field])))
    data_map = defaultdict(lambda: defaultdict(int))
    for item in data_list:
        if item.get(cat_field) and item.get(series_field):
            data_map[item[cat_field]][item[series_field]] += 1
    series_data = []
    for s_name in series_names: series_data.append({'name': s_name, 'data': [data_map[cat][s_name] for cat in categories]})
    return {'categories': categories, 'series': series_data}

def generar_navegacion(nombre_parroquia):
    slug = nombre_parroquia.replace(" ", "_")
    return [{"href": f"/poblacion/{slug}", "texto": "Población y Diversidad"}, {"href": f"/economia/{slug}", "texto": "Economía"}, {"href": f"/produccion/{slug}", "texto": "Producción Rural"}, {"href": f"/vivienda/{slug}", "texto": "Vivienda y Servicios"}]

def build_dynamic_where_clause(filters, mapping):
    where_clauses, params = [], []
    for key, value in filters.items():
        if value:
            db_field = key.replace('filtro_', '')
            if db_field == 'grupo_edad':
                where_clauses.append(f"({get_age_group_sql('c.edad')}) = %s")
                params.append(value)
            else:
                table_alias = mapping.get(db_field)
                if table_alias:
                    where_clauses.append(f"{table_alias}.{db_field} ILIKE %s")
                    params.append(value)
    return " AND ".join(where_clauses), params

def execute_query(conn, query, params=None):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]

# --- RUTAS DE PÁGINAS ---
@app.route("/")
@app.route("/poblacion/<nombre_parroquia>")
def perfil_poblacion_page(nombre_parroquia="Aguas_Negras"): return render_template('perfil_poblacion.html', nombre_parroquia=nombre_parroquia.replace("_", " "), navegacion=generar_navegacion(nombre_parroquia.replace("_", " ")))
@app.route("/economia/<nombre_parroquia>")
def perfil_economia_page(nombre_parroquia): return render_template('perfil_economia.html', nombre_parroquia=nombre_parroquia.replace("_", " "), navegacion=generar_navegacion(nombre_parroquia.replace("_", " ")))
@app.route("/produccion/<nombre_parroquia>")
def perfil_produccion_page(nombre_parroquia): return render_template('perfil_produccion.html', nombre_parroquia=nombre_parroquia.replace("_", " "), navegacion=generar_navegacion(nombre_parroquia.replace("_", " ")))
@app.route("/vivienda/<nombre_parroquia>")
def perfil_vivienda_page(nombre_parroquia): return render_template('perfil_vivienda.html', nombre_parroquia=nombre_parroquia.replace("_", " "), navegacion=generar_navegacion(nombre_parroquia.replace("_", " ")))

# --- LÓGICA DE APIs ---

@app.route("/api/poblacion/<nombre_parroquia>")
def api_poblacion_data(nombre_parroquia):
    try:
        conn = get_db_connection()
        nombre_limpio = nombre_parroquia.replace("_", " ")
        where_sql = f"WHERE c.parroquia ILIKE %s"
        params = (nombre_limpio,)
        ciudadanos = execute_query(conn, f"SELECT *, INITCAP(TRIM(nivel)) as nivel_normalizado, {get_age_group_sql('edad')} as grupo_edad FROM public.ciudadanos c {where_sql}", params)
        jefes = execute_query(conn, f"SELECT c.genero, c.estado_civil, c.id_ciudadano FROM public.familias f JOIN public.ciudadanos c ON c.id_ciudadano = split_part(f.id_ciudadano_cabeza, ':', 1) {where_sql}", params)
        return jsonify({"ciudadanos": ciudadanos, "jefes": jefes})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        if 'conn' in locals() and conn: conn.close()

@app.route("/api/economia/<nombre_parroquia>")
def api_economia_data(nombre_parroquia):
    try:
        conn = get_db_connection()
        nombre_limpio = nombre_parroquia.replace("_", " ")
        where_sql = f"WHERE c.parroquia ILIKE %s"
        params = (nombre_limpio,)
        ciudadanos = execute_query(conn, f"SELECT * FROM public.ciudadanos c {where_sql}", params)
        return jsonify({"ciudadanos": ciudadanos})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        if 'conn' in locals() and conn: conn.close()

@app.route("/api/produccion/<nombre_parroquia>")
def api_produccion_data(nombre_parroquia):
    try:
        conn = get_db_connection()
        nombre_limpio = nombre_parroquia.replace("_", " ")
        base_from = "FROM public.produccion p JOIN public.ciudadanos c ON split_part(p.id_ciudadano, ':', 1) = c.id_ciudadano"
        where_sql = f"WHERE c.parroquia ILIKE %s"
        params = (nombre_limpio,)
        unidades = execute_query(conn, f"SELECT * {base_from} {where_sql}", params)
        return jsonify({"unidades": unidades})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        if 'conn' in locals() and conn: conn.close()

@app.route("/api/vivienda/<nombre_parroquia>")
def api_vivienda_data(nombre_parroquia):
    try:
        conn = get_db_connection()
        nombre_limpio = nombre_parroquia.replace("_", " ")
        base_from = "FROM public.viviendas v JOIN public.ciudadanos c ON split_part(v.id_ciudadano, ':', 1) = c.id_ciudadano"
        where_sql = f"WHERE c.parroquia ILIKE %s"
        params = (nombre_limpio,)
        viviendas = execute_query(conn, f"SELECT * {base_from} {where_sql}", params)
        return jsonify({"viviendas": viviendas})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        if 'conn' in locals() and conn: conn.close()

if __name__ == '__main__':
    app.run(debug=True, port=5000)