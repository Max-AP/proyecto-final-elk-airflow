"""
DAG: batch_kpi_resumen
-----------------------
Propósito
    Flujo BATCH del proyecto final. Toma el Data Warehouse estrella que el
    Taller 1 (KNIME) ya dejó cargado en Postgres (dw.fact_juego_metricas +
    dimensiones dim_genero, dim_fecha, dim_desarrollador, dim_publisher,
    dim_juego) y calcula KPIs agregados hacia una tabla resumen
    (dw.kpi_resumen_juegos), lista para consumir desde dashboards o desde
    el DAG de integración batch+streaming (merge_batch_speed).

Por qué esto SÍ es "batch" y no duplica el trabajo de KNIME
    KNIME resolvió la ingesta y el modelado dimensional (ETL inicial).
    Este DAG no vuelve a hacer ese trabajo: opera sobre el resultado ya
    modelado, agregando y resumiendo — es un segundo salto batch, propio
    de Airflow, que se ejecuta de forma programada (1 vez/día).

Tareas
    1. crear_tabla_resumen      -> DDL (CREATE TABLE IF NOT EXISTS)
    2. kpis_por_genero          -> agrega precio/reviews/playtime por género
    3. kpis_por_decada          -> agrega precio/reviews/playtime por década
       (2 y 3 corren en PARALELO: ambas dependen solo de la tabla creada
        y son independientes entre sí)
    4. resumen_ejecucion        -> cuenta filas escritas hoy y lo loguea
       (depende de que 2 y 3 hayan terminado -> fan-in)

Conexión
    Usa la Airflow Connection "postgres_dw" (Admin > Connections), leída
    con BaseHook para no acoplar el DAG a un provider adicional.
"""

from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

POSTGRES_CONN_ID = "postgres_dw"

DEFAULT_ARGS = {
    "owner": "maximo",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def _get_pg_connection():
    """Abre una conexión psycopg2 usando las credenciales guardadas en
    la Airflow Connection 'postgres_dw' (no hardcodeadas en el DAG)."""
    conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    return psycopg2.connect(
        host=conn.host,
        port=conn.port or 5432,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
    )


def crear_tabla_resumen():
    ddl = """
    CREATE TABLE IF NOT EXISTS dw.kpi_resumen_juegos (
        dimension              varchar(20)   NOT NULL,  -- 'genero' o 'decada'
        categoria              varchar(150)  NOT NULL,
        total_juegos           integer,
        precio_promedio        numeric(10,2),
        pct_pos_promedio       numeric(5,2),
        avg_playtime_promedio  integer,
        fecha_calculo          date          NOT NULL,
        PRIMARY KEY (dimension, categoria, fecha_calculo)
    );
    """
    pg = _get_pg_connection()
    try:
        with pg.cursor() as cur:
            cur.execute(ddl)
        pg.commit()
        print("Tabla dw.kpi_resumen_juegos verificada/creada.")
    finally:
        pg.close()


def _calcular_y_cargar(dimension, query_agregacion):
    """Función genérica: corre la agregación y hace upsert en
    dw.kpi_resumen_juegos con dimension='genero' o 'decada'."""
    pg = _get_pg_connection()
    try:
        with pg.cursor() as cur:
            cur.execute(query_agregacion)
            filas = cur.fetchall()

            upsert = """
            INSERT INTO dw.kpi_resumen_juegos
                (dimension, categoria, total_juegos, precio_promedio,
                 pct_pos_promedio, avg_playtime_promedio, fecha_calculo)
            VALUES (%s, %s, %s, %s, %s, %s, CURRENT_DATE)
            ON CONFLICT (dimension, categoria, fecha_calculo)
            DO UPDATE SET
                total_juegos          = EXCLUDED.total_juegos,
                precio_promedio       = EXCLUDED.precio_promedio,
                pct_pos_promedio      = EXCLUDED.pct_pos_promedio,
                avg_playtime_promedio = EXCLUDED.avg_playtime_promedio;
            """
            for categoria, total, precio, pct_pos, playtime in filas:
                cur.execute(
                    upsert,
                    (dimension, categoria, total, precio, pct_pos, playtime),
                )
        pg.commit()
        print(f"[{dimension}] {len(filas)} categorías actualizadas.")
    finally:
        pg.close()


def kpis_por_genero():
    query = """
        SELECT
            g.nombre_genero AS categoria,
            COUNT(*) AS total_juegos,
            ROUND(AVG(f.price), 2) AS precio_promedio,
            ROUND(AVG(f.pct_pos_total), 2) AS pct_pos_promedio,
            ROUND(AVG(f.average_playtime_forever)) AS avg_playtime_promedio
        FROM dw.fact_juego_metricas f
        JOIN dw.dim_genero g ON f.genero_key = g.genero_key
        GROUP BY g.nombre_genero;
    """
    _calcular_y_cargar("genero", query)


def kpis_por_decada():
    query = """
        SELECT
            d.decada AS categoria,
            COUNT(*) AS total_juegos,
            ROUND(AVG(f.price), 2) AS precio_promedio,
            ROUND(AVG(f.pct_pos_total), 2) AS pct_pos_promedio,
            ROUND(AVG(f.average_playtime_forever)) AS avg_playtime_promedio
        FROM dw.fact_juego_metricas f
        JOIN dw.dim_fecha d ON f.fecha_key = d.fecha_key
        GROUP BY d.decada;
    """
    _calcular_y_cargar("decada", query)


def resumen_ejecucion():
    pg = _get_pg_connection()
    try:
        with pg.cursor() as cur:
            cur.execute(
                """
                SELECT dimension, COUNT(*)
                FROM dw.kpi_resumen_juegos
                WHERE fecha_calculo = CURRENT_DATE
                GROUP BY dimension;
                """
            )
            for dimension, total in cur.fetchall():
                print(f"Resumen del día — dimension={dimension}: {total} filas")
    finally:
        pg.close()


with DAG(
    dag_id="batch_kpi_resumen",
    description="Calcula KPIs agregados desde el DW (Taller 1/KNIME) hacia una tabla resumen — flujo BATCH.",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["batch", "dw", "proyecto-final"],
) as dag:

    t1_crear_tabla = PythonOperator(
        task_id="crear_tabla_resumen",
        python_callable=crear_tabla_resumen,
    )

    t2_kpis_genero = PythonOperator(
        task_id="kpis_por_genero",
        python_callable=kpis_por_genero,
    )

    t3_kpis_decada = PythonOperator(
        task_id="kpis_por_decada",
        python_callable=kpis_por_decada,
    )

    t4_resumen = PythonOperator(
        task_id="resumen_ejecucion",
        python_callable=resumen_ejecucion,
    )

    # crear_tabla -> [kpis_por_genero, kpis_por_decada] (paralelo) -> resumen_ejecucion
    t1_crear_tabla >> [t2_kpis_genero, t3_kpis_decada] >> t4_resumen
