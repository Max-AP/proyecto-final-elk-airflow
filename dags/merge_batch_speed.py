"""
DAG: merge_batch_speed
------------------------
Propósito
    Es el corazón de la arquitectura Lambda del proyecto: combina la capa
    de VELOCIDAD (eventos near real-time ya indexados en Elasticsearch por
    Logstash, provenientes de MySQL y MongoDB) con la capa BATCH (catálogo
    de juegos modelado en el Data Warehouse de Postgres: dim_juego,
    dim_genero, fact_juego_metricas).

    El resultado es un índice nuevo en Elasticsearch ("actividad_enriquecida-*")
    que cualquier dashboard de Kibana puede consumir: para cada appid con
    actividad reciente, muestra cuántos eventos tuvo en los últimos N
    minutos JUNTO con sus atributos de catálogo (nombre, género, precio,
    metacritic_score) — algo que ni el flujo batch ni el streaming pueden
    dar por separado.

Tareas
    1. extraer_actividad_streaming  -> agregación en ES (terms agg por
       appid) sobre las últimas VENTANA_MINUTOS, para los índices
       eventos_juegos-* (MySQL) y eventos_foro-* (MongoDB)
    2. enriquecer_con_catalogo      -> por cada appid con actividad,
       consulta Postgres (fact_juego_metricas + dim_juego + dim_genero)
       y arma el documento combinado
    3. cargar_a_elasticsearch       -> bulk insert del resultado combinado
       al índice actividad_enriquecida-YYYY.MM.dd

Conexiones / configuración
    - Elasticsearch: host tomado de la Airflow Variable "es_host"
      (default: elasticsearch-dev:9200 — no requiere credenciales porque
      xpack.security.enabled=false en este entorno de desarrollo).
    - Postgres: Airflow Connection "postgres_dw" (la misma que usa
      batch_kpi_resumen).

Frecuencia: cada 15 minutos (más lento que el polling de Logstash de
10s/schedule, pero suficiente para mostrar "near real-time" a nivel de
negocio sin sobrecargar Postgres con consultas repetidas).
"""

from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

POSTGRES_CONN_ID = "postgres_dw"
VENTANA_MINUTOS = 5

DEFAULT_ARGS = {
    "owner": "maximo",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def _get_es_client():
    es_host = Variable.get("es_host", default_var="elasticsearch-dev:9200")
    return Elasticsearch(hosts=[f"http://{es_host}"])


def _get_pg_connection():
    conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    return psycopg2.connect(
        host=conn.host,
        port=conn.port or 5432,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
    )


def extraer_actividad_streaming(ti):
    """Cuenta eventos por appid en los últimos VENTANA_MINUTOS, separado
    por fuente (MySQL vs MongoDB), y publica el resultado combinado
    por XCom para la siguiente tarea."""
    es = _get_es_client()
    desde = (datetime.utcnow() - timedelta(minutes=VENTANA_MINUTOS)).isoformat()

    agg_query = {
        "size": 0,
        "query": {"range": {"@timestamp": {"gte": desde}}},
        "aggs": {"por_appid": {"terms": {"field": "appid", "size": 100}}},
    }

    actividad = {}  # {appid: {"eventos_mysql": n, "eventos_foro": n}}

    try:
        res_mysql = es.search(index="eventos_juegos-*", body=agg_query)
        for bucket in res_mysql["aggregations"]["por_appid"]["buckets"]:
            appid = bucket["key"]
            actividad.setdefault(appid, {"eventos_mysql": 0, "eventos_foro": 0})
            actividad[appid]["eventos_mysql"] = bucket["doc_count"]
    except Exception as e:
        print(f"Aviso: no se pudo leer eventos_juegos-* ({e}). Se continúa igual.")

    try:
        res_foro = es.search(index="eventos_foro-*", body=agg_query)
        for bucket in res_foro["aggregations"]["por_appid"]["buckets"]:
            appid = bucket["key"]
            actividad.setdefault(appid, {"eventos_mysql": 0, "eventos_foro": 0})
            actividad[appid]["eventos_foro"] = bucket["doc_count"]
    except Exception as e:
        print(f"Aviso: no se pudo leer eventos_foro-* ({e}). Se continúa igual.")

    print(f"Appids con actividad en los últimos {VENTANA_MINUTOS} min: {len(actividad)}")
    ti.xcom_push(key="actividad", value=actividad)


def enriquecer_con_catalogo(ti):
    """Toma los appids con actividad y les pega los atributos de catálogo
    del Data Warehouse (nombre, género, precio, metacritic_score)."""
    actividad = ti.xcom_pull(task_ids="extraer_actividad_streaming", key="actividad")

    if not actividad:
        print("No hay actividad reciente; nada que enriquecer.")
        ti.xcom_push(key="documentos", value=[])
        return

    appids_str = list(actividad.keys())
    appids = [int(x) for x in appids_str]

    pg = _get_pg_connection()
    documentos = []
    try:
        with pg.cursor() as cur:
            cur.execute(
                """
                SELECT j.appid,
                       j.name,
                       g.nombre_genero,
                       f.price,
                       f.metacritic_score,
                       f.pct_pos_total
                FROM dw.dim_juego j
                         JOIN dw.fact_juego_metricas f ON f.juego_key = j.juego_key
                         JOIN dw.dim_genero g ON f.genero_key = g.genero_key
                WHERE j.appid = ANY (%s);
                """,
                (appids,),
            )
            catalogo = {
                row[0]: {
                    "nombre_juego": row[1],
                    "genero": row[2],
                    "price": float(row[3]) if row[3] is not None else None,
                    "metacritic_score": row[4],
                    "pct_pos_total": row[5],
                }
                for row in cur.fetchall()
            }
    finally:
        pg.close()

    ahora = datetime.utcnow().isoformat()
    for appid, contadores in actividad.items():
        info_catalogo = catalogo.get(appid, {})
        documentos.append(
            {
                "appid": appid,
                "eventos_mysql_ultimos_min": contadores["eventos_mysql"],
                "eventos_foro_ultimos_min": contadores["eventos_foro"],
                "ventana_minutos": VENTANA_MINUTOS,
                "@timestamp": ahora,
                **info_catalogo,
            }
        )

    print(f"{len(documentos)} documentos enriquecidos listos para indexar.")
    ti.xcom_push(key="documentos", value=documentos)


def cargar_a_elasticsearch(ti):
    documentos = ti.xcom_pull(task_ids="enriquecer_con_catalogo", key="documentos")
    if not documentos:
        print("Nada que cargar en esta corrida.")
        return

    es = _get_es_client()
    fecha = datetime.utcnow().strftime("%Y.%m.%d")
    indice = f"actividad_enriquecida-{fecha}"

    acciones = [{"_index": indice, "_source": doc} for doc in documentos]
    exitosos, errores = bulk(es, acciones, raise_on_error=False)
    print(f"Indexados {exitosos} documentos en {indice}. Errores: {len(errores)}")


with DAG(
        dag_id="merge_batch_speed",
        description="Combina actividad streaming (Elasticsearch) con catálogo batch (Postgres DW) — capa de integración Lambda.",
        default_args=DEFAULT_ARGS,
        schedule_interval=timedelta(minutes=5),
        start_date=datetime(2026, 9, 1),
        catchup=False,
        tags=["streaming", "batch", "integracion", "proyecto-final"],
) as dag:
    t1_extraer = PythonOperator(
        task_id="extraer_actividad_streaming",
        python_callable=extraer_actividad_streaming,
    )

    t2_enriquecer = PythonOperator(
        task_id="enriquecer_con_catalogo",
        python_callable=enriquecer_con_catalogo,
    )

    t3_cargar = PythonOperator(
        task_id="cargar_a_elasticsearch",
        python_callable=cargar_a_elasticsearch,
    )

    t1_extraer >> t2_enriquecer >> t3_cargar
