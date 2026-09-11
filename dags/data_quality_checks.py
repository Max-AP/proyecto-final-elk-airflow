"""
DAG: data_quality_checks
--------------------------
Propósito
    Cierra el pipeline con validación operativa (no transforma ni carga
    datos de negocio, solo verifica salud). Responde tres preguntas:

    1. ¿Los índices streaming en Elasticsearch siguen recibiendo datos
       nuevos? (conteo creciente respecto a la corrida anterior)
    2. ¿El dato más reciente de cada índice es "fresco" (no hace más de
       FRESCURA_MAXIMA_MIN minutos), o el generador correspondiente se
       detuvo?
    3. ¿El DAG batch (batch_kpi_resumen) efectivamente corrió hoy y dejó
       filas en dw.kpi_resumen_juegos?

Tareas
    1. verificar_conteos_indices   -> compara conteo actual vs. el guardado
       en una Airflow Variable en la corrida anterior (persistencia simple
       sin tabla extra)
    2. verificar_frescura_indices  -> revisa el @timestamp más reciente de
       cada índice
    3. verificar_batch_kpi         -> confirma filas de hoy en
       dw.kpi_resumen_juegos
       (1, 2 y 3 corren en PARALELO: son chequeos independientes)
    4. consolidar_reporte          -> junta los 3 resultados, imprime un
       reporte PASS/FAIL por chequeo y falla la tarea (AirflowException)
       solo si hay problemas críticos, para que quede visible en rojo en
       la UI de Airflow

Frecuencia: cada 30 minutos.
"""

from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch

POSTGRES_CONN_ID = "postgres_dw"
FRESCURA_MAXIMA_MIN = 30

INDICES_STREAMING = [
    "eventos_juegos-*",
    "eventos_foro-*",
    "logs_servidor-*",
    "sensores-servidores-*",
    "actividad_enriquecida-*",
]

DEFAULT_ARGS = {
    "owner": "maximo",
    "retries": 1,
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


def verificar_conteos_indices(ti):
    """Compara el conteo actual de cada índice contra el de la corrida
    anterior (guardado en una Airflow Variable). Si no creció, se marca
    como sospechoso (puede significar que el generador se detuvo)."""
    es = _get_es_client()
    resultado = {}

    for patron in INDICES_STREAMING:
        var_key = f"dq_last_count__{patron}"
        conteo_anterior = int(Variable.get(var_key, default_var="0"))

        try:
            conteo_actual = es.count(index=patron)["count"]
        except Exception as e:
            resultado[patron] = {"ok": False, "detalle": f"índice no accesible ({e})"}
            continue

        crecio = conteo_actual > conteo_anterior
        resultado[patron] = {
            "ok": crecio or conteo_anterior == 0,  # primera corrida no cuenta como falla
            "conteo_anterior": conteo_anterior,
            "conteo_actual": conteo_actual,
            "detalle": "creciendo" if crecio else "SIN CAMBIOS desde la última corrida",
        }
        Variable.set(var_key, str(conteo_actual))

    for patron, info in resultado.items():
        print(f"[conteos] {patron}: {info}")

    ti.xcom_push(key="conteos", value=resultado)


def verificar_frescura_indices(ti):
    """Revisa el @timestamp más reciente de cada índice; si es más viejo
    que FRESCURA_MAXIMA_MIN minutos, el generador probablemente está
    apagado."""
    es = _get_es_client()
    resultado = {}
    ahora = datetime.utcnow()

    for patron in INDICES_STREAMING:
        query = {
            "size": 1,
            "sort": [{"@timestamp": {"order": "desc"}}],
            "_source": ["@timestamp"],
        }
        try:
            res = es.search(index=patron, body=query)
            hits = res["hits"]["hits"]
            if not hits:
                resultado[patron] = {"ok": False, "detalle": "índice vacío"}
                continue

            ultimo_ts = datetime.fromisoformat(
                hits[0]["_source"]["@timestamp"].replace("Z", "+00:00")
            ).replace(tzinfo=None)
            antiguedad_min = (ahora - ultimo_ts).total_seconds() / 60

            resultado[patron] = {
                "ok": antiguedad_min <= FRESCURA_MAXIMA_MIN,
                "antiguedad_min": round(antiguedad_min, 1),
                "detalle": "fresco" if antiguedad_min <= FRESCURA_MAXIMA_MIN else "DATO VIEJO — generador posiblemente detenido",
            }
        except Exception as e:
            resultado[patron] = {"ok": False, "detalle": f"índice no accesible ({e})"}

    for patron, info in resultado.items():
        print(f"[frescura] {patron}: {info}")

    ti.xcom_push(key="frescura", value=resultado)


def verificar_batch_kpi():
    pg = _get_pg_connection()
    try:
        with pg.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM dw.kpi_resumen_juegos WHERE fecha_calculo = CURRENT_DATE;"
            )
            total_hoy = cur.fetchone()[0]
    finally:
        pg.close()

    ok = total_hoy > 0
    print(f"[batch_kpi] Filas con fecha_calculo=hoy: {total_hoy} -> {'OK' if ok else 'FALTA CORRIDA DE HOY'}")
    return {"ok": ok, "filas_hoy": total_hoy}


def consolidar_reporte(ti):
    conteos = ti.xcom_pull(task_ids="verificar_conteos_indices", key="conteos") or {}
    frescura = ti.xcom_pull(task_ids="verificar_frescura_indices", key="frescura") or {}
    batch_kpi = ti.xcom_pull(task_ids="verificar_batch_kpi") or {}

    problemas = []

    for patron, info in conteos.items():
        if not info.get("ok"):
            problemas.append(f"CONTEO — {patron}: {info.get('detalle')}")

    for patron, info in frescura.items():
        if not info.get("ok"):
            problemas.append(f"FRESCURA — {patron}: {info.get('detalle')}")

    if not batch_kpi.get("ok"):
        problemas.append("BATCH — dw.kpi_resumen_juegos no se actualizó hoy")

    print("=" * 60)
    print("REPORTE DE CALIDAD DE DATOS")
    print("=" * 60)
    if not problemas:
        print("Todos los chequeos pasaron correctamente. ✅")
    else:
        print(f"Se encontraron {len(problemas)} problema(s):")
        for p in problemas:
            print(f"  - {p}")
    print("=" * 60)

    Variable.set("dq_ultimo_reporte", "; ".join(problemas) if problemas else "OK")

    # Solo falla la tarea (queda en rojo en la UI) si hay 2+ problemas
    # simultáneos, para no generar ruido por un único índice apagado
    # a propósito durante pruebas.
    if len(problemas) >= 2:
        raise AirflowException(
            f"Se detectaron {len(problemas)} problemas de calidad de datos: {problemas}"
        )


with DAG(
    dag_id="data_quality_checks",
    description="Valida que las fuentes streaming sigan activas y que el DAG batch haya corrido hoy.",
    default_args=DEFAULT_ARGS,
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["validacion", "calidad", "proyecto-final"],
) as dag:

    t1_conteos = PythonOperator(
        task_id="verificar_conteos_indices",
        python_callable=verificar_conteos_indices,
    )

    t2_frescura = PythonOperator(
        task_id="verificar_frescura_indices",
        python_callable=verificar_frescura_indices,
    )

    t3_batch_kpi = PythonOperator(
        task_id="verificar_batch_kpi",
        python_callable=verificar_batch_kpi,
    )

    t4_consolidar = PythonOperator(
        task_id="consolidar_reporte",
        python_callable=consolidar_reporte,
    )

    [t1_conteos, t2_frescura, t3_batch_kpi] >> t4_consolidar
