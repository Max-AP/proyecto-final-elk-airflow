# Pruebas y validación

Este documento resume las pruebas realizadas sobre el pipeline completo. Las
capturas referenciadas están en `capturas/`.

## 1. Infraestructura

- `docker ps` confirmando los 9 contenedores activos (mysql-dev, postgres-dev,
  elasticsearch-dev, kibana-dev, mongo-dev, postgres-airflow, airflow-webserver,
  airflow-scheduler; airflow-init corre una vez y termina).
- `curl http://localhost:9200` → 200 OK, versión de Elasticsearch confirmada.
- `curl http://localhost:5601/api/status` → 200 OK.
- `http://localhost:8080` (Airflow UI) accesible con el usuario admin creado por
  `airflow-init`.

<img width="2275" height="533" alt="Screenshot 2026-09-10 211414" src="https://github.com/user-attachments/assets/42c015f3-db12-4142-a9d0-8f61841cb103" />
<img width="2275" height="913" alt="Screenshot 2026-09-10 211454" src="https://github.com/user-attachments/assets/f6a5e66f-b652-4492-af1c-2d69d1b0925b" />
<img width="2275" height="913" alt="Screenshot 2026-09-10 211439" src="https://github.com/user-attachments/assets/587aaaf2-1212-4ded-bb26-76a8f8a4a446" />
<img width="1917" height="607" alt="Screenshot 2026-09-10 214656" src="https://github.com/user-attachments/assets/9984215c-7a2f-481f-9ed3-3692e7c1d278" />


## 2. Capa batch (Taller 1 — KNIME)

- Flujo de KNIME ejecutado sin errores, tabla `dw.fact_juego_metricas` poblada con
  94,948 filas / 26 columnas (ver `capturas/knime_taller1_dw_flow.png`).
- Esquema estrella verificado: `dim_juego`, `dim_genero`, `dim_desarrollador`,
  `dim_publisher`, `dim_fecha` conectados correctamente a la tabla de hechos
  (ver `capturas/dw_esquema_estrella.png`).

## 3. Capa streaming (Taller 2)

Para cada fuente se verificó que Logstash indexara documentos nuevos en
Elasticsearch mientras el notebook generador corría:

| Fuente | Verificación | Resultado |
|---|---|---|
| MySQL (`eventos_juegos`) | `curl http://localhost:9200/eventos_juegos-*/_count` creciendo con el notebook corriendo | OK |
| MongoDB (`eventos_foro`) | ídem sobre `eventos_foro-*` | OK |
| CSV (`logs_servidor`) | ídem sobre `logs_servidor-*` | OK |
| Sensores (bulk directo) | ídem sobre `sensores-servidores-*` | OK |

> Reto documentado: un desajuste entre las columnas reales del CSV (7, tras
> agregar `host`/`ip_origen` con Faker) y las 5 columnas declaradas originalmente
> en el filtro `csv` de `taller2_pipeline.conf`, causando mapeo posicional
> incorrecto. Corregido actualizando la lista `columns` del filtro.

## 4. Dashboard Kibana

Dashboard "Vista Principal" con un panel por fuente + KPI de salud del sistema +
gauge de CPU promedio (ver `capturas/dashboard_vista_principal_v1.png`).

> Nota: el volumen de datos de Elasticsearch se perdió al recrear el contenedor
> durante el desarrollo de este proyecto; el dashboard se reconstruyó y los
> generadores se volvieron a correr para repoblar los índices. El histórico
> anterior al incidente no se recuperó por decisión deliberada (datos sintéticos,
> sin valor de negocio real) — se documenta como lección operativa: los
> volúmenes de Docker deben preservarse explícitamente entre recreaciones de
> contenedor.

## 5. DAGs de Airflow

Para cada DAG:

- Ejecutado manualmente desde la UI (botón *Trigger DAG*).
- Confirmado en la vista **Grid**/**Graph** que todas las tareas terminan en
  verde (`success`), respetando las dependencias diseñadas.
- Revisado el historial de corridas (*DAG Runs*) para confirmar ejecuciones
  programadas subsecuentes sin intervención manual.

| DAG | Resultado de la prueba |
|---|---|
| `batch_kpi_resumen` | Corrida manual exitosa. Verificado con `SELECT * FROM dw.kpi_resumen_juegos;` — filas por género y por década presentes. |
| `merge_batch_speed` | Corrida manual exitosa. `curl http://localhost:9200/actividad_enriquecida-*/_search?pretty` devolvió 11 documentos combinando actividad streaming + atributos de catálogo. |
| `data_quality_checks` | Corrida manual exitosa, reporte consolidado sin problemas (con las 4 fuentes streaming activas al momento de la prueba). |

(ver `batch_kpi_resumen_dag.png`, `merge_batch_speed_dag.png`, `data_quality_checks_dag.png`, `data_quality_checks_dag_log.png`, `data_quality_checks_dag_log_error.png`. 
Se incluye una captura de error para demostar el correcto funcionamiento).

## 6. Resumen de resultados

El pipeline demuestra:

1. **Dos tipos de flujo simultáneos**: batch (KNIME → DW → `batch_kpi_resumen`) y
   near real-time (4 fuentes → Logstash/bulk → Elasticsearch, con polling de
   segundos).
2. **Al menos 4 fuentes de datos distintas**: MySQL, MongoDB, archivo CSV,
   Python/sensores directo — más el catálogo batch como quinta fuente.
3. **Procesamiento e integración real**: `merge_batch_speed` combina ambas capas,
   no solo las almacena por separado.
4. **Validación automatizada**: `data_quality_checks` da evidencia objetiva y
   repetible de que el pipeline sigue funcionando, sin depender de revisión
   manual constante.
