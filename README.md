# Proyecto Final — Pipeline Híbrido Batch + Near Real-Time con ELK y Airflow

**Curso:** Ingeniería de Datos — USFQ
**Autores:**
- Máximo Pinta
- Steeven Quezada

Pipeline de datos sobre el ecosistema de videojuegos de Steam que combina un flujo
**batch** (catálogo de juegos, modelado dimensional) con un flujo **near
real-time** (eventos de 4 fuentes distintas), orquestados y fusionados mediante
**Apache Airflow**, indexados en **Elasticsearch** y visualizados en **Kibana**.

Este proyecto reutiliza y extiende la infraestructura de dos talleres previos del
curso en vez de partir de cero: el Taller 1 (ETL batch en KNIME) aporta la capa
batch, y el Taller 2 (pipeline ELK) aporta la capa streaming. El trabajo nuevo de
este entregable es la capa de orquestación con Airflow que conecta ambas capas.
El detalle completo de esta decisión está en [`docs/arquitectura.md`](docs/arquitectura.md).

## Arquitectura (resumen)

```
KNIME (Taller 1) ──► Postgres DW (dw.fact_juego_metricas + dimensiones)
                                        │
                          ┌─────────────┘
                          ▼
              Airflow: batch_kpi_resumen (@daily)
                          │
                          ▼
              dw.kpi_resumen_juegos

MySQL / MongoDB / CSV / Python-sensores (Taller 2)
        │ (Logstash / bulk API)
        ▼
   Elasticsearch  ──────────────────────────┐
                                             ▼
                          Airflow: merge_batch_speed (cada 15 min)
                          (lee ES + Postgres, enriquece, reindexa)
                                             │
                                             ▼
                          Elasticsearch: actividad_enriquecida-*
                                             │
                                             ▼
                                        Kibana (dashboard)

Airflow: data_quality_checks (cada 30 min) ── valida conteos, frescura y
                                                corridas batch del día
```

Diagrama completo (Mermaid) y justificación de cada decisión técnica en
[`docs/arquitectura.md`](docs/arquitectura.md).

## Estructura del repositorio

```
.
├── docker-compose.yml       # Infraestructura completa (9 servicios)
├── .env.example             # Variables de entorno requeridas (sin credenciales)
├── dags/                    # Los 3 DAGs de Airflow (código nuevo de este proyecto)
│   ├── batch_kpi_resumen.py
│   ├── merge_batch_speed.py
│   └── data_quality_checks.py
├── generadores/             # Notebooks Python/Faker que simulan las 4 fuentes streaming (Taller 2)
│   ├── insertar_n_datos_mysql.ipynb
│   ├── insertar_n_datos_mongo.ipynb
│   ├── insertar_n_datos_csv.ipynb
│   └── sensores.ipynb
├── logstash/
│   └── taller2_pipeline.conf   # Pipeline de Logstash (3 inputs: jdbc, mongodb, file)
├── docs/
│   ├── arquitectura.md      # Arquitectura detallada + diagrama Mermaid
│   └── pruebas.md           # Evidencia de pruebas y validación
└── capturas/                 # Screenshots de KNIME, Kibana y Airflow
```

## Requisitos

- Docker y Docker Compose
- ~8 GB de RAM libres como mínimo (Elasticsearch + Airflow + MySQL + Postgres +
  MongoDB corriendo en simultáneo)
- Logstash 7.17.10 instalado de forma nativa (no está containerizado en este
  proyecto — ver nota abajo)

## Cómo levantar el proyecto

1. **Clonar y configurar variables de entorno**

   ```bash
   git clone <url-de-este-repo>
   cd <carpeta-del-repo>
   cp .env.example .env
   # editar .env con contraseñas reales
   ```

2. **Levantar la infraestructura**

   ```bash
   mkdir -p dags airflow-logs plugins
   cp dags_del_repo/*.py dags/   # o simplemente usa la carpeta dags/ del repo directamente
   docker compose up -d
   ```

3. **Verificar servicios**

   | Servicio | URL / puerto |
   |---|---|
   | Kibana | http://localhost:5601 |
   | Elasticsearch | http://localhost:9200 |
   | Airflow | http://localhost:8080 |
   | MySQL | localhost:3306 |
   | Postgres (DW) | localhost:5432 |
   | MongoDB | localhost:27017 |

4. **Configurar la conexión de Airflow a Postgres**

   En la UI de Airflow: Admin → Connections → crear `postgres_dw` (tipo Postgres,
   host `postgres-dev`, puerto 5432, con las credenciales de `.env`). Ver detalle
   en `docs/arquitectura.md`.

5. **Iniciar Logstash** (nativo, fuera de Docker)

   ```bash
   logstash -f logstash/taller2_pipeline.conf
   ```

6. **Correr los generadores de datos**

   Ejecutar los notebooks en `generadores/` (requieren `pip install Faker
   mysql-connector-python pymongo`). Cada uno simula tráfico continuo hacia su
   fuente respectiva.

7. **Activar los DAGs**

   En la UI de Airflow, activa los 3 DAGs (`batch_kpi_resumen`,
   `merge_batch_speed`, `data_quality_checks`) y dispáralos manualmente la
   primera vez para validar que corren sin errores.

## Los 3 DAGs

| DAG | Frecuencia | Propósito |
|---|---|---|
| `batch_kpi_resumen` | Diaria | Agrega KPIs (precio, % reseñas positivas, playtime) por género y década desde el DW batch. |
| `merge_batch_speed` | Cada 15 min | Combina actividad streaming reciente (Elasticsearch) con atributos de catálogo (Postgres) — capa de integración Lambda. |
| `data_quality_checks` | Cada 30 min | Verifica que las 4 fuentes streaming sigan activas y que el flujo batch haya corrido hoy. |

Explicación tarea por tarea de cada DAG en [`docs/arquitectura.md`](docs/arquitectura.md).

## Pruebas realizadas

Ver [`docs/pruebas.md`](docs/pruebas.md) para el detalle de las validaciones
hechas sobre infraestructura, cada fuente streaming, el dashboard de Kibana y
cada uno de los 3 DAGs.

## Notas y decisiones a destacar

- **Por qué no se reimplementó el ETL de KNIME en Airflow**: ver
  `docs/arquitectura.md`, sección correspondiente.
- **Logstash corre nativo, no en Docker**: decisión heredada del Taller 2, se
  mantiene por continuidad; el `docker-compose.yml` de este proyecto no incluye
  un contenedor de Logstash.
- **`_PIP_ADDITIONAL_REQUIREMENTS` en Airflow**: válido para desarrollo, no para
  producción — ver advertencia y justificación en `docs/arquitectura.md`.
