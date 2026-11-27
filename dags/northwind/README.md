# Northwind ETL con Airflow y PostgreSQL

DAG `northwind_postgres_pipeline`: procesa los CSV de Northwind, limpia y valida los datos, crea tablas analíticas, guarda artefactos versionados y carga el resultado en PostgreSQL.

## Archivos

- `northwind_postgres_dag.py`: definición del DAG.
- `etl_pipeline.py`: lógica de extracción, validación, transformación, salida y carga a PostgreSQL.
- `data/`: CSV de entrada (`orders`, `customers`, `products`, `categories`, `order-details`).
- `sql/northwind_postgres_schema.sql`: tablas de PostgreSQL.
- `sql/northwind_queries.sql`: consultas analíticas.
- `notebooks/northwind_analysis.ipynb`: lectura de las tablas limpias desde PostgreSQL.
- `output/`: artefactos versionados y staging de Airflow (se genera al correr).

## Tareas

1. `check_input_directory`: verifica que estén los archivos requeridos.
2. `extract_raw_files`: lee y valida los cinco CSV.
3. `transform_and_validate`: limpia valores, detecta filas inválidas, une los datasets y crea las tablas analíticas.
4. `save_output_files`: guarda los CSV versionados en `output/`.
5. `load_postgres`: inserta los datos transformados y la auditoría en PostgreSQL.

## Configuración de PostgreSQL

La conexión se lee de la variable `PG_DSN` (definida en el `.env` de la raíz). Si no está definida, la tarea `load_postgres` falla con un mensaje claro.

```bash
PG_DSN=postgresql+psycopg2://usuario:contraseña@host:5432/northwind
```

Crea las tablas ejecutando `sql/northwind_postgres_schema.sql` en la base `northwind`.

Si PostgreSQL corre en Windows y Airflow en WSL2, `pg_hba.conf` debe permitir la subred de WSL y el Firewall de Windows debe permitir conexiones TCP entrantes al puerto `5432` desde esa subred.

## Correr sin Airflow

Desde la raíz del repositorio:

```bash
python dags/northwind/etl_pipeline.py
```

## Reglas de calidad de datos

- validación del número de filas esperado en los cinco archivos;
- manejo explícito de `order-details.csv`;
- limpieza de valores `NULL` literales;
- detección de países que parecen códigos postales;
- `batch_id` y marcas de tiempo de ejecución;
- tabla `audit_invalid_rows`;
- archivos de salida versionados;
- logging estructurado.

## Tablas en PostgreSQL

- `fact_orders`
- `agg_sales_by_country`
- `agg_sales_by_category`
- `audit_invalid_rows`

El esquema también incluye las tablas raw y limpias que usa el pipeline.

## Problemas comunes

- **`connection refused` en el puerto `5432`**: revisa que PostgreSQL esté corriendo en Windows y que `pg_hba.conf` y el Firewall permitan conexiones desde WSL2.
- **Una tarea queda como zombie**: corre Airflow desde el proyecto y el entorno virtual en Linux, no desde `/mnt/c` ni una carpeta de OneDrive.
