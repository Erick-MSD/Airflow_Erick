# Pipelines de Airflow

Repositorio para desarrollar pipelines de datos con Apache Airflow. Cada DAG vive en su propia carpeta dentro de `dags/`, con su código, datos de entrada, SQL y notebooks.

## Estructura

```
.
├── airflow/                  # Configuración y arranque de Airflow (AIRFLOW_HOME)
│   ├── airflow.cfg
│   ├── webserver_config.py
│   └── start_airflow.sh      # start | stop | status
├── dags/                     # Carpeta de DAGs que lee Airflow
│   ├── .airflowignore
│   ├── _template/            # Plantilla para crear un DAG nuevo (Airflow la ignora)
│   └── northwind/            # DAG: northwind_postgres_pipeline
│       ├── northwind_postgres_dag.py
│       ├── etl_pipeline.py
│       ├── data/             # CSV de entrada
│       ├── sql/              # Esquema y consultas de PostgreSQL
│       ├── notebooks/        # Análisis de los resultados
│       └── output/           # Generado al correr (no se sube a git)
├── plugins/                  # Plugins compartidos de Airflow
├── requirements.txt
└── .env.example              # Variables locales (copiar a .env)
```

Al correr Airflow se generan `airflow/airflow.db`, `airflow/logs/` y `airflow/.airflow-pids/`. Esos archivos son estado local y están en `.gitignore`.

## Instalación (Ubuntu / WSL2)

Crea el entorno virtual en la raíz del repositorio, dentro del sistema de archivos de Linux (no en `/mnt/c` ni en OneDrive):

```bash
python3 -m venv .venv
```

```bash
source .venv/bin/activate
```

```bash
python -m pip install -r requirements.txt
```

Crea tu archivo de variables y edítalo con tus credenciales:

```bash
cp .env.example .env
```

## Iniciar Airflow

```bash
./airflow/start_airflow.sh start
```

El script define `AIRFLOW_HOME=airflow/`, apunta a `dags/` y `plugins/`, carga `.env`, desactiva los DAGs de ejemplo, ejecuta `airflow db migrate` y levanta el scheduler y el webserver en segundo plano. La interfaz queda en `http://localhost:8080`.

```bash
./airflow/start_airflow.sh status
```

```bash
./airflow/start_airflow.sh stop
```

```bash
tail -f airflow/logs/scheduler.log
```

Para usar el CLI de `airflow` directamente en tu terminal, exporta las mismas rutas:

```bash
export AIRFLOW_HOME="$PWD/airflow" AIRFLOW_PROJECT_DIR="$PWD"
```

Crear un usuario administrador (pide la contraseña de forma interactiva):

```bash
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com
```

## Agregar un DAG nuevo

```bash
cp -r dags/_template dags/mi_pipeline
```

Después sigue los pasos de `dags/_template/README.md`: renombrar el archivo del DAG, cambiar el `dag_id` y el import (`from mi_pipeline.pipeline import ...`).

## Pipelines

| Carpeta | DAG | Descripción |
| --- | --- | --- |
| [`dags/northwind`](dags/northwind/README.md) | `northwind_postgres_pipeline` | ETL de los CSV de Northwind hacia PostgreSQL |
