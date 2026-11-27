#!/usr/bin/env bash
set -Eeuo pipefail

# airflow/ guarda la configuración; la raíz del repo tiene dags/, plugins/ y .venv/
AIRFLOW_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "${AIRFLOW_DIR}")"
VENV_DIR="${PROJECT_DIR}/.venv"
LOG_DIR="${AIRFLOW_DIR}/logs"
PID_DIR="${AIRFLOW_DIR}/.airflow-pids"
ENV_FILE="${PROJECT_DIR}/.env"

# Carga variables locales (por ejemplo PG_DSN) si existe el archivo .env
if [[ -f "${ENV_FILE}" ]]; then
    set -a
    source "${ENV_FILE}"
    set +a
fi

if [[ ! -x "${VENV_DIR}/bin/airflow" ]]; then
    echo "Airflow was not found at ${VENV_DIR}/bin/airflow."
    echo "Create the Linux virtual environment and install the requirements first."
    exit 1
fi

export AIRFLOW_HOME="${AIRFLOW_DIR}"
export AIRFLOW_PROJECT_DIR="${PROJECT_DIR}"
export AIRFLOW__CORE__DAGS_FOLDER="${PROJECT_DIR}/dags"
export AIRFLOW__CORE__PLUGINS_FOLDER="${PROJECT_DIR}/plugins"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
export AIRFLOW__CORE__EXECUTOR="SequentialExecutor"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:////${AIRFLOW_DIR#/}/airflow.db"

mkdir -p "${LOG_DIR}" "${PID_DIR}"

pid_is_running() {
    local pid_file="$1"
    [[ -f "${pid_file}" ]] && kill -0 "$(<"${pid_file}")" 2>/dev/null
}

start_process() {
    local name="$1"
    local command="$2"
    local pid_file="${PID_DIR}/${name}.pid"
    local log_file="${LOG_DIR}/${name}.log"

    if pid_is_running "${pid_file}"; then
        echo "${name} is already running (PID $(<"${pid_file}"))"
        return
    fi

    rm -f "${pid_file}"
    nohup bash -c "${command}" >"${log_file}" 2>&1 &
    echo "$!" >"${pid_file}"
    echo "Started ${name} (PID $(<"${pid_file}")); log: ${log_file}"
}

stop_process() {
    local name="$1"
    local pid_file="${PID_DIR}/${name}.pid"

    if ! pid_is_running "${pid_file}"; then
        rm -f "${pid_file}"
        echo "${name} is not running"
        return
    fi

    local pid
    pid="$(<"${pid_file}")"
    kill "${pid}" 2>/dev/null || true
    rm -f "${pid_file}"
    echo "Stopped ${name} (PID ${pid})"
}

status_process() {
    local name="$1"
    local pid_file="${PID_DIR}/${name}.pid"

    if pid_is_running "${pid_file}"; then
        echo "${name}: running (PID $(<"${pid_file}"))"
    else
        echo "${name}: stopped"
    fi
}

case "${1:-start}" in
    start)
        "${VENV_DIR}/bin/airflow" db migrate
        start_process "scheduler" "exec '${VENV_DIR}/bin/airflow' scheduler"
        start_process "webserver" "exec '${VENV_DIR}/bin/airflow' webserver --port 8080"
        echo "Airflow UI: http://localhost:8080"
        ;;
    stop)
        stop_process "webserver"
        stop_process "scheduler"
        ;;
    status)
        status_process "scheduler"
        status_process "webserver"
        ;;
    *)
        echo "Usage: $0 {start|stop|status}"
        exit 2
        ;;
esac
