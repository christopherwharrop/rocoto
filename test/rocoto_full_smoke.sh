#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ROCOTO_BIN="${ROOT_DIR}/sbin"
ROCOTO_RUBY="${ROCOTO_RUBY:-ruby}"
ROCOTO_SCHEDULER="${ROCOTO_SCHEDULER:-slurm}"
ROCOTO_ACCOUNT="${ROCOTO_ACCOUNT:-test}"
ROCOTO_QUEUE="${ROCOTO_QUEUE:-debug}"
SUBMIT_THREADS="${SUBMIT_THREADS:-8}"
TIMEOUT_SEC="${TIMEOUT_SEC:-30}"
RUN_STATUS_CHECKS="${RUN_STATUS_CHECKS:-0}"
RUN_REAL="${RUN_REAL:-0}"
REAL_BATCH_QUEUE_SERVER="${REAL_BATCH_QUEUE_SERVER:-true}"
REAL_WAIT_SEC="${REAL_WAIT_SEC:-10}"
AUTO_CONFIRM="${AUTO_CONFIRM:-1}"
KEEP_WORKDIR="${KEEP_WORKDIR:-0}"

WORK_DIR="${WORK_DIR:-$(mktemp -d)}"
export HOME="${WORK_DIR}/home"
mkdir -p "${HOME}"

cleanup() {
  if [[ "${KEEP_WORKDIR}" != "1" ]]; then
    rm -rf "${WORK_DIR}"
  else
    echo "Work dir kept at: ${WORK_DIR}"
  fi
}
trap cleanup EXIT

ROCOTO_VERSION=$(cat "${ROOT_DIR}/VERSION")
CONFIG_DIR="${HOME}/.rocoto/${ROCOTO_VERSION}"
mkdir -p "${CONFIG_DIR}"

write_config() {
  local batch_queue_server="$1"
  cat >"${CONFIG_DIR}/rocotorc" <<EOF
:DatabaseType: SQLite3
:WorkflowDocType: XML
:DatabaseServer: false
:BatchQueueServer: ${batch_queue_server}
:WorkflowIOServer: false
:MaxUnknowns: 3
:MaxLogDays: 7
:AutoVacuum: true
:VacuumPurgeDays: 30
:SubmitThreads: ${SUBMIT_THREADS}
:JobQueueTimeout: 45
:JobAcctTimeout: 45
EOF
}

WORKFLOW_XML="${WORK_DIR}/workflow.xml"
LOG_DIR="${WORK_DIR}/log"
DATA_DIR="${WORK_DIR}/data"
mkdir -p "${LOG_DIR}" "${DATA_DIR}"

write_workflow() {
  cat >"${WORKFLOW_XML}" <<EOF
<?xml version="1.0"?>
<!DOCTYPE workflow []>
<workflow realtime="f" scheduler="${ROCOTO_SCHEDULER}" cyclelifespan="0:01:00:00" cyclethrottle="1" corethrottle="10" taskthrottle="2">
  <log verbosity="3"><cyclestr>${LOG_DIR}/workflow_@Y@m@d@H@M.log</cyclestr></log>
  <cycledef group="group1">202001010000 203001010000 24:00:00</cycledef>
  <task name="dryrun_task" cycledefs="group1" maxtries="1">
    <command><cyclestr>${ROOT_DIR}/test/bin/test.ksh -d @Y-@m-@d_@H @p</cyclestr></command>
    <account>${ROCOTO_ACCOUNT}</account>
    <queue>${ROCOTO_QUEUE}</queue>
    <cores>1</cores>
    <walltime>00:00:30</walltime>
    <join>${LOG_DIR}/dryrun_task.join</join>
    <jobname>rocoto_dryrun</jobname>
    <envar>
      <name>START_TIME</name><value><cyclestr>@Y@m@d@H</cyclestr></value>
    </envar>
    <envar>
      <name>FORMAT</name><value><cyclestr>%Y%m%d%H</cyclestr></value>
    </envar>
  </task>
</workflow>
EOF
}
write_workflow

TIMEOUT_BIN=""
if command -v timeout >/dev/null 2>&1; then
  TIMEOUT_BIN="timeout"
elif command -v gtimeout >/dev/null 2>&1; then
  TIMEOUT_BIN="gtimeout"
fi

run_cmd() {
  echo "+ $*"
  "$@"
}

run_with_timeout() {
  local seconds="$1"
  shift
  if [[ -n "${TIMEOUT_BIN}" ]]; then
    run_cmd "${TIMEOUT_BIN}" "${seconds}" "$@"
  else
    run_cmd "$@"
  fi
}

DB_PATH="${WORK_DIR}/rocoto.db"
CYCLE="${TEST_CYCLE:-202001010000}"
TASK_NAME="dryrun_task"

echo "== Dryrun with BatchQueueServer=false (SubmitThreads=${SUBMIT_THREADS})"
write_config false
if [[ "${AUTO_CONFIRM}" == "1" ]]; then
  printf 'y\n' | run_with_timeout "${TIMEOUT_SEC}" "${ROCOTO_RUBY}" "${ROCOTO_BIN}/rocotoboot.rb" -n -d "${DB_PATH}" -w "${WORKFLOW_XML}" -c "${CYCLE}" -t "${TASK_NAME}"
else
  run_with_timeout "${TIMEOUT_SEC}" "${ROCOTO_RUBY}" "${ROCOTO_BIN}/rocotoboot.rb" -n -d "${DB_PATH}" -w "${WORKFLOW_XML}" -c "${CYCLE}" -t "${TASK_NAME}"
fi
run_with_timeout "${TIMEOUT_SEC}" "${ROCOTO_RUBY}" "${ROCOTO_BIN}/rocotorun.rb" -n -d "${DB_PATH}" -w "${WORKFLOW_XML}"
run_with_timeout "${TIMEOUT_SEC}" "${ROCOTO_RUBY}" "${ROCOTO_BIN}/rocotorun.rb" -n -d "${DB_PATH}" -w "${WORKFLOW_XML}"

test -s "${DB_PATH}"

echo "== Dryrun with BatchQueueServer=true"
DB_PATH_BQS="${WORK_DIR}/rocoto_bqs.db"
write_config true
if [[ "${AUTO_CONFIRM}" == "1" ]]; then
  printf 'y\n' | run_with_timeout "${TIMEOUT_SEC}" "${ROCOTO_RUBY}" "${ROCOTO_BIN}/rocotoboot.rb" -n -d "${DB_PATH_BQS}" -w "${WORKFLOW_XML}" -c "${CYCLE}" -t "${TASK_NAME}"
else
  run_with_timeout "${TIMEOUT_SEC}" "${ROCOTO_RUBY}" "${ROCOTO_BIN}/rocotoboot.rb" -n -d "${DB_PATH_BQS}" -w "${WORKFLOW_XML}" -c "${CYCLE}" -t "${TASK_NAME}"
fi
run_with_timeout "${TIMEOUT_SEC}" "${ROCOTO_RUBY}" "${ROCOTO_BIN}/rocotorun.rb" -n -d "${DB_PATH_BQS}" -w "${WORKFLOW_XML}"

if [[ "${RUN_STATUS_CHECKS}" == "1" ]]; then
  echo "== Status checks (may require scheduler commands on PATH)"
  run_with_timeout "${TIMEOUT_SEC}" "${ROCOTO_RUBY}" "${ROCOTO_BIN}/rocotostat.rb" -d "${DB_PATH}" -w "${WORKFLOW_XML}" -c "${CYCLE}" -t "${TASK_NAME}" -s || true
  run_with_timeout "${TIMEOUT_SEC}" "${ROCOTO_RUBY}" "${ROCOTO_BIN}/rocotocheck.rb" -d "${DB_PATH}" -w "${WORKFLOW_XML}" -c "${CYCLE}" -t "${TASK_NAME}" || true
fi

if [[ "${RUN_REAL}" == "1" ]]; then
  echo "== Non-dryrun functional run (requires scheduler configured)"
  if [[ "${REAL_BATCH_QUEUE_SERVER}" == "false" ]]; then
    echo "WARNING: REAL_BATCH_QUEUE_SERVER=false can deadlock in non-dryrun mode."
  fi
  DB_PATH_REAL="${WORK_DIR}/rocoto_real.db"
  write_config "${REAL_BATCH_QUEUE_SERVER}"

  if [[ "${AUTO_CONFIRM}" == "1" ]]; then
    printf 'y\n' | run_with_timeout "${TIMEOUT_SEC}" "${ROCOTO_RUBY}" "${ROCOTO_BIN}/rocotoboot.rb" -d "${DB_PATH_REAL}" -w "${WORKFLOW_XML}" -c "${CYCLE}" -t "${TASK_NAME}"
  else
    run_with_timeout "${TIMEOUT_SEC}" "${ROCOTO_RUBY}" "${ROCOTO_BIN}/rocotoboot.rb" -d "${DB_PATH_REAL}" -w "${WORKFLOW_XML}" -c "${CYCLE}" -t "${TASK_NAME}"
  fi

  run_with_timeout "${TIMEOUT_SEC}" "${ROCOTO_RUBY}" "${ROCOTO_BIN}/rocotorun.rb" -d "${DB_PATH_REAL}" -w "${WORKFLOW_XML}"
  sleep "${REAL_WAIT_SEC}"
  run_with_timeout "${TIMEOUT_SEC}" "${ROCOTO_RUBY}" "${ROCOTO_BIN}/rocotorun.rb" -d "${DB_PATH_REAL}" -w "${WORKFLOW_XML}"

  if [[ "${RUN_STATUS_CHECKS}" == "1" ]]; then
    run_with_timeout "${TIMEOUT_SEC}" "${ROCOTO_RUBY}" "${ROCOTO_BIN}/rocotostat.rb" -d "${DB_PATH_REAL}" -w "${WORKFLOW_XML}" -c "${CYCLE}" -t "${TASK_NAME}" -s || true
    run_with_timeout "${TIMEOUT_SEC}" "${ROCOTO_RUBY}" "${ROCOTO_BIN}/rocotocheck.rb" -d "${DB_PATH_REAL}" -w "${WORKFLOW_XML}" -c "${CYCLE}" -t "${TASK_NAME}" || true
  fi
fi

echo "OK: rocoto dryrun smoke tests complete"
