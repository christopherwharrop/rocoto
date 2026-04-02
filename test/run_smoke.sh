#!/usr/bin/env bash
################################################################################
# run_smoke.sh — Rocoto smoke test runner with named test cases
#
# Usage:
#   ./run_smoke.sh                  # interactive menu
#   ./run_smoke.sh dryrun           # quick dryrun (no Slurm submission)
#   ./run_smoke.sh real             # real Slurm submission
#   ./run_smoke.sh status           # dryrun + status checks
#   ./run_smoke.sh full             # real + status checks (full integration)
#   ./run_smoke.sh list             # list all test cases
#
# Auto-detects local Slurm partition and account.
################################################################################
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SMOKE_SCRIPT="${SCRIPT_DIR}/rocoto_full_smoke.sh"

if [[ ! -x "${SMOKE_SCRIPT}" ]]; then
  chmod +x "${SMOKE_SCRIPT}"
fi

# ── Report tracking ──────────────────────────────────────────────────────────

RESULTS=()        # "case_name:PASS" or "case_name:FAIL"
RUN_START=""
CASES_REQUESTED="" # label for the header

record_result() {
  local case_name="$1"
  local exit_code="$2"
  if [[ "${exit_code}" -eq 0 ]]; then
    RESULTS+=("${case_name}:PASS")
  else
    RESULTS+=("${case_name}:FAIL")
  fi
}

print_header() {
  local hostname
  hostname="$(hostname -s 2>/dev/null || echo "unknown")"
  local rocoto_version
  rocoto_version="$(cat "${SCRIPT_DIR}/../VERSION" 2>/dev/null || echo "unknown")"
  local branch
  branch="$(cd "${SCRIPT_DIR}/.." && git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")"
  local rocoto_path
  rocoto_path="$(cd "${SCRIPT_DIR}/../sbin" && pwd)"
  local system_rocoto
  system_rocoto="$(which rocotorun 2>/dev/null || echo "none")"
  local ruby_ver
  ruby_ver="$(ruby --version 2>/dev/null | awk '{print $2}' || echo "unknown")"

  echo ""
  echo "================================================================"
  echo "  Rocoto Smoke Test Report"
  echo "================================================================"
  echo "  Date:       $(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo "  Host:       ${hostname}"
  echo "  Branch:     ${branch}"
  echo "  Rocoto:     ${rocoto_version}"
  echo "  Build:      ${rocoto_path}"
  echo "  System:     ${system_rocoto}"
  echo "  Ruby:       ${ruby_ver}"
  echo "  Slurm:      partition=${PARTITION}  account=${ACCOUNT}"
  echo "  Test case:  ${CASES_REQUESTED}"
  echo "================================================================"
  echo ""
}

print_report() {
  local total=${#RESULTS[@]}
  local pass=0
  local fail=0

  for r in "${RESULTS[@]}"; do
    if [[ "${r}" == *":PASS" ]]; then
      ((pass++))
    else
      ((fail++))
    fi
  done

  local elapsed=""
  if [[ -n "${RUN_START}" ]]; then
    local now
    now=$(date +%s)
    elapsed="$(( now - RUN_START ))s"
  fi

  echo ""
  echo "================================================================"
  echo "  RESULTS                                        ${elapsed:+[${elapsed}]}"
  echo "================================================================"

  for r in "${RESULTS[@]}"; do
    local name="${r%%:*}"
    local status="${r##*:}"
    if [[ "${status}" == "PASS" ]]; then
      printf "  [PASS]  %s\n" "${name}"
    else
      printf "  [FAIL]  %s\n" "${name}"
    fi
  done

  echo "----------------------------------------------------------------"
  printf "  Total: %d   Passed: %d   Failed: %d\n" "${total}" "${pass}" "${fail}"
  echo "================================================================"

  if [[ "${fail}" -gt 0 ]]; then
    echo ""
    echo "  OVERALL: FAIL"
    echo ""
    return 1
  else
    echo ""
    echo "  OVERALL: PASS"
    echo ""
    return 0
  fi
}

# ── Auto-detect local Slurm configuration ────────────────────────────────────

detect_partition() {
  if command -v scontrol &>/dev/null; then
    scontrol show partition 2>/dev/null | awk '
      /PartitionName=/ {
        for (i = 1; i <= NF; i++) {
          if ($i ~ /^PartitionName=/) {
            split($i, a, "=");
            print a[2];
            exit;
          }
        }
      }
    ' || true
  fi
}

detect_account() {
  if command -v sacctmgr &>/dev/null; then
    sacctmgr -nP show account format=account 2>/dev/null \
      | head -1 | tr -d ' ' || true
  fi
}

PARTITION="$(detect_partition)"
ACCOUNT="$(detect_account)"

# Fallbacks
PARTITION="${PARTITION:-batch}"
ACCOUNT="${ACCOUNT:-root}"

# ── Test cases ────────────────────────────────────────────────────────────────

run_dryrun() {
  echo "=== Test Case: dryrun ==="
  echo "  Scheduler: slurm | No Slurm jobs submitted"
  echo ""
  local rc=0
  ROCOTO_SCHEDULER=slurm \
  ROCOTO_ACCOUNT="${ACCOUNT}" \
  ROCOTO_QUEUE="${PARTITION}" \
  KEEP_WORKDIR=1 \
    bash "${SMOKE_SCRIPT}" || rc=$?
  record_result "dryrun" "${rc}"
  return "${rc}"
}

run_dryrun_bqs_false() {
  echo "=== Test Case: dryrun-bqs-false ==="
  echo "  Scheduler: slurm | BatchQueueServer=false only | Thread pool guard test"
  echo ""
  local rc=0
  ROCOTO_SCHEDULER=slurm \
  ROCOTO_ACCOUNT="${ACCOUNT}" \
  ROCOTO_QUEUE="${PARTITION}" \
  SUBMIT_THREADS=8 \
  KEEP_WORKDIR=1 \
    bash "${SMOKE_SCRIPT}" || rc=$?
  record_result "dryrun-bqs-false" "${rc}"
  return "${rc}"
}

run_status() {
  echo "=== Test Case: status ==="
  echo "  Scheduler: slurm | Dryrun + rocotostat/rocotocheck"
  echo ""
  local rc=0
  ROCOTO_SCHEDULER=slurm \
  ROCOTO_ACCOUNT="${ACCOUNT}" \
  ROCOTO_QUEUE="${PARTITION}" \
  RUN_STATUS_CHECKS=1 \
  KEEP_WORKDIR=1 \
    bash "${SMOKE_SCRIPT}" || rc=$?
  record_result "status" "${rc}"
  return "${rc}"
}

run_real() {
  echo "=== Test Case: real ==="
  echo "  Scheduler: slurm | Submits a real job to partition '${PARTITION}'"
  echo ""
  local rc=0
  ROCOTO_SCHEDULER=slurm \
  ROCOTO_ACCOUNT="${ACCOUNT}" \
  ROCOTO_QUEUE="${PARTITION}" \
  RUN_REAL=1 \
  KEEP_WORKDIR=1 \
    bash "${SMOKE_SCRIPT}" || rc=$?
  record_result "real" "${rc}"
  return "${rc}"
}

run_full() {
  echo "=== Test Case: full ==="
  echo "  Scheduler: slurm | Real submission + status checks (full integration)"
  echo ""
  local rc=0
  ROCOTO_SCHEDULER=slurm \
  ROCOTO_ACCOUNT="${ACCOUNT}" \
  ROCOTO_QUEUE="${PARTITION}" \
  RUN_REAL=1 \
  RUN_STATUS_CHECKS=1 \
  KEEP_WORKDIR=1 \
    bash "${SMOKE_SCRIPT}" || rc=$?
  record_result "full" "${rc}"
  return "${rc}"
}

run_threads() {
  echo "=== Test Case: threads ==="
  echo "  Scheduler: slurm | Dryrun with varying thread pool sizes"
  echo ""
  local any_fail=0
  for threads in 1 4 8 16; do
    echo "--- SubmitThreads=${threads} ---"
    local rc=0
    ROCOTO_SCHEDULER=slurm \
    ROCOTO_ACCOUNT="${ACCOUNT}" \
    ROCOTO_QUEUE="${PARTITION}" \
    SUBMIT_THREADS="${threads}" \
    TIMEOUT_SEC=30 \
      bash "${SMOKE_SCRIPT}" || rc=$?
    record_result "threads-${threads}" "${rc}"
    [[ "${rc}" -ne 0 ]] && any_fail=1
    echo ""
  done
  return "${any_fail}"
}

# ── Menu / dispatch ───────────────────────────────────────────────────────────

list_cases() {
  cat <<EOF
Available test cases:

  dryrun      Quick dryrun — no Slurm jobs (default)
  status      Dryrun + rocotostat/rocotocheck
  real        Real Slurm submission (submits a job)
  full        Real submission + status checks (full integration)
  threads     Dryrun with thread pool sizes 1, 4, 8, 16
  all         Run dryrun → status → real → full sequentially

Detected Slurm config:
  Partition:  ${PARTITION}
  Account:    ${ACCOUNT}
EOF
}

run_all() {
  run_dryrun || true
  echo ""
  run_status || true
  echo ""
  run_real || true
  echo ""
  run_full || true
}

interactive_menu() {
  echo "Rocoto Smoke Test Runner"
  echo "────────────────────────"
  echo "Detected: partition=${PARTITION}  account=${ACCOUNT}"
  echo ""
  echo "  1) dryrun     — quick dryrun, no Slurm jobs"
  echo "  2) status     — dryrun + status checks"
  echo "  3) real       — real Slurm submission"
  echo "  4) full       — real + status checks"
  echo "  5) threads    — dryrun with varying thread pools"
  echo "  6) all        — run all cases sequentially"
  echo "  q) quit"
  echo ""
  read -rp "Select [1-6, q]: " choice

  case "${choice}" in
    1) CASES_REQUESTED="dryrun";   print_header; run_dryrun   || true ;;
    2) CASES_REQUESTED="status";   print_header; run_status   || true ;;
    3) CASES_REQUESTED="real";     print_header; run_real     || true ;;
    4) CASES_REQUESTED="full";     print_header; run_full     || true ;;
    5) CASES_REQUESTED="threads";  print_header; run_threads  || true ;;
    6) CASES_REQUESTED="all (dryrun, status, real, full)"; print_header; run_all ;;
    q|Q) exit 0 ;;
    *) echo "Invalid choice: ${choice}"; exit 1 ;;
  esac
}

# ── Main ──────────────────────────────────────────────────────────────────────

RUN_START=$(date +%s)

case "${1:-}" in
  dryrun)   CASES_REQUESTED="dryrun";   print_header; run_dryrun   || true ;;
  status)   CASES_REQUESTED="status";   print_header; run_status   || true ;;
  real)     CASES_REQUESTED="real";     print_header; run_real     || true ;;
  full)     CASES_REQUESTED="full";     print_header; run_full     || true ;;
  threads)  CASES_REQUESTED="threads";  print_header; run_threads  || true ;;
  all)      CASES_REQUESTED="all (dryrun, status, real, full)"; print_header; run_all ;;
  list)     list_cases; exit 0 ;;
  "")
    interactive_menu
    ;;
  *)
    echo "Unknown test case: $1"
    echo ""
    list_cases
    exit 1
    ;;
esac

# Print report if any tests were run
if [[ ${#RESULTS[@]} -gt 0 ]]; then
  print_report
  exit $?
fi
