#!/usr/bin/env bash
# Workron Kubernetes gang-preemption + checkpoint demo.
#
# Submits a 3-task gang, lets in-cluster worker pods claim and run it,
# triggers preemption on one task via the API, observes the siblings
# drain (emitting synthetic checkpoints), waits for re-admission, then
# proves resume by inspecting the worker logs for the
# `checkpoint_data_present=true` line on the next claim.
#
# Requires: a running cluster brought up via `make k8s-up`, plus curl,
# jq, and kubectl on PATH.

set -euo pipefail

readonly API="${WORKRON_API:-http://localhost:30080}"
readonly NS="${WORKRON_NAMESPACE:-workron}"
readonly GANG_SIZE=3
readonly SLEEP_SECONDS=60
readonly POLL_TIMEOUT=90

# ---------- helpers ----------

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
muted() { printf '\033[2m%s\033[0m\n' "$*"; }
section() { printf '\n'; bold "=== $* ==="; }

require() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing required tool: $1" >&2; exit 1; }
}

api_get() { curl -sf "$API$1"; }
api_post_json() { curl -sf -X POST -H 'Content-Type: application/json' -d "$2" "$API$1"; }
api_post_empty() { curl -sf -X POST "$API$1"; }

gang_status_counts() {
  local id=$1
  api_get "/gangs/$id" | jq -r '[.[].status] | group_by(.) | map("\(.[0])=\(length)") | join(" ")'
}

gang_task_ids() {
  local id=$1
  api_get "/gangs/$id" | jq -r '.[].id'
}

# Wait until a jq filter against the gang body returns a non-empty result.
# Args: gang_id, jq_filter, human_label.
wait_for_gang() {
  local id=$1
  local filter=$2
  local label=$3
  local deadline=$(( $(date +%s) + POLL_TIMEOUT ))
  while [[ $(date +%s) -lt $deadline ]]; do
    if api_get "/gangs/$id" | jq -e "$filter" >/dev/null 2>&1; then
      muted "  $(gang_status_counts "$id")"
      return 0
    fi
    muted "  waiting: $label  ($(gang_status_counts "$id"))"
    sleep 1
  done
  echo "timeout waiting for: $label" >&2
  return 1
}

# ---------- preflight ----------

require curl
require jq
require kubectl

section "Preflight"
api_get /healthz >/dev/null && echo "scheduler /healthz: ok"
worker_count=$(api_get /workers | jq 'length')
echo "registered workers: $worker_count"
if [[ "$worker_count" -lt "$GANG_SIZE" ]]; then
  echo "need at least $GANG_SIZE active workers; only $worker_count registered" >&2
  exit 1
fi

# ---------- submit ----------

section "Submit 3-task gang (demo:sleep $SLEEP_SECONDS)"
submit_body=$(jq -nc \
  --arg cmd "demo:sleep $SLEEP_SECONDS" \
  --argjson n "$GANG_SIZE" \
  '{command: $cmd, gang_size: $n}')

submit_resp=$(api_post_json /jobs "$submit_body")
GANG_ID=$(echo "$submit_resp" | jq -r '.gang_id')
TASK_IDS=()
while IFS= read -r line; do
  TASK_IDS+=("$line")
done < <(echo "$submit_resp" | jq -r '.tasks[]')

echo "gang_id: $GANG_ID"
for i in "${!TASK_IDS[@]}"; do
  echo "  task[$i]: ${TASK_IDS[$i]}"
done

# ---------- wait for running ----------

section "Wait for all tasks to be running"
wait_for_gang "$GANG_ID" '[.[].status] | all(. == "running")' "all running"

# ---------- inject failure ----------

VICTIM=${TASK_IDS[0]}
section "Trigger preemption (fail task[0]: $VICTIM)"
api_post_empty "/jobs/$VICTIM/fail" >/dev/null
echo "fail acknowledged"

# ---------- observe drain ----------

section "Observe siblings drain (preempting -> preempted)"
# Siblings should hit either preempting or preempted; failed/blocked are also fine
# transient states. We just want the gang to leave 'running'.
wait_for_gang "$GANG_ID" \
  '[.[].status] | all(. != "running")' \
  "siblings drained"

# ---------- wait for re-admission ----------

section "Wait for re-admission and re-claim"
wait_for_gang "$GANG_ID" '[.[].status] | all(. == "running")' "all running again"

# Give worker logs a moment to flush the new claim line.
sleep 3

# ---------- prove resume from checkpoint ----------

section 'Resume proof: scan worker logs for checkpoint_data_present":true'
# Workers log in JSON via slog, so the marker is "key":value, not key=value.
# -c worker scopes to the worker container (silences Defaulted-container
# noise on stdout); ignore stderr so transient pod-not-ready blips don't
# pollute the output. Scope to the task IDs from this run so prior gangs
# from earlier demo invocations don't show up.
task_id_pattern=$(IFS='|'; echo "${TASK_IDS[*]}")
proof=$(kubectl -n "$NS" logs -l app=workron-worker -c worker --tail=300 --prefix=true 2>/dev/null \
  | grep '"checkpoint_data_present":true' \
  | grep -E "$task_id_pattern" || true)

if [[ -n "$proof" ]]; then
  echo "$proof"
else
  echo "no checkpoint_data_present:true log line found for this run's task IDs" >&2
  echo "(rare: try \`kubectl -n $NS logs -l app=workron-worker -c worker --tail=500\` for more context.)" >&2
fi

# ---------- summary ----------

section "Final gang state"
api_get "/gangs/$GANG_ID" \
  | jq -r '.[] | "  task[\(.gang_index // 0)] status=\(.status) attempts=\(.attempts) worker=\(.worker_id // "")"'

section "Done"
echo "API: $API"
echo "Gang: $GANG_ID"
echo
echo "Run \`make k8s-logs\` in another terminal to keep watching scheduler logs."
