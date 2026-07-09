#!/usr/bin/env bash
# Generic Antithesis launcher shared by every scenario.
#
#   ./launch.sh <scenario> [extra snouty flags]
#
# <scenario> is a sibling directory holding a docker-compose.yaml and a launch.env.
# launch.env supplies the per-scenario bits; everything else — image tagging,
# property-history key, the fault profile shape, build-before-submit — is common
# and lives here so every shot is identical and comparable and no fault flag is
# ever fumbled or forgotten. Change a shot's faults by editing launch.env's node
# list, not by passing one-off flags.
#
# launch.env (sourced from the scenario directory) sets:
#   SCENARIO_TEST_NAME      test name reported to Antithesis
#   SCENARIO_DESCRIPTION    human description; the git commit is appended
#   SCENARIO_FAULT_NODES    space-separated SUT container names to fault
#   SCENARIO_WEBHOOK        optional; tenant webhook, default persistent_storage
#
# Required environment (read by snouty):
#   ANTITHESIS_TENANT       tenant name
#   ANTITHESIS_API_KEY      api key  (or ANTITHESIS_USERNAME + ANTITHESIS_PASSWORD)
#   ANTITHESIS_REPOSITORY   registry to push the built config + service images to
#
# Optional overrides (win over launch.env / defaults):
#   DURATION=<minutes>      default 30
#   TEST_NAME=<name>        default SCENARIO_TEST_NAME
#   DESCRIPTION=<text>      default SCENARIO_DESCRIPTION; commit is appended
#   FAULT_NODES=<names>     default SCENARIO_FAULT_NODES
#   WEBHOOK=<name>          default SCENARIO_WEBHOOK or persistent_storage
#   SOURCE=<identifier>     property-history key; default is the git branch
#   DRY_RUN=1               print the exact command and exit without submitting
#   SKIP_BUILD=1            skip docker build/push; reuse images already in the registry
#   GIT_SHA=<tag>           override the image tag (use with SKIP_BUILD=1 to pin to a prior build)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SCENARIO="${1:?usage: launch.sh <scenario> [extra snouty flags]}"
shift
SCENARIO_DIR="$SCRIPT_DIR/$SCENARIO"
[ -d "$SCENARIO_DIR" ] || {
  echo "error: no scenario directory $SCENARIO_DIR" >&2
  exit 1
}
[ -f "$SCENARIO_DIR/docker-compose.yaml" ] || {
  echo "error: $SCENARIO_DIR/docker-compose.yaml not found" >&2
  exit 1
}
[ -f "$SCENARIO_DIR/launch.env" ] || {
  echo "error: $SCENARIO_DIR/launch.env not found" >&2
  exit 1
}

# Per-scenario settings. Declared here so a missing one is caught, not silently empty.
SCENARIO_TEST_NAME=""
SCENARIO_DESCRIPTION=""
SCENARIO_FAULT_NODES=""
SCENARIO_WEBHOOK=""
# shellcheck source=/dev/null
. "$SCENARIO_DIR/launch.env"

# Immutable per-build revision: the short commit, marked -dirty when the working
# tree has uncommitted changes so the tag never claims to be a clean commit it is
# not. Images are tagged by this, never :latest, so a shot can never reuse a stale
# mutable tag and every pushed image traces back to the source it was built from.
# GIT_SHA can be overridden (e.g. with SKIP_BUILD=1) to reuse images from a prior build.
if [[ -z "${GIT_SHA:-}" ]]; then
  GIT_SHA="$(git -C "$SCRIPT_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
  if [[ -n "$(git -C "$SCRIPT_DIR" status --porcelain 2>/dev/null)" ]]; then
    GIT_SHA="${GIT_SHA}-dirty"
  fi
fi
export ANTITHESIS_IMAGE_TAG="$GIT_SHA"

WEBHOOK="${WEBHOOK:-${SCENARIO_WEBHOOK:-persistent_storage}}"
DURATION="${DURATION:-30}"
TEST_NAME="${TEST_NAME:-${SCENARIO_TEST_NAME:?launch.env must set SCENARIO_TEST_NAME}}"
DESCRIPTION="${DESCRIPTION:-$SCENARIO_DESCRIPTION} (commit ${GIT_SHA})"
FAULT_NODES="${FAULT_NODES:-${SCENARIO_FAULT_NODES:?launch.env must set SCENARIO_FAULT_NODES}}"

# Property-history key. Passing --source makes the run tracked (not ephemeral),
# so findings are produced and each property's history is grouped by this key.
# Default to the branch so history follows the branch; without it snouty runs
# ephemeral and no findings are available to triage.
SOURCE="${SOURCE:-$(git -C "$SCRIPT_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)}"

# Fault profile: all fault types are currently disabled so runs exercise the
# conservation property under normal operating conditions, without crash/recover
# noise. Node termination, hang, and throttle are cleared (empty include lists
# mean no containers are targeted). The SUT nodes are also excluded from network
# faults via exclude_from_network_faults so partitions do not interfere. cpu_mod
# and clock_jitter are disabled. The oracle is never included in the node-fault
# lists regardless — its obligation ledger is in-memory, so killing or freezing
# it would erase the source of truth.
FAULTS=(
  --param custom.include_for_node_termination=""
  --param custom.include_for_node_hang=""
  --param custom.include_for_node_throttle=""
  --param custom.exclude_from_network_faults="$FAULT_NODES"
  --param custom.cpu_mod=false
  --param custom.clock_jitter=false
)

for v in ANTITHESIS_TENANT ANTITHESIS_REPOSITORY; do
  if [[ -z "${!v:-}" ]]; then
    echo "error: $v is not set (required to build and submit the run)" >&2
    exit 1
  fi
done

# Rebuild the images from current source before submitting. snouty reuses a
# matching :latest tag instead of rebuilding, so without this a shot can ship
# stale code (e.g. an image baked before a config rename or a code change).
# Layer caching keeps this near-instant when nothing changed.
build=(docker compose -f "$SCENARIO_DIR/docker-compose.yaml" build)

# Launch from a rendered copy so the image tag is concrete. snouty ships the compose
# uninterpolated, so an `${ANTITHESIS_IMAGE_TAG:-dev}` tag reaches the platform as the
# never-pushed `:dev`; `docker compose config` bakes in the tag snouty actually pushed.
LAUNCH_DIR="$SCENARIO_DIR/.launch"
render=(docker compose -f "$SCENARIO_DIR/docker-compose.yaml" config)

cmd=(snouty launch
  --webhook "$WEBHOOK"
  --config "$LAUNCH_DIR"
  --test-name "$TEST_NAME"
  --description "$DESCRIPTION"
  --source "$SOURCE"
  --duration "$DURATION"
  "${FAULTS[@]}"
  "$@")

printf 'build: '
printf ' %q' "${build[@]}"
printf '\n'
printf 'render:'
printf ' %q' "${render[@]}"
printf ' > %q\n' "$LAUNCH_DIR/docker-compose.yaml"
printf 'launch:'
printf ' %q' "${cmd[@]}"
printf '\n'
if [[ "${DRY_RUN:-0}" == "1" ]]; then
  echo "(dry run; not building or submitting)"
  exit 0
fi
if [[ "${SKIP_BUILD:-0}" == "1" ]]; then
  echo "(skipping build; reusing images already in the registry)"
else
  # Remove any existing images carrying this tag before building. BuildKit errors
  # with "already exists" when re-exporting to a tag already in the local store,
  # which happens on every re-run when the tree is dirty (the tag stays
  # <sha>-dirty across edits).
  docker images --format "{{.Repository}}:{{.Tag}}" |
    { grep ":${GIT_SHA}$" || true; } |
    xargs -r docker rmi --force 2>/dev/null || true
  "${build[@]}"
fi
mkdir -p "$LAUNCH_DIR"
"${render[@]}" >"$LAUNCH_DIR/docker-compose.yaml"
exec "${cmd[@]}"
