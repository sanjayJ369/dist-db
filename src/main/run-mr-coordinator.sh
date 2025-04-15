#!/usr/bin/env bash
set -e

# Usage: ./run.sh <plugin.go> <input-files...>
# Example: ./run.sh rtiming.go ../pg-*.txt

if [ $# -lt 2 ]; then
  echo "Usage: $0 <plugin.go> <input-files...>"
  exit 1
fi

PLUGIN_GO=$1
PLUGIN_NAME=$(basename "$PLUGIN_GO" .go)
PLUGIN_SO="${PLUGIN_NAME}.so"
shift
INPUT_FILES=("$@")

echo "*** Cleaning old builds"
(cd ../mrapps && go clean)
(cd ..         && go clean)

echo "*** Building plugin: $PLUGIN_GO"
(cd ../mrapps && go build $RACE -buildmode=plugin "$PLUGIN_GO")

echo "*** Building MapReduce binaries"
(cd . && go build $RACE ./mrcoordinator.go)
(cd . && go build $RACE ./mrworker.go)

echo "*** Starting coordinator with inputs: ${INPUT_FILES[*]}"
./mrcoordinator "${INPUT_FILES[@]}" &
COORD_PID=$!
sleep 1

echo "*** Starting workers using plugin: ../mrapps/${PLUGIN_SO}"
./mrworker "../mrapps/${PLUGIN_SO}" &
./mrworker "../mrapps/${PLUGIN_SO}"

wait $COORD_PID
echo "*** Run complete"
