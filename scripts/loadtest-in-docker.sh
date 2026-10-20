#!/usr/bin/env bash
# Runs the Gatling simulation from a container on the Compose network, so traffic goes straight to the
# gateway container instead of through the Docker VM's host port forwarding (which, on Colima, became
# the bottleneck and failed under load). Needs the stack from deploy/compose.yaml to be running.
# Usage: scripts/loadtest-in-docker.sh [rate sessions/s] [duration s]
set -euo pipefail
cd "$(dirname "$0")/.."
RATE=${1:-200}
DURATION=${2:-60}
docker run --rm --network matchbook_default \
  -v "$PWD":/src -v "$HOME/.m2":/root/.m2 -w /src \
  maven:3.10.0-eclipse-temurin-25-noble \
  mvn -B -q -pl loadtest gatling:test -Dgateway=http://gateway:8080 -Drate="$RATE" -Dduration="$DURATION"
