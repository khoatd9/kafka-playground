#!/usr/bin/env bash
# One-time setup: downloads the Prometheus JMX exporter agent (from Maven Central) and verifies it.
set -euo pipefail
cd "$(dirname "$0")/.."
VER=1.0.1
SHA1=f9c53eb0aa1828c3c9cd7647238563e43a4ed68c
JAR=config/jmx-exporter/jmx_prometheus_javaagent.jar
if [[ -f "$JAR" ]] && [[ "$(shasum "$JAR" | cut -d' ' -f1)" == "$SHA1" ]]; then
  echo "JMX exporter agent already present."; exit 0
fi
echo "Downloading jmx_prometheus_javaagent $VER ..."
curl -fsSL -o "$JAR" "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/$VER/jmx_prometheus_javaagent-$VER.jar"
[[ "$(shasum "$JAR" | cut -d' ' -f1)" == "$SHA1" ]] || { echo "Checksum mismatch for $JAR"; rm -f "$JAR"; exit 1; }
echo "OK: $JAR"
