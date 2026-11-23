#!/usr/bin/env bash
set -euo pipefail

# anchor to this script's directory so the relative ../dns-lib, ../pcap-lib cd's below always work
cd "$(dirname "$0")"

export TOOL_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)

if [[ -z "$TOOL_VERSION" ]]; then
  echo "ERROR: could not determine project.version from pom.xml (mvn help:evaluate returned empty output)" >&2
  exit 1
fi

echo "Build image for version $TOOL_VERSION"

cd ../dns-lib && \
  mvn clean && mvn install -U && \
  cd ../pcap-lib && \
  mvn clean && mvn install -U && \
  cd ../entrada2 && \
  mvn clean && mvn package -U &&  \
  docker build --platform=linux/amd64 --tag=sidnlabs/entrada2:$TOOL_VERSION . && \
  docker push sidnlabs/entrada2:$TOOL_VERSION
