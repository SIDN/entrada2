#!/usr/bin/env bash

export TOOL_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)

echo "Build image for version $TOOL_VERSION"

cd ../dns-lib && \
  mvn clean && mvn install -U && \
  cd ../pcap-lib && \
  mvn clean && mvn install -U && \
  cd ../entrada2 && \
  mvn clean && mvn package -U &&  \
  docker build --platform=linux/amd64 --tag=sidnlabs/entrada2:$TOOL_VERSION . && \
  docker push sidnlabs/entrada2:$TOOL_VERSION
