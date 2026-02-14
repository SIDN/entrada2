#!/usr/bin/env bash

export TOOL_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)

echo "Build image for version $TOOL_VERSION"

mvn clean && mvn package &&  \
  docker build --platform=linux/amd64 --tag=sidnlabs/entrada2:$TOOL_VERSION . && \
  docker push sidnlabs/entrada2:$TOOL_VERSION
