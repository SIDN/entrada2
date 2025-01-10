#!/usr/bin/env bash

echo "Build image for version $TOOL_VERSION"

mvn clean && mvn package &&  \
  docker build --platform=linux/amd64 --tag=sidnlabs/entrada2:$TOOL_VERSION .
