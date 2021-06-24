#!/bin/bash

REPOSITORY=$2
MODEL_VERSION=$1
docker build \
-t $REPOSITORY:$MODEL_VERSION \
-f Dockerfile . && \
docker tag $REPOSITORY:$MODEL_VERSION $REPOSITORY:latest
