#!/bin/bash

export QUICKSTART_IP=$(docker network inspect analytics-net -f '{{(index .IPAM.Config 0).Gateway}}')
export QUICKSTART_LISTEN_ADDR=$QUICKSTART_IP
echo $QUICKSTART_LISTEN_ADDR
export IMPALA_QUICKSTART_IMAGE_PREFIX="apache/impala:81d5377c2-"
docker-compose -f ../../setup/impala/docker-compose.yml up -d
