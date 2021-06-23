#!/bin/bash

docker network create analytics-net
docker-compose -f ../../setup/rmq/docker-compose.yml up -d
