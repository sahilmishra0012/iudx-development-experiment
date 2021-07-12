#!/bin/bash

docker-compose -f ../../setup/flink/docker-compose.yml build
docker-compose -f ../../setup/flink/docker-compose.yml up -d
