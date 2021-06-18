#!/bin/bash

docker network create adaptor-net
docker-compose -f ../../setup/kudu/docker-compose.yml up -d
