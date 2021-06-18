#!/bin/bash

docker network create adaptor-net
docker-compose -f ../../setup/rmq/docker-compose.yml up -d
