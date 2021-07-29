#!/bin/bash

docker-compose -f ../../setup/livy/docker-compose.yml build
docker-compose -f ../../setup/livy/docker-compose.yml up -d
