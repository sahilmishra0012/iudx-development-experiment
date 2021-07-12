#!/bin/bash


docker-compose -f ../../setup/spark/docker-compose.yml build

docker-compose -f ../../setup/spark/docker-compose.yml up -d
