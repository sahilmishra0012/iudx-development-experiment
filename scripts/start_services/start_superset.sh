#!/bin/bash

docker-compose -f ../../setup/superset/docker-compose.yml up -d
docker exec -it superset_superset_1 superset-init
