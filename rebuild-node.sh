#!/bin/bash

docker-compose rm -vsf minerva-node && docker-compose up --build --force-recreate minerva-node