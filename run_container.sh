#!/bin/bash

mkdir logs
mkdir -p data/episodes
mkdir -p data/episode
mkdir -p data/repository
mkdir -p data/podcast
mkdir -p data/reference
mkdir -p data/dataframe
mkdir -p data/stargazer
mkdir -p data/validation_result

docker run -it -v ${PWD}/logs:/usr/src/app/logs -v ${PWD}/data:/usr/src/app/data jimfawkes/project-luther:latest
