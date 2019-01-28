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

docker run -it -v ${PWD}/logs:/usr/src/app/logs \
    -v ${PWD}/data:/usr/src/app/data \
    -e GITHUB_USERNAME='username' \
    -e GITHUB_API_ACCESS_TOKEN='some-token' \ 
    -e GITHUB_API_URL='https://api.github.com/graphql' \
    jimfawkes/project-luther:latest
