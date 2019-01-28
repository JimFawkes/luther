FROM joyzoursky/python-chromedriver:3.7-selenium
WORKDIR /usr/src/app
RUN mkdir luther
RUN python3 -m pip install --upgrade pip

RUN mkdir ./logs
RUN mkdir -p ./data/dataframe
RUN mkdir ./data/episode
RUN mkdir ./data/podcast
RUN mkdir ./data/reference
RUN mkdir ./data/stargazer
RUN mkdir ./data/validation_result
RUN mkdir ./data/episodes

COPY luther ./luther
COPY requirements.txt .
RUN python3 -m pip install -r ./requirements.txt

ENTRYPOINT [ "python3", "luther/main.py" ]
