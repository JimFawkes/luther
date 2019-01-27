FROM joyzoursky/python-chromedriver:3.7-selenium
WORKDIR /usr/src/app
RUN mkdir luther
RUN python3 -m pip install --upgrade pip

# RUN wget -O "chromedriver_linux64.zip" "https://chromedriver.storage.googleapis.com/2.37/chromedriver_linux64.zip"
# RUN unzip -o "chromedriver_linux64.zip"
# RUN cp "chromedriver" "/usr/local/bin/chromedriver"
# RUN chmod +x "/usr/local/bin/chromedriver"

RUN mkdir ./logs
RUN mkdir ./data
RUN mkdir ./data/dataframe
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
