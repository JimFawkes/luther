# Project Luther

This is my second Project for the Metis Data Science Bootcamp.
Presented January 25th, 2019

The Task was to use:
 - Web-scraping to get at least a portion of the Data
 - Linear Regression to get insights


The Question I wanted to answer was:
 _"How great of an impact on your GitHub Repository can I predict if it is featured on a Python Podcast?"_

**Summary:**
The entire pipeline can be run by running the [run_container.sh](run_container.sh) script. This will run the pipeline in a docker container, using the docker image: jimfawkes/project-luther.

_Note: Running the entire pipleine will take 60min+_

 - [Presentation Slides](./presentation/Project\ Luther.pptx)
____

This repository contains the code used to 
 - get the data
 - clean it
 - divide it into a training (60%), validation (20%) and test (20%) set
 - run, fit and validate models on the data


____

The data used, is from the [Talk Python To Me](talkpython.fm) Podcast, and retrieved using the GitHub API v4.

___

Tools & Technology:
 - Selenium
 - Pandas and Numpy
 - Statsmodels
 - Sklearn
 - plotly, matplotlib, seaborn
 - Docker
 - AWS
 - GraphQL

___