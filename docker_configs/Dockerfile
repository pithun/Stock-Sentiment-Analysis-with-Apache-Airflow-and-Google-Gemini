# Firstly, we specify the parent image we want. I have already created a docker file to build an airflow image but I didn't fully
# understand what I did. I'll explain that from this tutorial

# This tells docker that we want this image from dockerhub our our local if it's there (Question: Does it first check our system
# for this local image?)

FROM apache/airflow:2.10.4-python3.11

# We could decide to add a layer to change the working directory for our further steps
WORKDIR /app

# I think this part creates a virtual environment and specifies the root dir
ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim wget -qqq

# This means copy requirements.txt file from the dir where the docker file is to the root dir of our docker image
# Runs
COPY requirements.txt .

USER airflow
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install -q -U google-genai
RUN mkdir -p /app/News-Api-Project/ai_analysis /app/News-Api-Project/compressed_data /app/News-Api-Project/data /app/News-Api-Project/labeled_data

COPY news_sentiment.py /opt/airflow/dags
COPY models /app/News-Api-Project
COPY functions.py /app/News-Api-Project

# Apparently, we need this in our image to be able to allow the docker container get exposed to our localhost. This expose seems
# to be saying 4000 is the port we're allowing you to expose to the local host. Now if an application isn't running on 4000, 
# does this mean we won't be able to expose that port?

# I decided to test this with the dockerfile from the course. In there I only exposed 4000 but changed the api port to 9000.
# What this basically does is that in the image from docker desktop, you can only map 4000 to a localhost port but becasue 
# there's nothing running on that port, you don't get any results but if you use docker run from a command line, you'll still
# be able to point to a port you didn't expose. Basically even though only 4000 was exposed, I could still aad localhost port to
# the actual api port of 9000. In summary, it only affects docker desktop.
EXPOSE 9091 9000

# Say we had an application like my crack detection algorithm and I wanted to put all the code into an image, what I could do
# is after specifying all the dependencies and stuff, I can tell docker that on running the image, it should run this file also
#CMD ["streamlit","app.py"]

