# Base image  
FROM apache/airflow:2.10.4-python3.11  

# Set working directory  
WORKDIR /app  

# Define environment variables  
ENV AIRFLOW_HOME=/home/airflow  

# Switch to root user  
USER root  

# Enable passwordless sudo for airflow user  
RUN echo "airflow ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers  

# Install necessary packages  
RUN apt-get update -qq && apt-get install -y vim wget libgomp1  

# Copy requirements file  
COPY requirements.txt .  

# Switch to airflow user  
USER airflow  

# Install Python dependencies  
RUN pip install --no-cache-dir -r requirements.txt  
RUN pip install -q -U google-genai  

# Create necessary directories  
RUN mkdir -p /app/News-Api-Project/models \
    /app/News-Api-Project/ai_analysis \
    /app/News-Api-Project/compressed_data \
    /app/News-Api-Project/data \
    /app/News-Api-Project/labeled_data  

# Copy project files  
COPY news_sentiment_docker.py /home/airflow/dags/news_sentiment_docker.py  
COPY models/* /app/News-Api-Project/models  
COPY entrypoint.sh /app/News-Api-Project/  
COPY profiles.yml /home/airflow/.dbt/profiles.yml  

# Expose necessary ports  
EXPOSE 9091 9000 8000 
