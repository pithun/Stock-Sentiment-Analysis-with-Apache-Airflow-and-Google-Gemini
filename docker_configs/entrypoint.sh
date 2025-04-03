#!/bin/bash
set -e

# Check if the Airflow database is not initialized we initialize it
if [ ! -f /home/airflow/airflow.db ]; then
  echo "Initializing Airflow database..."
  airflow db init &&
  airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
fi

if [ ! -f /app/News-Api-Project/My_DBT/Airflow_Stock_Sentiment_Project/models/airflow_stock_sentiment_models ]; then
  mkdir /app/News-Api-Project/My_DBT/ &&
  cd /app/News-Api-Project/My_DBT/ &&
  dbt init Airflow_Stock_Sentiment_Project -s &&
  rm -r /app/News-Api-Project/My_DBT/Airflow_Stock_Sentiment_Project/models/example &&
  mkdir /app/News-Api-Project/My_DBT/Airflow_Stock_Sentiment_Project/models/airflow_stock_sentiment_models &&
  touch /app/News-Api-Project/My_DBT/Airflow_Stock_Sentiment_Project/models/airflow_stock_sentiment_models/Non_Stock_News.sql &&
  touch /app/News-Api-Project/My_DBT/Airflow_Stock_Sentiment_Project/models/airflow_stock_sentiment_models/Stock_News.sql
fi

# Start Airflow components (this runs every time)
echo "Starting Airflow webserver and scheduler..."
exec airflow webserver & airflow scheduler