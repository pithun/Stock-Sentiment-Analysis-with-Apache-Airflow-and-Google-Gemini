# Stock News Sentiment Analysis Pipeline

---

## Table of Contents
1. [Project Overview](##Project-Overview)
2. [Architecture and Workflow](#architecture-and-workflow)
3. [Installation and Setup](##installation-and-setup)
4. [Configure Environment Variables](##configure-environment-variables)
5. [Code Walkthrough](#code-walkthrough)
6. [Project Story and Journey](#project-story-and-journey)
7. [To Do / Future Enhancements](#to-do--future-enhancements)

---

## Project Overview
This project features an end-to-end pipeline that automates the collection, processing and analysis of news related to the stock market and delivers stock sentiment insights. 

Using Apache Airflow to orchestrate tasks, it retrieves news data from NewsAPI, processes and classifies the articles using natural language processing (NLP) and machine learning, loads the data into a Vertica database, compresses historical data as needed, runs dbt for further transformation, and finally leverages Gemini AI to generate summarized analysis that is sent via email.
An initial set of articles into stock related and non stock related using a deep learning model `FacebookAI/roberta-large-mnli` which served as our labeled dataset. After which `lightgbm` model was pretrained using traditional NLP techniques (Bag of Words, TFIDF for vectorization and ngrams of 1) for a more efficient modeling. loads the data into a Vertica database, compresses historical data as needed, runs dbt for further transformation, and finally leverages Gemini AI to generate summarized analysis that is sent via email.

## Technical Architecture

### Core Components
1. **Data Collection Layer**
   - NewsAPI integration for real-time news fetching
   - BeautifulSoup4 for article content extraction
   - Error handling for failed requests with fallback to article descriptions
   - Idempotent execution support for backfilling

2. **Machine Learning Layer**
   - TF-IDF Vectorization (300 features)
   - LightGBM classification model
   - NLTK for text preprocessing
   - Cloudpickle for model serialization

3. **Data Storage & Processing**
   - Vertica as the primary database
   - dbt for data transformation
   - JSON Lines format for data exchange
   - Monthly data archival system

4. **AI Analysis Layer**
   - Google Gemini 2.0 Flash model integration
   - Custom system prompts for stock analysis
   - Structured insight generation

5. **Notification System**
   - Gmail SMTP integration
   - UTF-8 encoded email support
   - Formatted daily reports

## Prerequisites

### API Keys & Credentials
1. NewsAPI Key
2. Google Gemini API Key
3. Gmail App Password for SMTP

### System Requirements
- Python 3.8+
- Apache Airflow 2.0+
- Vertica Database
- dbt Core
- Docker environment
- NLTK data files:
  - wordnet
  - averaged_perceptron_tagger_eng

### Python Dependencies
```bash
pip install apache-airflow
pip install apache-airflow-providers-vertica
pip install google-generativeai
pip install nltk
pip install beautifulsoup4
pip install dbt-vertica
pip install cloudpickle
pip install joblib
pip install pandas
pip install protobuf==4.25.3  # Required for specific compatibility
```

## Directory Structure
```
project_root/
├── data/                    # Raw news data organized by month
│   └── YYYY-MM/            # Monthly folders
│       └── YYYY-MM-DD_news_file.txt
├── labeled_data/           # ML-processed data
│   └── YYYY-MM/           
│       └── YYYY-MM-DD_news_file_labeled.jsonl
├── models/                 # ML model files
│   ├── tfidf_vectorizer_300.pkl
│   └── lgb.joblib
├── compressed_data/        # Monthly archives
│   └── YYYY-MM_raw_news_data.zip
├── ai_analysis/           # Gemini outputs
│   └── YYYY-MM/
│       └── YYYY-MM-DD_llm_advice.txt
└── dbt_project/          # dbt transformations
    └── models/
        ├── Stock_News.sql
        └── Non_Stock_News.sql
```

## Implementation Details

### Data Collection
```python
def connect_to_api_csv(**context):
    exec_datetime = context["execution_date"]
    exec_date = exec_datetime.strftime("%Y-%m-%d")
    
    # API connection with error handling
    url = f"https://newsapi.org/v2/everything?from={exec_date}&to={exec_date}&language=en&q=(market OR stock)&apiKey={api_key}"
    response = requests.get(url)
    articles = response.json()['articles']
    
    # Content extraction with fallback
    for article in articles:
        content = get_full_article(article['url'])
        if content is None:
            content = article['description']
```

### Machine Learning Pipeline
```python
def filter_news(** ):
    # Load pre-trained vectorizer
    with open(f"{model_path}/tfidf_vectorizer_300.pkl", "rb") as f:
        tfidf_loaded = cloudpickle.load(f)
    
    # Transform and predict
    char_array = tfidf_loaded.transform(df.content).toarray()
    frequency_matrix = pd.DataFrame(char_array, 
                                  columns=tfidf_loaded.get_feature_names_out())
    model = load(f"{model_path}/lgb.joblib")
    model_pred = model.predict(frequency_matrix)
```

### Data Archival System
The pipeline implements a sophisticated data archival system:
1. Checks execution date for month-end
2. Compresses entire month's data if at month-end
3. Maintains data accessibility for backfilling
4. Automatically cleans up original files after compression

```python
def compress_choice(**context):
    exec_datetime = context["next_execution_date"]
    exec_day = exec_datetime.day
    return 'compress_and_remove_files' if exec_day == 1 else 'do_nothing_start_dbt'
```

### dbt Implementation
The project uses dbt for data transformation with incremental loading:

1. **Stock News Model**
```sql
{{ config(materialized='incremental', unique_key='content') }}
select * from News_DB.Full_News_Table 
{% if is_incremental() %}
    where label = 1 and date = {{ ds }}
{% endif %}
```

2. **Dynamic Model Generation**
```bash
rm {{ params.stock_path }} {{ params.non_stock_path }} &&
echo "{{ dbt_model_content }}" >> {{ params.stock_path }}
```

### AI Analysis Integration
```python
def LLM_advice(**context):
    sys_instruct = "You are a Stock Sentiment Analyst..."
    client = genai.Client(api_key=gemini_api_key)
    
    # Extract stock news from Vertica
    news = context["task_instance"].xcom_pull(
        task_ids="extract_stock_news_info", 
        key="return_value"
    )
    
    # Generate insights
    response = client.models.generate_content(
        model="gemini-2.0-flash",
        config=types.GenerateContentConfig(
            system_instruction=sys_instruct
        ),
        contents=[news_prompt + news_as_text]
    )
```

## DAG Structure


```
          ┌─────────────────────────────┐
          │   Airflow DAG Scheduler     │
          └────────────┬────────────────┘
                       │
         ┌─────────────▼─────────────┐
         │  Extract News Articles    │  (get_news_data)
         └─────────────┬─────────────┘
                       │
         ┌─────────────▼─────────────┐
         │   Filter & Label News     │  (get_only_stock_news)
         └─────────────┬─────────────┘
                       │
         ┌─────────────▼─────────────┐
         │   Load Data into Vertica  │  (SQLExecuteQueryOperator)
         └─────────────┬─────────────┘
                       │
         ┌─────────────▼─────────────┐
         │  Compress/Archive Data    │  (BranchPythonOperator)
         └─────────────┬─────────────┘
                       │
         ┌─────────────▼─────────────┐
         │    dbt Transformation     │  (BashOperator)
         └─────────────┬─────────────┘
                       │
         ┌─────────────▼─────────────┐
         │   Generate AI Insights    │  (LLM_advice)
         └─────────────┬─────────────┘
                       │
         ┌─────────────▼─────────────┐
         │   Send Email Notification │  (send_ai_stock_insight)
         └───────────────────────────┘
```
*Figure: High-level architecture of the Stock Sentiment Analysis pipeline*


## Configuration Guide

### Airflow Variables
You can set Airflow Variables via command as shown below or in the Airflow UI.

```bash
# Email Configuration
airflow variables set email_vars '{
    "smtp_user": "your_email@gmail.com",
    "smtp_password": "app_specific_password",
    "smtp_url": "smtp://smtp.gmail.com:587"
}' -j

# API Keys
airflow variables set NEWS_API_KEY "your_key"
airflow variables set GEMINI_API_KEY "your_key"
```

### Vertica Setup
Veri
```bash
# Start Vertica Container
docker run -p 6433:5433 -p 6444:5444 \
    --mount "type=bind,source=/path/to/labeled_data,target=/home/data,ro" \
    --name vertica_ce_synced vertica/vertica-ce

# Configure Airflow Connection
airflow connections add \
    --conn-json '{
        "conn_type": "vertica",
        "login": "dbadmin",
        "password": "",
        "host": "127.0.0.1",
        "port": 6433,
        "schema": "VMart"
    }' vertica
```

## Testing & Debugging

### Common Issues

1. **Vertica Connection**
   - Enable connection testing: `export AIRFLOW__CORE__TEST_CONNECTION=Enabled`
   - Test connection: `airflow connections test vertica`
   - Check port forwarding: 6433:5433, 6444:5444

2. **API Rate Limits**
   - NewsAPI has daily request limits
   - Implement exponential backoff
   - Monitor usage in NewsAPI dashboard

3. **Data Persistence**
   - Verify mounted volumes
   - Check file permissions
   - Monitor disk space

4. **DBT Issues**
   - Validate model files exist
   - Check incremental logic
   - Verify table permissions

## Monitoring & Maintenance

1. **Daily Monitoring**
   - Check Airflow task status
   - Verify email delivery
   - Monitor API quotas

2. **Monthly Tasks**
   - Verify data archival
   - Check compression ratios
   - Audit storage usage

3. **Quarterly Maintenance**
   - Update API keys
   - Retrain ML models
   - Review system performance

## Future Enhancements

1. **Technical Improvements**
   - Implement parallel processing for news fetching
   - Add more sophisticated NLP preprocessing
   - Enhance error handling and retry logic

2. **Feature Additions**
   - Add support for multiple news sources
   - Implement sentiment strength scoring
   - Create interactive dashboard

3. **Infrastructure**
   - Add monitoring and alerting
   - Implement automated backups
   - Add CI/CD pipeline

## Troubleshooting Guide

### Task Failures

1. **News Collection Fails**
   ```bash
   airflow tasks test Stock_sentiment_analysis get_news_data 2024-02-11
   ```

2. **ML Processing Issues**
   - Check model files exist
   - Verify NLTK downloads
   - Review input data format

3. **Database Issues**
   ```bash
   vsql -h localhost -p 6433 -U dbadmin -w ""
   ```

## Reproducing this Project
I compiled all the codes and model files I created into a single image and have made that accessible on docker hub. Find below the steps to replcate.
1. Pull the image
   ```bash
   docker pull stock_sentiment_with_airflow_and_gemini
   ```
2. Populate the variables.
3. Add the connections.


## Contributing

1. Fork the repository
2. Create feature branch
3. Follow PEP 8 guidelines
4. Add appropriate tests
5. Submit pull request

## License

This project is licensed under the MIT License. See LICENSE file for details.
