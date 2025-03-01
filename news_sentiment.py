# Imports

# Airflow Modules
import airflow
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.email import send_email

# Trad NLP Modules
from joblib import load
import nltk
# Download necessary NLTK resources quietly
nltk.download("wordnet", quiet=True)
nltk.download('averaged_perceptron_tagger_eng', quiet=True)
from nltk.tag import pos_tag
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import cloudpickle

# Useful Functionality Modules
import pandas as pd
import datetime as dt
import os
from bs4 import BeautifulSoup
import markdown
import shutil
import requests
import re
import datetime as dt
import pathlib
import json

# Google Gemini Modules
from google import genai
from google.genai import types

# Retrieve credentials from Airflow Variables
api_key = Variable.get("NEWS_API_KEY")
airflow_vars_str = Variable.get("email_vars", deserialize_json=True)
airflow_vars = json.loads(airflow_vars_str)
gemini_api_key = Variable.get("GEMINI_API_KEY")

SMTP_URL = airflow_vars['smtp_url']
SMTP_USER = airflow_vars['smtp_user']
SMTP_PASSWORD = airflow_vars['smtp_password']

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2024, 12, 1),
}

# Defining path variables
parent_path = "/mnt/c/Users/User/News-Project/Stock-Sentiment-Analysis-with-Apache-Airflow-and-Google-Gemini/"
data_path = f"{parent_path}data/"
labeled_data_path = f"{parent_path}labeled_data/"
model_path = f"{parent_path}models/"
compressed_data_path = f"{parent_path}compressed_data/"
ai_content = f"{parent_path}ai_analysis/"

# DBT file paths
stock_news_path = "/mnt/c/Users/User/Documents/My_DBT/Airflow_Stock_Sentiment_Project/models/airflow_stock_sentiment_models/Stock_News.sql"
non_stock_news_path = "/mnt/c/Users/User/Documents/My_DBT/Airflow_Stock_Sentiment_Project/models/airflow_stock_sentiment_models/Non_Stock_News.sql"
dbt_path = "/mnt/c/Users/User/Documents/My_DBT/Airflow_Stock_Sentiment_Project"

# DBT file content for stock news and non-stock news
stock_news_dbt_file_content = """{{ config(materialized='incremental',
unique_key='content') }}

select * from News_DB.Full_News_Table 

{% if is_incremental() %}
    where label =1 and date={{ ds }}
{% endif %}"""

non_stock_news_dbt_file_content = """{{ config(materialized='incremental',
unique_key='content') }}

select * from News_DB.Full_News_Table 

{% if is_incremental() %}
    where label =1 and date={{ ds }}
{% endif %}"""


def get_full_article(url):
    """
    Scrapes and returns the full text of a news article from the given URL.

    Parameters:
        url (str): URL of the news article.

    Returns:
        str: Concatenated text from all <p> tags if successful.
        None: If the request fails or content is unavailable.
    """
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            paragraphs = soup.find_all('p')
            full_text = "\n".join([p.get_text() for p in paragraphs])
            return full_text
    except requests.exceptions.RequestException:
        return None  # Return None if the request fails

    return None


def connect_to_api_csv(**context):
    """
    Connects to the news API, retrieves news articles for a given day,
    processes the articles, and saves them as a CSV file.

    The function:
    - Formats the execution date.
    - Removes an existing file if it exists.
    - Creates necessary directories.
    - Builds the API URL and retrieves data.
    - Scrapes full article content (or uses description if not available).
    - Saves the data in CSV format.
    """
    exec_datetime = context["execution_date"]
    exec_date = exec_datetime.strftime("%Y-%m-%d")
    exec_month = exec_datetime.strftime("%Y-%m")

    file_name = str(exec_date) + '_news_file.txt'

    # Drop file if it exists
    if os.path.exists(f"{data_path}{exec_month}/{file_name}"):
        os.remove(f"{data_path}{exec_month}/{file_name}")

    # Create required directory for the month
    pathlib.Path(f"{data_path}{exec_month}").mkdir(parents=True, exist_ok=True)

    # Build the news API URL
    url = (f"https://newsapi.org/v2/everything?from={exec_date}&to={exec_date}&language=en&q=(market OR stock)&apiKey={api_key}")
    response = requests.get(url)
    resp_dict = response.json()
    articles = resp_dict['articles']

    empty_json = {'date': [], 'content': []}

    # Process each article in the response
    for article in articles:
        published_date = article['publishedAt']
        published_date = re.findall('[\d-]+', published_date)[0]
        content_url = article['url']
        content = get_full_article(content_url)

        # If full content is unavailable, use the description
        if content is None:
            content_descrp = article['description']
            empty_json['content'].append(content_descrp)
        else:
            empty_json['content'].append(content)
        empty_json['date'].append(published_date)
            
    news_df = pd.DataFrame(empty_json)
    news_df_shape = news_df.shape
    if news_df_shape[0] == 0:
        new_row = [exec_date, "No data for this day"]
        news_df.loc[len(news_df)] = new_row
        news_df.to_csv(f"{data_path}{exec_month}/{file_name}")
    else:
        news_df.to_csv(f"{data_path}{exec_month}/{file_name}")


# Load pre-trained TF-IDF vectorizer using cloudpickle
with open(f"{model_path}/tfidf_vectorizer_300.pkl", "rb") as f:
    tfidf_loaded = cloudpickle.load(f)


def filter_news(**context):
    """
    Filters news data to extract stock-related articles using a pre-trained model.

    The function:
    - Loads the daily news CSV file.
    - Cleans the data by dropping unnecessary columns and rows with missing values.
    - Transforms the content using a loaded TF-IDF vectorizer.
    - Predicts labels using a pre-trained model.
    - Saves the labeled data as a line-delimited JSON (.jsonl) file.
    """
    exec_datetime = context["execution_date"]    # <datetime> with timezone
    exec_date = exec_datetime.strftime("%Y-%m-%d")
    exec_month = exec_datetime.strftime("%Y-%m")

    file_name = str(exec_date) + '_news_file.txt'

    # Create required directories for labeled data and models
    pathlib.Path(f"{labeled_data_path}{exec_month}").mkdir(parents=True, exist_ok=True)
    pathlib.Path(model_path).mkdir(parents=True, exist_ok=True)

    # Load and clean the dataset
    df = pd.read_csv(f"{data_path}{exec_month}/{file_name}")
    df.drop('Unnamed: 0', axis=1, inplace=True)
    df.dropna(inplace=True)

    # Transform content using the loaded TF-IDF vectorizer
    char_array = tfidf_loaded.transform(df.content).toarray()
    frequency_matrix = pd.DataFrame(char_array, columns=tfidf_loaded.get_feature_names_out())

    # Load the model and predict labels
    model = load(f"{model_path}/lgb.joblib")
    model_pred = model.predict(frequency_matrix)
    df['label'] = model_pred
    df["content"] = df["content"].str.replace("\n", " ", regex=True)

    # Save the labeled data as line-delimited JSON (.jsonl)
    df.to_json(f"{labeled_data_path}{exec_month}/" + file_name.replace('.txt', '_labeled.jsonl'), orient="records", lines=True)


def compress_choice(**context):
    """
    Determines whether to compress and remove files based on the next execution day.

    Returns:
        str: 'compress_and_remove_files' if next execution day is the 1st,
             otherwise 'do_nothing_start_dbt'.
    """
    exec_datetime = context["next_execution_date"]
    exec_day = exec_datetime.day  # Get day of month
    if exec_day == 1:
        return 'compress_and_remove_files'
    else:
        return 'do_nothing_start_dbt'


def compress_and_remove_files_(**context):
    """
    Compresses raw news data for the month and removes the original folder if it's time to archive.

    The function:
    - Checks if it's the start of a new month.
    - Compresses the folder containing raw news data.
    - Removes the original uncompressed folder after archiving.
    """
    next_exec_datetime = context["next_execution_date"]
    next_exec_day = next_exec_datetime.day  # Get day of month

    exec_datetime = context["execution_date"]
    exec_month = exec_datetime.strftime("%Y-%m")

    compressed_file_name = f"{exec_month}_raw_news_data"

    # Create compressed data directory if it doesn't exist
    if os.path.exists(f"{compressed_data_path}"):
        pass
    else:
        os.makedirs(f"{compressed_data_path}")

    # If it's the 1st day and the archive does not exist, then compress
    if next_exec_day == 1 and os.path.exists(f"{compressed_data_path}{compressed_file_name}.zip"):
        pass
    elif next_exec_day == 1:
        # Path to the folder to compress
        folder_to_compress = f"{data_path}{exec_month}"

        # Output archive path (without .zip extension)
        archive_name = f"{compressed_data_path}{compressed_file_name}"

        # Create a zip archive of the folder
        shutil.make_archive(archive_name, 'zip', folder_to_compress)

        # Remove the original folder after compression
        shutil.rmtree(f"{data_path}{exec_month}")
    else:
        pass


def LLM_advice(**context):
    """
    Generates AI-based stock sentiment advice using Google Gemini based on extracted news.

    The function:
    - Retrieves news content from XCom.
    - Sends the news content to Google Gemini for summarization and stock advice.
    - Saves the AI-generated advice to a text file.
    """
    exec_datetime = context["execution_date"]
    exec_date = exec_datetime.strftime("%Y-%m-%d")
    exec_month = exec_datetime.strftime("%Y-%m")

    llm_output = f"{ai_content}{exec_month}/{exec_date}_llm_advice.txt"

    # System instruction for the language model
    sys_instruct = "You are a Stock Sentiment Analyst. Your work is to summarize news data and give stock investment advice in a nicely formated way."
    client = genai.Client(api_key=gemini_api_key)
    news = context["task_instance"].xcom_pull(
        task_ids="extract_stock_news_info", key="return_value"
    )
    news_as_text = ''
    for ind, cont in enumerate(news):
        news_as_text += f"News_{ind+1} \n '\n'.join{[str(a) for a in cont]} \n"

    response = client.models.generate_content(
        model="gemini-2.0-flash",
        config=types.GenerateContentConfig(
            system_instruction=sys_instruct),
        contents=["""These are the recent stock news for today. Please in a summarized way, tell me 1. The stocks mentioned today 2. Which of these stocks had a bad sentiment 3. Which had good sentiment 4. Which would you advice me to keep an eye on \n""" + news_as_text]
    )
    # Create directory for AI content if it doesn't exist
    if os.path.exists(f"{ai_content}{exec_month}"):
        pass
    else:
        os.makedirs(f"{ai_content}{exec_month}")
    # Save the AI-generated advice to file
    with open(llm_output, "w") as f:
        f.write(response.text)


def format_file_to_html(file_path):
    """
    Converts a Markdown-formatted text file into a styled HTML email.

    Parameters:
        file_path (str): Path to the Markdown file.

    Returns:
        str: HTML content with inline CSS styling.
    """
    # Read the raw Markdown text from the file
    with open(file_path, 'r', encoding='utf-8') as f:
        raw_text = f.read()
    
    # Convert Markdown to HTML
    html_body = markdown.markdown(raw_text)
    
    # Wrap the HTML body in a complete HTML email template with inline CSS
    html_email = f"""
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf-8">
        <title>AI Generated Stock News & Investment Advice</title>
        <style>
          body {{
            font-family: Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            margin: 0;
            padding: 0;
            background-color: #f9f9f9;
          }}
          .container {{
            max-width: 800px;
            margin: 30px auto;
            background-color: #fff;
            padding: 20px;
            border: 1px solid #ddd;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
          }}
          h1 {{
            color: #00539C;
          }}
          h2 {{
            color: #0073e6;
            border-bottom: 1px solid #ddd;
            padding-bottom: 5px;
          }}
          ul {{
            list-style-type: disc;
            margin-left: 20px;
          }}
          .good {{
            color: green;
            font-weight: bold;
          }}
          .bad {{
            color: red;
            font-weight: bold;
          }}
          .disclaimer {{
            font-size: 0.9em;
            color: #666;
            margin-top: 20px;
            border-top: 1px solid #ccc;
            padding-top: 10px;
          }}
        </style>
      </head>
      <body>
        <div class="container">
          <h2>AI Analysis on Stock News Provided</h2>
          {html_body}
        </div>
      </body>
    </html>
    """
    return html_email


def notify_email(**context):
    """
    Formats the AI advice file as an HTML email and sends it to designated recipients.

    The function:
    - Converts the advice file from Markdown to styled HTML.
    - Composes the email with subject and attachments.
    - Sends the email using Airflow's send_email utility.
    """
    exec_datetime = context["execution_date"]
    exec_date = exec_datetime.strftime("%Y-%m-%d")
    exec_month = exec_datetime.strftime("%Y-%m")

    file_path = f"{ai_content}{exec_month}/{exec_date}_llm_advice.txt"

    subject = f"AI Analysis Report on Stock News Today {exec_date}"
    html_content = format_file_to_html(file_path)
    # Specify file attachments for the email
    attachments = [file_path]
    
    send_email(
        to=["udohchigozie2017@gmail.com"],
        subject=subject,
        html_content=html_content,
        files=attachments
    )


# Define the DAG and its tasks using a context manager
with DAG(dag_id="Stock_sentiment_analysis", default_args=default_args, 
         schedule_interval="@daily", catchup=True) as dag:

    # Task: Retrieve news data from the API and save as CSV
    get_data = PythonOperator(
        task_id='get_news_data', 
        python_callable=connect_to_api_csv
    )

    # Task: Filter and label news data for stock relevance
    filter_data = PythonOperator(
        task_id='get_only_stock_news', 
        python_callable=filter_news
    )

    # Task: Load filtered data into the database (Vertica)
    load_to_table = SQLExecuteQueryOperator(
        task_id="load_to_vertica",
        conn_id="vertica",
        sql=r"""COPY News_DB.Full_News_Table from LOCAL '{{ params.labeled_data_path }}{{ ds[:7] }}/{{ ds }}_news_file_labeled.jsonl' PARSER fjsonparser();""",
        params={'labeled_data_path': labeled_data_path}
    )

    # Task: Decide whether to compress raw data files
    compress_or_not = BranchPythonOperator(
        task_id='compress_choice', 
        python_callable=compress_choice
    )

    # Set task dependencies: get data -> filter data -> load data -> branch decision
    get_data >> filter_data >> load_to_table >> compress_or_not

    # Task: Compress and remove files if the branch condition is met
    compress_and_remove_files = PythonOperator(
        task_id='compress_and_remove_files', 
        python_callable=compress_and_remove_files_
    )

    # Dummy task: No action needed if compression is not required
    do_nothing = DummyOperator(task_id='do_nothing_start_dbt', dag=dag)

    # Task: Execute DBT commands to separate stock and non-stock news and run DBT models
    start_dbt = BashOperator(
        task_id="separate_stock_news",
        bash_command="""
        rm {{ params.stock_path }} {{ params.non_stock_path }} &&
        
        echo "{% raw %}{{ config(materialized='incremental', unique_key='content') }} 
        select * from News_DB.Full_News_Table 
        {% if is_incremental() %}
            where label = {% endraw %}{{ params.one }}{% raw %} and date = '{% endraw %}{{ ds }}{% raw %}'
        {% endif %}{% endraw %}" >> {{ params.stock_path }} &&

        echo "{% raw %}{{ config(materialized='incremental', unique_key='content') }} 
        select * from News_DB.Full_News_Table 
        {% if is_incremental() %}
            where label = {% endraw %}{{ params.zero }}{% raw %} and date = '{% endraw %}{{ ds }}{% raw %}'
        {% endif %}{% endraw %}" >> {{ params.non_stock_path }} &&

        cd {{ params.dbt_path }} && dbt run
    """,
        params={
            "stock_path": stock_news_path,
            "non_stock_path": non_stock_news_path,
            "dbt_path": dbt_path,
            "one": 1,
            "zero": 0
        },
        trigger_rule="none_failed"
    )

    # Define branch dependencies: compress or not -> start DBT
    compress_or_not >> [compress_and_remove_files, do_nothing] >> start_dbt

    # Task: Extract stock news content from the database for AI processing
    extract_news_info = SQLExecuteQueryOperator(
        task_id="extract_stock_news_info",
        conn_id="vertica",
        sql=r"""select content from News_DB.Stock_News where date='{{ ds }}' and label=1;""",
        do_xcom_push=True
    )

    # Task: Generate AI-based stock recommendations using LLM
    get_ai_recommendation = PythonOperator(
        task_id='LLM_advice', 
        python_callable=LLM_advice
    )

    # Set dependency: after DBT, extract news info then generate AI recommendations
    start_dbt >> extract_news_info >> get_ai_recommendation

    # Task: Send the AI-generated stock insights via email
    notify = PythonOperator(
        task_id="send_ai_stock_insight", 
        python_callable=notify_email
    )
    
    # Set final dependency: after AI recommendations, send notification email
    get_ai_recommendation >> notify