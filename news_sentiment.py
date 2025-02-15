#imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.email import send_email
import datetime as dt
from airflow.operators.dummy import DummyOperator
import airflow
from airflow.models import Variable
from joblib import load
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import nltk
nltk.download("wordnet", quiet=True)
nltk.download('averaged_perceptron_tagger_eng', quiet=True)
from nltk.tag import pos_tag
from nltk.tokenize import word_tokenize
import cloudpickle
from functions import lemmatized
import shutil
from google import genai
from google.genai import types
import markdown

from bs4 import BeautifulSoup

#for connecting to the API server
import requests

# I am saving the API key in the system with setx NEWS_API_KEY "content". So, I need to get it back
import os

# using regular expression to extract date from the Json file
import re
import datetime as dt
#from datetime.datetime import strptime
import pathlib
import json
import pandas as pd

# Retrieve credentials from Airflow Variables
# The variables are stored as a single variable in the json syntax so we don't have to make multiple connections to 
# get all our variables
# to create in linux, we use export var=val, in windows cmd setx var=val

#command used 
#airflow variables set email_vars '{"smtp_user":"udohchigozie2017@gmail.com", "smtp_password":"", "smtp_url":"smtp://smtp.gmail.com:587"}' -j --description "These are the email variables"
#airflow variables set NEWS_API_KEY '' --description "News api key"
#airflow variables set GEMINI_API_KEY '' --description "Gemini api key"


#api_key=os.getenv("NEWS_API_KEY")
api_key=Variable.get("NEWS_API_KEY")
airflow_vars_str=Variable.get("email_vars", deserialize_json=True)
airflow_vars = json.loads(airflow_vars_str)

gemini_api_key=Variable.get("GEMINI_API_KEY")

SMTP_URL=airflow_vars['smtp_url']
SMTP_USER = airflow_vars['smtp_user']
SMTP_PASSWORD = airflow_vars['smtp_password']

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2024, 12, 1),
}

parent_path="/mnt/c/Users/User/News-Project/Stock-Sentiment-Analysis-with-Apache-Airflow-and-Google-Gemini/"
data_path=f"{parent_path}data/"
labeled_data_path=f"{parent_path}labeled_data/"
model_path=f"{parent_path}models/"
compressed_data_path=f"{parent_path}compressed_data/"
ai_content=f"{parent_path}ai_analysis/"

#dbt file paths
stock_news_path="/mnt/c/Users/User/Documents/My_DBT/Airflow_Stock_Sentiment_Project/models/airflow_stock_sentiment_models/Stock_News.sql"
non_stock_news_path="/mnt/c/Users/User/Documents/My_DBT/Airflow_Stock_Sentiment_Project/models/airflow_stock_sentiment_models/Non_Stock_News.sql"
dbt_path="/mnt/c/Users/User/Documents/My_DBT/Airflow_Stock_Sentiment_Project"

stock_news_dbt_file_content="""{{ config(materialized='incremental',
unique_key='content') }}

select * from News_DB.Full_News_Table 

{% if is_incremental() %}
    where label =1 and date={{ ds }}
{% endif %}"""

non_stock_news_dbt_file_content="""{{ config(materialized='incremental',
unique_key='content') }}

select * from News_DB.Full_News_Table 

{% if is_incremental() %}
    where label =1 and date={{ ds }}
{% endif %}"""

# Generally, it seems there's constituents of the url, my brian is trying to remember the basic web concepts I learned before.

def get_full_article(url):
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            paragraphs = soup.find_all('p')
            full_text = "\n".join([p.get_text() for p in paragraphs])
            return full_text
    except requests.exceptions.RequestException:
        return None  # Return None if request fails

    return None


def connect_to_api_csv(**context):
    
    # I want the DAG to be idempotent so that the model will use the same data to train the model or to do anything in any case 
    # need to backfill
    
    exec_datetime = context["execution_date"]    #<datetime> type with timezone
    exec_date=exec_datetime.strftime("%Y-%m-%d")
    exec_month=exec_datetime.strftime("%Y-%m")

    # One idea I had to implement branching had to do with saving the csvs on sunday only but this affects atomicity. If some 
    # one decided to run the train model task only, how can he do that when they data is from teh past and not abailable?
    # I was thinking that

    # dropping csv if exists
    file_name=str(exec_date)+'_news_file.txt'

    # dropping file if exists
    if os.path.exists(f"{data_path}{exec_month}/{file_name}"):
        os.remove(f"{data_path}{exec_month}/{file_name}")

    # creating dirs needed
    pathlib.Path(f"{data_path}{exec_month}").mkdir(parents=True, exist_ok=True)

    url=(f"https://newsapi.org/v2/everything?from={exec_date}&to={exec_date}&language=en&q=(market OR stock)&apiKey={api_key}")
    response = requests.get(url)
    resp_dict=response.json()
    articles=resp_dict['articles']

    empty_json={'date':[], 'content':[]}

    for article in articles:
        published_date=article['publishedAt']
        published_date=re.findall('[\d-]+', published_date)[0]
        content_url=article['url']
        content=get_full_article(content_url)

        #if content is none, get the description
        if content==None:
            content_descrp=article['description']
            empty_json['content'].append(content_descrp)
        else:
            empty_json['content'].append(content)
        empty_json['date'].append(published_date)
            
    news_df = pd.DataFrame(empty_json)
    news_df_shape=news_df.shape
    if news_df_shape[0]==0:
        new_row = [exec_date, "No data for this day"]
        news_df.loc[len(news_df)] = new_row
        news_df.to_csv(f"{data_path}{exec_month}/{file_name}")
    else:
        news_df.to_csv(f"{data_path}{exec_month}/{file_name}")

with open(f"{model_path}/tfidf_vectorizer_300.pkl", "rb") as f:
        tfidf_loaded=cloudpickle.load(f)

def filter_news(**context):

    exec_datetime = context["execution_date"]    #<datetime> type with timezone
    exec_date=exec_datetime.strftime("%Y-%m-%d")
    exec_month=exec_datetime.strftime("%Y-%m")

    file_name=str(exec_date)+'_news_file.txt'

    # creating dirs needed
    pathlib.Path(f"{labeled_data_path}{exec_month}").mkdir(parents=True, exist_ok=True)
    pathlib.Path(model_path).mkdir(parents=True, exist_ok=True)

    # vectorize the dataset for the day
    # loading the dataset
    df=pd.read_csv(f"{data_path}{exec_month}/{file_name}")
    df.drop('Unnamed: 0', axis=1, inplace=True)
    df.dropna(inplace=True)

    # Transform using the loaded vectorizer
    char_array = tfidf_loaded.transform(df.content).toarray()
    frequency_matrix = pd.DataFrame(char_array, columns= tfidf_loaded.get_feature_names_out())

    # load the model
    model=load(f"{model_path}/lgb.joblib")
    model_pred=model.predict(frequency_matrix)
    df['label']=model_pred
    df["content"] = df["content"].str.replace("\n", " ", regex=True)

    # Save as line-delimited JSON (.jsonl)
    df.to_json(f"{labeled_data_path}{exec_month}/"+file_name.replace('.txt', '_labeled.jsonl'), orient="records", lines=True)

def compress_choice(**context):
    exec_datetime = context["next_execution_date"]
    exec_day=exec_datetime.day #strftime("%Y-%m-%d")
    if exec_day==1:
        return 'compress_and_remove_files'
    else:
        return 'do_nothing_start_dbt'

def compress_and_remove_files_(**context):
    next_exec_datetime = context["next_execution_date"]
    next_exec_day=next_exec_datetime.day #strftime("%Y-%m-%d")

    exec_datetime = context["execution_date"]
    exec_month=exec_datetime.strftime("%Y-%m")

    compressed_file_name=f"{exec_month}_raw_news_data"

    #create compressed dir if it doesn't exist
    if os.path.exists(f"{compressed_data_path}"):
        pass
    else:
        os.makedirs(f"{compressed_data_path}")

    if next_exec_day==1 and os.path.exists(f"{compressed_data_path}{compressed_file_name}.zip"):
        pass
    elif next_exec_day==1:
        # Path to the folder you want to compress
        folder_to_compress = f"{data_path}{exec_month}"

        # Output archive path (without the .zip extension)
        archive_name = f"{compressed_data_path}{compressed_file_name}"

        # Create a zip archive of the folder
        shutil.make_archive(archive_name, 'zip', folder_to_compress)

        #remove folders
        #os.removedirs(f"{data_path}{exec_month}")
        shutil.rmtree(f"{data_path}{exec_month}")
    else:
        pass
        #start_dbt

#def start_dbt_():
    #print('done')

def LLM_advice(**context):
    exec_datetime = context["execution_date"]
    exec_date=exec_datetime.strftime("%Y-%m-%d")
    exec_month=exec_datetime.strftime("%Y-%m")

    llm_output=f"{ai_content}{exec_month}/{exec_date}_llm_advice.txt"

    sys_instruct="You are a Stock Sentiment Analyst. Your work is to summarize news data and give stock investment advice in a nicely formated way."
    client = genai.Client(api_key=gemini_api_key)
    news= context["task_instance"].xcom_pull(
    task_ids="extract_stock_news_info", key="return_value"
    )
    news_as_text=''
    for ind, cont in enumerate(news):
        news_as_text+=f"News_{ind+1} \n '\n'.join{[str(a) for a in cont]} \n"

    response = client.models.generate_content(
        model="gemini-2.0-flash",
        config=types.GenerateContentConfig(
            system_instruction=sys_instruct),
        contents=["""These are the recent stock news for today. Please in a summarized way, tell me 1. The stocks mentioned today 2. Which of these stocks had a bad sentiment 3. Which had good sentiment 4. Which would you advice me to keep an eye on \n"""+news_as_text]
    )
    if os.path.exists(f"{ai_content}{exec_month}"):
        pass
    else:
        os.makedirs(f"{ai_content}{exec_month}")
    with open(llm_output, "w") as f:
        f.write(response.text)

'''def send_email(**context):
    exec_datetime = context["execution_date"]
    exec_date=exec_datetime.strftime("%Y-%m-%d")
    exec_month=exec_datetime.strftime("%Y-%m")

    # creates SMTP session
    s = smtplib.SMTP('smtp.gmail.com', 587)
    # start TLS for security
    s.starttls()
    # Authentication
    s.login(SMTP_USER, SMTP_PASSWORD)
    # message to be sent
    with open(f"{ai_content}{exec_month}/{exec_date}_llm_advice.txt", 'r', encoding='utf-8') as f:
        body=f.read()
    
    subject = f"AI Analysis Report on Stock News Today {exec_date}"
    message = f"Subject: {subject}\n\n{body}"

    # sending the mail
    s.sendmail(SMTP_USER, SMTP_USER, message.encode('utf-8'))
    # terminating the session
    s.quit()'''

def format_file_to_html(file_path):
    """
    Reads a plain text file (formatted in Markdown) and converts it into a styled HTML email.
    """
    # Read the raw text from the file
    with open(file_path, 'r', encoding='utf-8') as f:
        raw_text = f.read()
    
    # Convert the Markdown text to HTML
    html_body = markdown.markdown(raw_text)
    
    # Wrap the converted HTML in a complete HTML template with inline CSS
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
    exec_datetime = context["execution_date"]
    exec_date=exec_datetime.strftime("%Y-%m-%d")
    exec_month=exec_datetime.strftime("%Y-%m")

    file_path=f"{ai_content}{exec_month}/{exec_date}_llm_advice.txt"

    subject = f"AI Analysis Report on Stock News Today {exec_date}"
    html_content = format_file_to_html(file_path)
    # If you want to attach a file, you can add it as a list of file paths:
    attachments = [file_path]
    
    send_email(
        to=["udohchigozie2017@gmail.com"],
        subject=subject,
        html_content=html_content,
        files=attachments
    )


# mount volume in local to vertica (it wasn't necessary actually except I was using ssh to first log into vertica server and then
    #run the code)
#docker run -p 6433:5433 -p 6444:5444 --mount "type=bind,source=C:/Users/User/News-Project/labeled_data,target=/home/data,ro" --name vertica_ce_synced vertica/vertica-ce
#needed to add connection details
#airflow connections add --conn-json '{"conn_type": "vertica", "login": "dbadmin", "password": "", "host": "127.0.0.1", "port": 6433, "schema": "VMart"}' vertica
#used this to enable testing connection export AIRFLOW__CORE__TEST_CONNECTION=Enabled
#then had ro pip install vertica I recall that recent version of airflow don't install these hooks any more
#pip install apache-airflow-providers-vertica
#the test suceeded after running airflow connections test vertica (which was the conn_id name I used when creating the
#connection)

# VERY USEFUL IN DEBUGGING SUDO issues IN KALI https://superuser.com/questions/1644520/apt-get-update-issue-in-kali
# Had to install pip install protobuf==4.25.3 to buy pass some issue with dbt

# Using with clause and specifying DAG structure
with DAG(dag_id="Stock_sentiment_analysis", default_args=default_args, 
         schedule_interval="@daily", catchup=True) as dag:
    get_data=PythonOperator(task_id='get_news_data', python_callable=connect_to_api_csv)
    filter_data=PythonOperator(task_id='get_only_stock_news', python_callable=filter_news)
    load_to_table=SQLExecuteQueryOperator(
        task_id="load_to_vertica",
        conn_id="vertica",
        sql=r"""COPY News_DB.Full_News_Table from LOCAL '{{ params.labeled_data_path }}{{ ds[:7] }}/{{ ds }}_news_file_labeled.jsonl' PARSER fjsonparser();""",
        params={'labeled_data_path':labeled_data_path}
    )
    compress_or_not=BranchPythonOperator(task_id='compress_choice', python_callable=compress_choice)
    get_data>>filter_data>>load_to_table>>compress_or_not

    compress_and_remove_files=PythonOperator(task_id='compress_and_remove_files', python_callable=compress_and_remove_files_)
    do_nothing=DummyOperator(task_id='do_nothing_start_dbt', dag=dag)

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


    compress_or_not>>[compress_and_remove_files, do_nothing]>>start_dbt

    
    extract_news_info=SQLExecuteQueryOperator(
        task_id="extract_stock_news_info",
        conn_id="vertica",
        #change this table after dbt task is created
        sql=r"""select content from News_DB.Stock_News where date='{{ ds }}' and label=1;""",
        do_xcom_push=True
        #params={'labeled_data_path':labeled_data_path}
    )
    get_ai_recommendation=PythonOperator(task_id='LLM_advice', python_callable=LLM_advice)
    start_dbt>>extract_news_info>>get_ai_recommendation

    #notify=PythonOperator(task_id='send_ai_stock_insight', python_callable=send_email)
    notify= PythonOperator(task_id="send_ai_stock_insight", python_callable=notify_email)
    '''notify = EmailOperator( 
        task_id='send_ai_stock_insight', 
        to='chigozie.udoh.244025@unn.edu.ng', 
        subject='ingestion complete', 
        html_content="Date: {{ ds }}"
        )'''
    get_ai_recommendation>>notify
    #get_data>>filter_data>>load_to_table>>notify