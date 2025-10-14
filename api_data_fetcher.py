"""
Module for fetching news data from News API
"""
import requests
import pandas as pd
import re
import sys
import os
import pathlib
from bs4 import BeautifulSoup
from main import main


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
        return None
    return None


def fetch_news_data(api_key, exec_date, data_path):
    """
    Connects to the news API, retrieves news articles for a given day,
    processes the articles, and saves them as a CSV file.

    Parameters:
        api_key (str): News API key
        exec_date (str): Execution date in YYYY-MM-DD format
        data_path (str): Path to save the data
    
    Returns:
        str: Path to the saved CSV file
    """
    exec_month = exec_date[:7]  # Extract YYYY-MM
    file_name = f'{exec_date}_news_file.txt'
    
    # Drop file if it exists
    if os.path.exists(f"{data_path}{exec_month}/{file_name}"):
        os.remove(f"{data_path}{exec_month}/{file_name}")
    
    # Create required directory for the month
    pathlib.Path(f"{data_path}{exec_month}").mkdir(parents=True, exist_ok=True)
    
    # Build the news API URL
    url = f"https://newsapi.org/v2/everything?from={exec_date}&to={exec_date}&language=en&q=(market OR stock)&apiKey={api_key}"
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
    
    output_path = f"{data_path}{exec_month}/{file_name}"
    news_df.to_csv(output_path)
    
    return output_path`
    
if __name__ == "__main__":
    main(fetch_news_data, segment="data")