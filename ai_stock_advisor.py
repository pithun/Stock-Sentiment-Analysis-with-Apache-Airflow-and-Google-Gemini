"""
Module for generating AI-based stock advice using Google Gemini
"""
import os
import pandas as pd
from google import genai
from google.genai import types
from main import main


def extract_stock_news(labeled_data_path, exec_date):
    """
    Extract stock-related news from labeled data
    
    Parameters:
        labeled_data_path (str): Path to labeled data
        exec_date (str): Execution date in YYYY-MM-DD format
    
    Returns:
        list: List of stock news content
    """
    exec_month = exec_date[:7]
    file_path = f"{labeled_data_path}{exec_month}/{exec_date}_news_file_labeled.jsonl"
    
    df = pd.read_json(file_path, lines=True)
    stock_news = df[df['label'] == 1]['content'].tolist()
    
    return stock_news


def generate_ai_advice(gemini_api_key, stock_news, ai_content_path, exec_date):
    """
    Generates AI-based stock sentiment advice using Google Gemini.
    
    Parameters:
        gemini_api_key (str): Google Gemini API key
        stock_news (list): List of stock news content
        ai_content_path (str): Path to save AI content
        exec_date (str): Execution date in YYYY-MM-DD format
    
    Returns:
        str: Path to the saved AI advice file
    """
    exec_month = exec_date[:7]
    llm_output = f"{ai_content_path}{exec_month}/{exec_date}_llm_advice.txt"
    
    # Create directory if it doesn't exist
    os.makedirs(f"{ai_content_path}{exec_month}", exist_ok=True)
    
    # System instruction for the language model
    sys_instruct = "You are a Stock Sentiment Analyst. Your work is to summarize news data and give stock investment advice in a nicely formatted way."
    client = genai.Client(api_key=gemini_api_key)
    
    # Format news as text
    news_as_text = ''
    for ind, cont in enumerate(stock_news):
        news_as_text += f"News_{ind+1}\n{cont}\n\n"
    
    # Generate content
    response = client.models.generate_content(
        model="gemini-2.0-flash",
        config=types.GenerateContentConfig(
            system_instruction=sys_instruct),
        contents=["""These are the recent stock news for today. Please in a summarized way, tell me:
1. The stocks mentioned today
2. Which of these stocks had a bad sentiment
3. Which had good sentiment
4. Which would you advice me to keep an eye on\n\n""" + news_as_text]
    )
    
    # Save the AI-generated advice to file
    with open(llm_output, "w") as f:
        f.write(response.text)
    
    return llm_output

if __name__ == "__main__":
    functions = {
        'extract_stock_news': extract_stock_news,
        'generate_ai_advice': generate_ai_advice
    }
    main(functions=functions, segment="reduce_and_llm")