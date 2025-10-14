"""
Main orchestration script for GitHub Actions workflow
"""
import os
import sys
from datetime import datetime
#from api_data_fetcher import fetch_news_data
#from news_classifier import classify_news
#from ai_stock_advisor import extract_stock_news, generate_ai_advice
#from email_notifier import send_email_notification


def main(functions={}, segment = "data"):
    """Main execution function"""
    
    # Define paths
    parent_path = "./"
    data_path = f"{parent_path}data/"
    labeled_data_path = f"{parent_path}labeled_data/"
    model_path = f"{parent_path}models/"
    ai_content_path = f"{parent_path}ai_analysis/"
    
    # Get current date
    exec_date = "2025-10-12"#datetime.now().strftime("%Y-%m-%d")
    
    if segment=="data":
        # Get environment variables
        NEWS_API_KEY = os.environ.get('NEWS_API_KEY')
        
        print(f"Starting workflow for {exec_date}")

        # Step 1: Fetch news data
        print("Step 1: Fetching news data from API...")
        try:
            function = functions.get('fetch_news_data')
            csv_path = function(NEWS_API_KEY, exec_date, data_path)
            print(f"✓ News data saved to {csv_path}")
        except Exception as e:
            print(f"✗ Error fetching news data: {e}")
            sys.exit(1)
    
    elif segment=="classify":
        # Step 2: Classify news
        print("Step 2: Classifying news articles...")
        try:
            function = functions.get('classify_news')
            labeled_path = function(data_path, labeled_data_path, model_path, exec_date)
            print(f"✓ Labeled data saved to {labeled_path}")
        except Exception as e:
            print(f"✗ Error classifying news: {e}")
            sys.exit(1)
    
    elif segment=="reduce, llm and email":
        GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')

        # Step 3: Extract stock news
        print("Step 3: Extracting stock-related news...")
        try:
            function1 = functions.get('extract_stock_news')
            function2 = functions.get('generate_ai_advice')
            stock_news = function1(labeled_data_path, exec_date)
            print(f"✓ Found {len(stock_news)} stock-related articles")
        except Exception as e:
            print(f"✗ Error extracting stock news: {e}")
            sys.exit(1)

        # Step 4: Generate AI advice
        print("Step 4: Generating AI stock advice...")
        try:
            advice_path = function2(GEMINI_API_KEY, stock_news, ai_content_path, exec_date)
            print(f"✓ AI advice saved to {advice_path}")
        except Exception as e:
            print(f"✗ Error generating AI advice: {e}")
            sys.exit(1)
    
        SMTP_URL = os.environ.get('SMTP_URL')
        SMTP_USER = os.environ.get('SMTP_USER')
        SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD')
        TO_EMAIL = os.environ.get('TO_EMAIL', 'udohchigozie2017@gmail.com')

        # Step 5: Send email notification
        print("Step 5: Sending email notification...")
        try:
            function3 = functions.get('send_email_notification')
            function3(SMTP_URL, SMTP_USER, SMTP_PASSWORD, TO_EMAIL, advice_path, exec_date)
            print(f"✓ Email sent successfully to {TO_EMAIL}")
            print(f"\n✓ Workflow completed successfully for {exec_date}")
        except Exception as e:
            print(f"✗ Error sending email: {e}")
            sys.exit(1)