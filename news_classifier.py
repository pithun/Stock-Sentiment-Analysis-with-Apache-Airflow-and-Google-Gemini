"""
Module for classifying news as stock-related or not
"""
import pandas as pd
import pathlib
import os
from joblib import load
import cloudpickle
from main import main


def load_vectorizer(model_path):
    """Load the pre-trained TF-IDF vectorizer"""
    print("Loading vectorizer...")
    with open(f"{model_path}/tfidf_vectorizer_300.pkl", "rb") as f:
        print("Vectorizer loaded.")
        return cloudpickle.load(f)


def classify_news(data_path, labeled_data_path, model_path, exec_date):
    """
    Filters news data to extract stock-related articles using a pre-trained model.

    Parameters:
        data_path (str): Path to raw news data
        labeled_data_path (str): Path to save labeled data
        model_path (str): Path to ML models
        exec_date (str): Execution date in YYYY-MM-DD format
    
    Returns:
        str: Path to the labeled JSONL file
    """
    exec_month = exec_date[:7]
    file_name = f'{exec_date}_news_file.txt'
    
    # Create required directories
    pathlib.Path(f"{labeled_data_path}/{exec_month}").mkdir(parents=True, exist_ok=True)
    pathlib.Path(model_path).mkdir(parents=True, exist_ok=True)
    
    # Load and clean the dataset
    df = pd.read_csv(f"{data_path}/{exec_month}/{file_name}")
    try:
        df.drop('Unnamed: 0', axis=1, inplace=True)
    except KeyError:
        pass
    df.dropna(inplace=True)

    display(df.head())
    
    print("Vectorizing content...")
    # Load vectorizer and transform content
    tfidf_loaded = load_vectorizer(model_path)
    char_array = tfidf_loaded.transform(df.content).toarray()
    frequency_matrix = pd.DataFrame(char_array, columns=tfidf_loaded.get_feature_names_out())
    

    print("Classifying articles...")
    # Load the model and predict labels
    model = load(f"{model_path}/lgb.joblib")
    model_pred = model.predict(frequency_matrix)
    df['label'] = model_pred
    df["content"] = df["content"].str.replace("\n", " ", regex=True)
    
    # Save the labeled data as line-delimited JSON (.jsonl)
    output_path = f"{labeled_data_path}/{exec_month}/{file_name.replace('.txt', '_labeled.jsonl')}"
    df.to_json(output_path, orient="records", lines=True)
    
    return output_path

if __name__ == "__main__":
    main(classify_news, segment="classify")