from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
import json
import requests

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

@dag(dag_id='news_pipeline_taskflow', default_args=default_args, schedule=None, catchup=False, schedule_interval='@weekly')
def news_pipeline():
    @task
    def extraction():
        with open('config.json', 'r') as json_file:
            config = json.load(json_file)
        base_url = config["base_url"]
        api_key = config["api_key"]
        keywords = config["keywords"]

        params = {
            "language": "en",  
            "api-key": api_key,  
            "number":100
        }

        #initialise dataframe
        output_df = pd.DataFrame()

        #iterate over all keywords
        for keyword in keywords:
            params["text"] = keyword 
            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                data = response.json()
                result_df = pd.DataFrame(data['news'])
                output_df = pd.concat([output_df, result_df])

        # store csv file in local machine
        filename = "extracted_news.csv"
        output_df.to_csv(filename, index=True)
        
        return filename
    
    @task
    def data_cleaning(filename: str):
        return filename 
    

    file = extraction()
    cleaned_file = data_cleaning(file)

news_pipeline_taskflow = news_pipeline()
