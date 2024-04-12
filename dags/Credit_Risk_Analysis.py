from airflow.decorators import dag,task
from datetime import datetime,timedelta
import sys 
import os 
import pandas as pd
import yfinance as yf

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

@dag(dag_id='Credit_Risk_Analysis',default_args=default_args,schedule=None,tags=['Project'])
def  Credit_Risk_Analysis_taskflow_api_etl():
    @task(task_id = 'Company data Extraction')
    def company_extraction():
        #read in companies to use 
        companies_df = pd.read_csv("../company/constituents.csv")
        company_tickers = companies_df["Symbol"].tolist()

        company_df = pd.DataFrame(columns=['id', 'name', 'fullTimeEmployees', 'Industry', 'Country'])
        for ticker in company_tickers:
            try:
                company_dict  = {'id':ticker}
                company  = yf.Ticker(ticker)
                information = company.info
                company_dict.update({'name':information.get('shortName', None), 'fullTimeEmployees':information.get('fullTimeEmployees', None),
                                'Industry':information.get('sector', None), 'Country':information.get('country', None)})
                company_df = pd.concat([company_df, pd.DataFrame([company_dict])], ignore_index=True)
            except Exception as e: 
                print(f"Error retrieving data for {ticker}: {e}")
        # Convert data types
        company_df['fullTimeEmployees'] = company_df['fullTimeEmployees'].astype(float)
        company_df.to_csv('../Company/Company.csv', index=False) 

    company_extraction

Credit_Risk_Analysis_etl_dag = Credit_Risk_Analysis_taskflow_api_etl()