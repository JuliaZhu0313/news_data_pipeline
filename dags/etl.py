from airflow.decorators import dag,task
from datetime import datetime,timedelta
import sys 
import os 
import pandas as pd
import yfinance as yf
import requests_cache
import numpy as np
import json

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=10)
}

@dag(dag_id='Credit_Risk_Analysis',default_args=default_args,schedule=None,tags=['Project'])
def  Credit_Risk_Analysis_taskflow_api_etl():
    @task(task_id = 'Company_Finance_Retreive')
    def company_extract():
        companies_df = pd.read_csv("datasets/constituents.csv")
        company_df = pd.DataFrame(columns=['id', 'name', 'fullTimeEmployees', 'Industry', 'Country'])
        #read in companies to use 
        company_tickers = companies_df["Symbol"].tolist()
        company_rawinfo = []
        for ticker in company_tickers:
            company_dict = {"id":ticker}
            company = yf.Ticker(ticker)
            information = company.info
            company_rawinfo.append(information)
            company_dict.update({'name':information.get('shortName', None), 'fullTimeEmployees':information.get('fullTimeEmployees', None),
                                 'Industry':information.get('sector', None), 'Country':information.get('country', None)})
            company_df = pd.concat([company_df, pd.DataFrame([company_dict])], ignore_index=True)
            break
            # try:
            #     company_dict = {"id":ticker}
            #     company = yf.Ticker(ticker)
            #     information = company.info
            #     company_rawinfo.append(information)
            #     company_dict.update({'name':information.get('shortName', None), 'fullTimeEmployees':information.get('fullTimeEmployees', None),
            #                  'Industry':information.get('sector', None), 'Country':information.get('country', None)})
            #     company_df = pd.concat([company_df, pd.DataFrame([company_dict])], ignore_index=True)
            #     break
            # except Exception as e:
            #     print(f"Error retrieving data for {ticker}: {e}")
        company_df.to_csv('Company/Company_extract.csv', index=False) 
    
    company_extract()  

Credit_Risk_Analysis_etl_dag = Credit_Risk_Analysis_taskflow_api_etl()