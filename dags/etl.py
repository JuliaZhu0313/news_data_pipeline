from airflow.decorators import dag,task
from datetime import datetime,timedelta
import sys 
import os 
import pandas as pd
import yfinance as yf
import requests_cache
import numpy as np
import json
from json.decoder import JSONDecodeError
import requests

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
    @task(task_id = 'Company_Extract')
    def company_extract():
        companies_df = pd.read_csv("datasets/constituents.csv")
        company_df = pd.DataFrame(columns=['id', 'name', 'fullTimeEmployees', 'Industry', 'Country'])
        #read in companies to use 
        company_tickers = companies_df["Symbol"].tolist()
        company_rawinfo = []
        for ticker in company_tickers:
            try:
                company_dict = {"id":ticker}
                company = yf.Ticker(ticker)
                information = company.info
                company_rawinfo.append(information)
                company_dict.update({'name':information.get('shortName', None), 'fullTimeEmployees':information.get('fullTimeEmployees', None),
                            'Industry':information.get('sector', None), 'Country':information.get('country', None)})
                company_df = pd.concat([company_df, pd.DataFrame([company_dict])], ignore_index=True)
            except Exception as e:
                print(f"Error retrieving data for {ticker}: {e}")
        
        company_rawfile = 'Company/Company_extract.csv'
        company_df.to_csv(company_rawfile, index=False) 
        return company_rawfile
    
    @task(task_id = 'Company_Transform')
    def company_transform(company_rawfile):
        company_df = pd.read_csv(company_rawfile)
        company_df['fullTimeEmployees'] = company_df['fullTimeEmployees'].astype(float)
        company_file = 'Company/Company.csv'        
        company_df.to_csv(company_file, index=False) 
        return company_file
    
    @task(task_id = 'Finance_Extraction')
    def finance_extraction():
        #read in companies to use 
        companies_df = pd.read_csv("../datasets/constituents.csv")
        company_tickers = companies_df["Symbol"].tolist()
        finance_situation_df = pd.DataFrame(columns=['company_id', 'AuditRisk', 'Dividend rate', 'Dividend Yield', 
                                             'Payout rate', 'Beta', 'Market Cap', 'profit margins', 'short ratio', 
                                             'quick ratio', 'current ratio', 'debtToEquity'])
        for ticker in company_tickers:
            try:
                finance_dict = {"company_id":ticker}
                company = yf.Ticker(ticker)
                information = company.info
                finance_dict.update({'AuditRisk': information.get('auditRisk', None), 
                                    'Dividend rate': information.get('dividendRate', None), 
                                    'Dividend Yield': information.get('dividendYield', None),
                                    'Payout rate': information.get('payoutRatio', None), 
                                    'Beta': information.get('beta', None), 
                                    'Market Cap': information.get('marketCap', None),
                                    'profit margins': information.get('profitMargins', None), 
                                    'short ratio': information.get('shortRatio', None),
                                    'quick ratio': information.get('quickRatio', None), 
                                    'current ratio': information.get('currentRatio', None),
                                    'debtToEquity': information.get('debtToEquity', None)})
                finance_situation_df = pd.concat([finance_situation_df, pd.DataFrame([finance_dict])], ignore_index=True)
            except Exception as e:
                print(f"Error retrieving data for {ticker}: {e}")

        finance_rawfile = 'Finance_Situation/Finance_Situation_raw.csv'
        finance_situation_df.to_csv(finance_rawfile, index = False)
        return finance_rawfile
    
    @task(task_id = 'Finance_Transform')
    def finance_transform(company_file,finance_rawfile):
        company_df = pd.read_csv(company_file)
        finance_situation_df = pd.read_csv(finance_rawfile)
        columns = finance_situation_df.columns.tolist()
        finance_situation_df[columns[1:]]= finance_situation_df[columns[1:]].astype(float)
        # calculate market cap ratio
        merged_df = pd.merge(company_df, finance_situation_df, left_on='id', right_on='company_id')
        industry_market_cap = merged_df.groupby('Industry')['Market Cap'].sum()
        finance_situation_df['Market Cap'] = merged_df.apply(lambda row: (row['Market Cap'] / industry_market_cap[row['Industry']]) * 100, axis=1)
        
        finance_file = 'Finance_Situation/Finance_Situation.csv'       
        finance_situation_df.to_csv(finance_file, index = False)
        return finance_file
      
    @task(task_id = 'News_Extraction')
    def news_extractions():
        api_list = [
                    '3bff9734cfbf4e1aa7b60c9fc224452d',
                    'ae48b5cb8ffe44c2b061a25785c74d97',
                    '65e16b9111744ab4840e59166dd659b9',
                    '4a8e398f23024576ae5e29c505e07270',
                    '81f3a64644484e32b542e9f48269b489',
                    '8a7893660af4483ba4da4b18349d6c79',
                    '9a49f2ddb5134355829ac5ae6946d718',
                    'e5b1e9fd2dee4f94aa30f7bfe73f21e4',
                    '0441e2cea6b84a5d976b4c0a037a9e60',
                    '7251ac941bb64e8cbee0eb58f00883a1',
                    'e672029cbbc944b586f436bd12a912d7'
                ]
        companies = pd.read_csv('datasets/constituents.csv')
        company = companies[['Security', 'Symbol']]
        ticker = company['Symbol'].to_list()
        ticker_dict = {}
        for i in ticker:
            ticker_dict[i] = []
        ticker2 = company['Security'].to_list()
        ticker_dict2 = {}
        for i in ticker2:
            ticker_dict2[i] = []
        # Get company_news 
        df = pd.DataFrame()
        empty_list = []
        base_url = "https://api.worldnewsapi.com/search-news"
        counter = 0
        #api_key = "3bff9734cfbf4e1aa7b60c9fc224452d"
        for comp in company['Security']:
            try:
                api_key = api_list[counter]
                name = "ORG: " + comp
                params = {
                "language": "en",  
                "api-key": api_key, 
                # very few weekly data
                "earliest-publish-date":"2024-01-01",
                "latest-publish-date" : "2024-04-01",
                'text': name,
                "number":100
                }
                response = requests.get(base_url, params=params)
                data = response.json()
            except JSONDecodeError:
                print(f"took extremely long to get data for {comp}")
                continue
            try:
                df1 = pd.DataFrame(data['news'])
            except KeyError:
                try:
                    if (data['code']) == 402:
                        if counter == len(api_list) - 1:
                            print(comp)
                            print(api_key)
                            break
                        else:
                            counter = counter + 1
                            api_key = api_list[counter]
                            print(f"update api key to {api_key}")
                            params = {
                            "language": "en",  
                            "api-key": api_key, 
                            # very few weekly data
                            "earliest-publish-date":"2024-01-01",
                            "latest-publish-date" : "2024-04-01",
                            'text': name,
                            "number":100
                            }
                            response = requests.get(base_url, params=params)
                            data = response.json()
                    else:
                        print(data)
                except KeyError:
                    print("No news found")
                    df1 = pd.DataFrame()

            try:
                ticker_dict2[comp] = df1['id'].to_list()
            except KeyError:
                print("No company data")
                empty_list.append(comp)
                ticker_dict2[comp] = []

            df = pd.concat([df, df1], ignore_index=True)
        
        news_file = "news/news2.csv"
        df.to_csv(news_file, index = False)
        for (key1, value1), (key2, value2) in zip(ticker_dict2.items(), ticker_dict.items()):
            ticker_dict[key2] = value1
        with open("company_news/tickers.json", "w") as json_file:
            json.dump(ticker_dict, json_file)
        
        return news_file
    
    def stock_extract():
        companies = pd.read_csv('datasets/constituents.csv')
        ticker = companies['Symbol'].to_list()
        
        session = requests_cache.CachedSession(cache_name='cache', backend='sqlite')
        session.headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
                        'Accept': 'application/json;charset=utf-8'}

        # Remember to change start_date and end_date in DAG
        def get_data_for_multiple_stocks(tickers, start_date='2024-01-01',end_date= '2024-04-01', session= session):
            stocks = dict()
            for ticker in tickers:
                data = yf.download(ticker, start=start_date, end=end_date, session=session)
                data.insert(0, 'Ticker', ticker)
                data = data.reset_index()
                data['Prev Close'] = data['Adj Close'].shift(1)
                data['log_return'] = np.log(data['Adj Close'] / data['Prev Close'])
                df1 = data[['Ticker', 'Date', 'log_return']]
                stocks[ticker] = df1
            return stocks
        
        stocks = get_data_for_multiple_stocks(ticker)
        
        return stocks
    
    def stock_transform(raw_stocks):
        companies = pd.read_csv('datasets/constituents.csv')
        ticker = companies['Symbol'].to_list()
        
        for i in ticker:
            data = raw_stocks[i]
            data['Date'] = data['Date'].apply(str)
            data['Date'] = pd.to_datetime(data['Date'], infer_datetime_format=True)
            raw_stocks[i] = data 
            
        df = pd.DataFrame()
        for (key,value) in raw_stocks.items():
            df1 = pd.DataFrame(value)
            df = pd.concat([df, df1], ignore_index=True) 
        
        stock_file = 'stock/stock_price.csv'
        df.to_csv(stock_file)
        return stock_file
    
    company_rawfile = company_extract()
    company_file = company_transform(company_rawfile)
    
    finance_rawfile = finance_extraction()
    finance_file = finance_transform(company_file,finance_rawfile)
    
    news_file = news_extractions()
    
    raw_stocks = stock_extract()
    stock_file = stock_transform(raw_stocks)

Credit_Risk_Analysis_etl_dag = Credit_Risk_Analysis_taskflow_api_etl()