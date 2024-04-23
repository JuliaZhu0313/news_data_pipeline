# streamlit_app.py
from pandas.core.arrays.integer import Int64Dtype
import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import seaborn as sns
import matplotlib.pyplot as plt
import datetime
from PIL import Image
import os
import numpy as np
import pandas as pd
import plotly.express as px
from langdetect import detect
from wordcloud import WordCloud
import re
path = os.getcwd()
print(path)

# Page config
st.set_page_config(page_icon="💰", page_title="StockAnalysis", layout="wide")

client = bigquery.Client()
@st.cache_data

def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    # Convert to list of dicts. Required for st.cache_data to hash the return value.
    rows = [dict(row) for row in rows_raw]
    return rows

hide_default_format = """
       <style>
       #MainMenu {visibility: hidden; }
       footer {visibility: hidden;}
       </style>
       """
st.markdown(hide_default_format, unsafe_allow_html=True)

dataset_id = 'raw_data'
dataset_ref = client.dataset(dataset_id)
stock_price_id = 'StockData'
table_ref = dataset_ref.table(stock_price_id)
table = client.get_table(table_ref) 
stock_price_query = run_query(f"SELECT * FROM `{dataset_id}.{stock_price_id}`")
stock_price = pd.DataFrame(stock_price_query)
news_id = 'News'
news_query = run_query(f"SELECT * FROM `{dataset_id}.{news_id}`")
news = pd.DataFrame(news_query)
finance_situation_id = 'FinanceSituation'
finance_situation_query = run_query(f"SELECT * FROM `{dataset_id}.{finance_situation_id}`")
finance_situation = pd.DataFrame(finance_situation_query)
company_id = 'Company'
company_query = run_query(f"SELECT * FROM `{dataset_id}.{company_id}`")
company = pd.DataFrame(company_query)

chinese_pattern = re.compile(r'[\u4e00-\u9fff]+')
filtered_news = news[news['text'].apply(lambda x: not bool(chinese_pattern.search(x)))]

st.title("StockAnalysis")
st.markdown('''
            <style>
            .st-c7 {
                flex-shrink: 0;
                position: absolute;
                opacity: 0;
                cursor: pointer;
                height: 0;
                width: 0;
            }
            </style>
            ''', unsafe_allow_html=True)

company_list = list(stock_price['Ticker'].unique())
selected_company = st.selectbox('Choose the company for visualization', (x for x in company_list), index = 0)
st.write(f"Selected Company: {selected_company}")
c1 = st.container()
with c1:
    company_info = company[company['id'] == selected_company]
    name = company_info['name'].iloc[0]
    employee = company_info['fullTimeEmployees'].iloc[0]
    industry = company_info['Industry'].iloc[0]
    country = company_info['Country'].iloc[0]
    st.write(f"Company name: **{name}**")
    st.write(f"Company full time employee numbers: **{employee}**")
    st.write(f"Industry: **{industry}**")
    st.write(f'Country: **{country}**')
st.markdown("""---""")
c4 = st.container()
with c4:
    selected_stock = stock_price[stock_price['Ticker'] == selected_company][1:]
    fig = px.line(selected_stock, x='Date', y='log_return', title='Log Return Over Time')
    fig.update_traces(mode='markers+lines', marker=dict(size=8))
    fig.update_layout(hovermode='x',width=800, height=600)
    st.plotly_chart(fig)
    total_news = ''.join(list(filtered_news['text']))
st.markdown("""---""")
c3 = st.container()
with c3:
    situation = finance_situation[finance_situation['company_id'] == selected_company]
    filtered_situation = {key: situation[key].iloc[0] for key in list(situation.columns) if isinstance(situation[key].iloc[0], float) and key!='company_id'}
    st.bar_chart(filtered_situation)
st.markdown("""---""")
c0 = st.container()
    
with c0:
    wordcloud = WordCloud(width=800, height=400,background_color = 'white').generate(total_news)
    plt.figure(figsize=(6, 3))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    st.pyplot()

