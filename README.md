# Investment Insights DataOps Platform
## Problem Statement
Investors require accurate, timely, and relevant information to make informed decisions. Extracting and organizing data from various online sources is time-consuming. Hence, there is a need for an integrated platform to efficiently gather, analyze, and visualize financial statistics and news for confident investment decisions.
## Project Objective 
Our goal is to develop an interactive investment analytics platform for investors. Key objectives include:
1. Data Extraction and Integration: Implement a pipeline to extract financial data and news from sources like Yahoo Finance and World News API, storing it on cloud data warehouses with automated updates for up-to-date analysis.
2. Visualisation and Analysis: Create intuitive dashboards with charts and tables to present financial metrics, stock trends, and news sentiment analysis.
3. User-friendly Interface: Design an interface allowing users to input company names and access relevant information effortlessly.
## Business Values
1. Enhanced Decision-making: Provide comprehensive, up-to-date data and analysis for informed decisions.
2. Risk Mitigation: Assess company profitability and reputation to mitigate investment risks.
3. Efficiency and Time Savings: Streamline data collection, analysis, and presentation, saving investors time and resources.
## Data Extraction
Two APIs were used for data extraction: 
1. [World News API](https://worldnewsapi.com/)
2. [Yahoo Finance API](https://developer.yahoo.com/api/)

Four tables were constructed using data from the two APIs.
1. Company: [click to see the code for data extraction & sample data](/Company)
2. Finance Situation: [click to see the code for data extraction & sample data](/Finance_Situation)
3. Company News: [click to see the code for data extraction & sample data](/company_news)
4. Stock Price : [click to see the code for data extraction & sample data](/stock_price)
   
## Data Transformation & Data Loading 
All the tables have been cleaned, transformed, and loaded into the Data Warehouse (BigQuery). Code for this can be found in: [BigQuery](/BigQuery)
After loading into the data warehouse, we performed data aggregation, which involved constructing the **Industry** table. This table combines data from both the News and Finance Situation tables, including average financial ratios and average sentiment scores for each industry that companies belong to. [Data Aggregation](/BigQuery)

## Data Pipeline
<img width="608" alt="Screenshot 2024-04-26 at 10 10 10 AM" src="https://github.com/JuliaZhu0313/news_data_pipeline/assets/77180636/b7538932-f495-4e80-b8f0-df49eb8eadbe"> 

We've used Airflow to construct a data pipeline that extracts, transforms, and loads data into the warehouse, which is then utilized downstream. By leveraging Airflow's 'schedule' component, we set the schedule interval to three months, enabling the automatic execution of tasks. 
[Data Pipeline](/dags)

## Downstream Application
An interactive dashboard has been developed for users to utilize. It enables users to select the company they are interested in and visualize related news and financial information effectively, encouraging them to make more insightful investment decisions.

Streamlit link : 

