# news_data_pipeline
## Data preprocessing
This part contains the basic data and also original data. We designed four entities and used these entities to grab the original raw data.
✅ done

## Airflow construction 
Working on it.

To set up virtual environment, I used the `python=3.11.7`. Probably you need to set up your environment according to me.

### Set up the virtual environment
First thing you need to do is build a virtual environment.
```bash
python3.11 -m venv .venv
```
Secondly, echo the AIRFLOW_HOME and change it:
```bash
export AIRFLOW=$(pwd)
```
Then install the `airflow` using the below command:
```bash
pip install "apache-airflow==2.8.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.11.txt"
```
Then setup the airflow connection:
```bash
airflow standalone
```