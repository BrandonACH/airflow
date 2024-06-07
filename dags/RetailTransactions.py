import os
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime, timedelta
#from pymongo import MongoClient
import pymongo
import pandas as pd

default_args = {
    'owner': 'Brandon and Eddie',
    'depends_on_past': False,  #  Tasks will run regardless of the status of the same task in the previous DAG run
    'start_date': datetime.now() - timedelta(days=1),
    'email': 'bralfarc7@alumnes.ub.edu',
    'email_on_failure': True, # Send email on failure
    'email_on_retry': False, # Do not send email on retry
    'retries': 1, # Number of retries
    'retry_delay': timedelta(minutes=1) # Time between retries
}

# Define the base directory for data
data_dir = '/opt/airflow/dags/data'

# ======================================== FUNCTIONS ================================================
def download_dataset():
    url = 'https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx'
    output_path = os.path.join(data_dir, 'online_retail.xlsx')

    # Ensure the directory exists, if the directory does not exist, it will be created
    os.makedirs(data_dir, exist_ok=True)

    # Download the dataset
    response = requests.get(url) 

    # Check if download was successful
    if response.status_code == 200: 
        with open(output_path, 'wb') as f:
            f.write(response.content)
        print(f"Dataset downloaded and saved to {output_path}")
    else:
        print(f"Failed to download dataset. Status code: {response.status_code}")

def clean_dataset():
    input_path = os.path.join(data_dir, 'online_retail.xlsx')
    output_path = os.path.join(data_dir, 'cleaned_online_retail.csv')

    # Read xlsx file
    df = pd.read_excel(input_path)
    # Create a mapping dictionary to fill in missing Description values
    stockcode_description_map = (pd.read_excel(input_path)
                                .groupby('StockCode')['Description']
                                .agg(lambda x: x.mode().iloc[0] if not x.empty and not x.mode().empty else None)
                                .to_dict())
    df['Description'] = df['Description'].fillna(df['StockCode'].map(stockcode_description_map))
    
    # Convert data types
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['CustomerID'] = df['CustomerID'].astype(str)
    
    # Remove duplicates
    df = df.drop_duplicates().reset_index(drop = True)
    # Save clean dataset
    df.to_csv(output_path, index=False)
    print("Data Cleaning completed successfully.")

def data_transformation():
    input_path = os.path.join(data_dir, 'cleaned_online_retail.csv')
    output_path = os.path.join(data_dir, 'transformed_online_retail.csv')

    # Read cleaned csv
    df = pd.read_csv(input_path)
    # Calculate Total Price
    df['TotalPrice'] = df['Quantity'] * df['UnitPrice']
    # Save transformed csv
    df.to_csv(output_path, index = False)
    print("Total Price column added successfully.")


def load_to_mongodb():
    # Establish connection using the MongoHook
    hook = MongoHook(mongo_conn_id='mongo_default')
    client = hook.get_conn()
    db = client.Online_Retail
    collection = db.Retail_Transactions
    print(f"Connected to MongoDB - {client.server_info()}")

    # Path to transformed dataset
    path = os.path.join(data_dir, 'transformed_online_retail.csv')
    df = pd.read_csv(path)

    # Create a unique index on the specified fields
    collection.create_index([
    ('InvoiceNo'),
    ('StockCode'),
    ('Description'),
    ('Quantity'),
    ('InvoiceDate'),
    ('UnitPrice'),
    ('CustomerID'),
    ('Country'),
    ('TotalPrice'),
    ], unique=True)

    # Split the DataFrame into chunks and insert each chunk separately (only non-duplicate data)
    chunk_size = 10000
    total_documents_inserted = 0
    total_duplicates = 0
    for start in range(0, len(df), chunk_size):
        chunk = df.iloc[start : (start + chunk_size)]
        try:
            #ordered=False ensures that the insertion continues even if some documents cause errors
            result = collection.insert_many(chunk.to_dict(orient='records'), ordered=False) 
            total_documents_inserted += len(result.inserted_ids)
            print(f"New documents inserted from chunk starting at row {start}: {len(result.inserted_ids)}")
        except pymongo.errors.BulkWriteError as e:
            # Count duplicate documents in chunk
            duplicates_in_chunk = len(e.details['writeErrors'])
            total_duplicates += duplicates_in_chunk
            print(f"Duplicated documents in chunk starting at row {start}: {duplicates_in_chunk}")

    print(f"Data insertion completed, new documents inserted {total_documents_inserted}, duplicated documents {total_duplicates}")
    return total_documents_inserted

# def load_to_mongodb(**kwargs):
#     path = os.path.join(data_dir, 'transformed_online_retail.csv')
    
#     # Retrieve sensitive information from environment variables
#     mongodb_username = os.environ.get("MONGODB_USERNAME")
#     mongodb_password = os.environ.get("MONGODB_PASSWORD")
    
#     # Connect to MongoDB
#     uri = f"mongodb+srv://{mongodb_username}:{mongodb_password}@cluster0.ij1pxim.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
#     client = MongoClient(uri)
    
#     # Specify the database and collection
#     db = client['Online_Retail']
#     collection = db['Retail_Transactions']

#     # Read the CSV file
#     df = pd.read_csv(path)

#     # Insert data into MongoDB
#     collection.insert_many(df.to_dict(orient='records'))

#     print("CSV data inserted into MongoDB successfully.")

# =========================================================

# ========================== DAG ==========================
dag = DAG('online_retail_etl',
    default_args=default_args,
    description='ETL pipeline for Online Retail dataset',
    schedule_interval='@daily',  # Run the task every midnight
    catchup=False,  # Do not catch up on missing DAG runs
)
# ========================================================


# ======================== TASKS =========================
# Task 1: Download data
download_task = PythonOperator(
    task_id='download_dataset',
    python_callable=download_dataset,
    dag=dag
)

# Task 2: Clean data
clean_task = PythonOperator(
    task_id='clean_dataset',
    python_callable=clean_dataset,
    dag=dag
)

# Task 3: Transform data
transformation_task = PythonOperator(
    task_id='data_transformation',
    python_callable=data_transformation,
    dag=dag
)

# Task 4: Load data into MongoDB
load_to_mongodb_task = PythonOperator(
    task_id='load_to_mongodb',
    python_callable=load_to_mongodb,
    dag=dag
)

# Task 5: Email notification on success
success_email_task = EmailOperator(
    task_id='send_email_on_success',
    to='bralfarc7@alumnes.ub.edu',
    subject='ETL Pipeline Success',
    html_content=
    '''The ETL pipeline for the Online Retail dataset has completed successfully. 
    {{ task_instance.xcom_pull(task_ids="load_to_mongodb") }} new documents were added to the database.''',
    dag=dag
)
# ==============================================================================


# Define dependencies
download_task >> clean_task >> transformation_task >> load_to_mongodb_task >> success_email_task
