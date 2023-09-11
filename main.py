import json
import os
import requests
from datetime import datetime, timedelta

from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage


SCRIPT_DIR = os.path.dirname(__file__)

SERVICE_ACCOUNT_FILE = "credentials.json"
PROJECT_ID = "football-analysis-demo"
BUCKET_NAME = "football-files"
INCOMING_FOLDER = "incoming"
ANALYTICS_FOLDER = "analytics"

# Create credentials object
CREDENTIALS = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)

# Create a GCS client object
STORAGE_CLIENT = storage.Client(project=PROJECT_ID, credentials=CREDENTIALS)

# Football api setup
URL = "http://apiv3.apifootball.com/?action=get_events"
API_FOOTBALL_SECRET_VALUE = os.environ['API_FOOTBALL_KEY']

# BigQuery setup
DATASET = "football_api"
INCOMING_TABLE = "events"
REGION = "US"
BIGQUERY_CLIENT = bigquery.Client(project=PROJECT_ID, credentials=CREDENTIALS)

def generate_date_list(start_date, finish_date):
    """
    Generate a list of dates.
    """
    start = datetime.strptime(start_date, '%Y-%m-%d')
    finish = datetime.strptime(finish_date, '%Y-%m-%d')
    delta = finish - start
    date_list = []
    for i in range(delta.days + 1):
        date_list.append((start + timedelta(days=i)).strftime('%Y-%m-%d'))
    return date_list

def create_local_file(json_response,file_name):
    """
    Saves a new json file in a /tmp/ folder separating each record from the list in a new line separation for downstream processing.
    """

    with open(file_name, "w") as f:
        for item in json_response:
            f.write(json.dumps(item) + "\n")

def upload_to_gcs(file_name, gcs_folder, local_file_path):
    """
    Uploads a local file to a GCS folder in the football-files bucket.
    """

    # Get the bucket and the blob
    bucket = STORAGE_CLIENT.get_bucket(BUCKET_NAME)
    blob = bucket.blob(os.path.join(gcs_folder,file_name))

    # Upload the local file
    with open(local_file_path, 'rb') as f:
        blob.upload_from_file(f)

    # Print the public URL of the uploaded file
    print(f'File uploaded to: {blob.public_url}')

def delete_local_file(file_path):
    """
    Deletes a local fle.
    """
    
    try:
        os.remove(file_path)
        print(f"{file_path} has been deleted.")
    except FileNotFoundError:
        print(f"{file_path} not found.")

def load_gcs_to_bigquery():
    """
    Loads files with a specific pattern from a GCS folder into a BigQuery table. Create a dataset if does not exist.
    """

    # Check if the dataset already exists
    dataset_ref = BIGQUERY_CLIENT.dataset(DATASET)
    try:
        BIGQUERY_CLIENT.get_dataset(dataset_ref)
        print(f'Dataset {DATASET} already exists.')
    except:
        # Create the dataset
        dataset = bigquery.Dataset(dataset_ref)
        dataset = BIGQUERY_CLIENT.create_dataset(dataset)
        print(f'Dataset {DATASET} created.')

    # Set the job configuration
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Set the source URI
    source_uri = f'gs://{BUCKET_NAME}/{INCOMING_FOLDER}/apifootball_get_events_*'

    # Set the destination table
    table_ref = BIGQUERY_CLIENT.dataset(DATASET).table(INCOMING_TABLE)

    # Create the load job
    load_job = BIGQUERY_CLIENT.load_table_from_uri(source_uri, table_ref, job_config=job_config)

    # Wait for the job to complete
    load_job.result()

    # Print the number of rows loaded
    print(f'{load_job.output_rows} rows loaded.')


def main(request):
    start_date = "2022-08-27"
    end_date = "2023-05-29"

    # avoid timeout error
    date_list = generate_date_list(start_date, end_date)

    # create a tmp folder for saving extraction files
    tmp_folder = os.path.join(SCRIPT_DIR,"tmp")
    if not os.path.exists(tmp_folder):
        os.makedirs(tmp_folder, exist_ok=True)

    # create a folder for analytics files
    analytics_folder = os.path.join(SCRIPT_DIR,"analytics")
    if not os.path.exists(analytics_folder):
        os.makedirs(analytics_folder, exist_ok=True)

    # extract data from football api and upload to Google Cloud Storage
    for date in date_list:
        print(date)
        response = requests.get(
            URL,
            params={"from":date,
                    "to":date,
                    "APIkey":API_FOOTBALL_SECRET_VALUE},
        )

        json_response = response.json()
        file_name = f"apifootball_get_events_{date}.json"
        local_file_path = os.path.join(tmp_folder,file_name)

        create_local_file(json_response,local_file_path)
        upload_to_gcs(file_name, INCOMING_FOLDER, local_file_path)
        delete_local_file(local_file_path)

    # Load GCS files into a BigQuery denormalized table
    load_gcs_to_bigquery()

    # Query and export results to GCS
    query_files = ["query_a.sql", "query_b.sql", "query_c.sql", "query_d.sql"]

    for query_file in query_files:
        print(query_file)
        with open(query_file, 'r') as f:
            sql = f.read()
        
        # Write query results to a new table
        analytics_table = query_file.replace('.sql',"")
        job_config = bigquery.QueryJobConfig()
        table_ref = BIGQUERY_CLIENT.dataset(DATASET).table(analytics_table)
        job_config.destination = table_ref
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

        query_job = BIGQUERY_CLIENT.query(
            sql,
            location=REGION,
            job_config=job_config)
        rows = list(query_job)  # Waits for the query to finish
        
        print(f"Exported '{query_file}' query result to '{table_ref}' BigQuery table")

        # Export table to GCS as a CSV file
        destination_uri = f"gs://{BUCKET_NAME}/{ANALYTICS_FOLDER}/{query_file.replace('sql','csv')}"
        dataset_ref = BIGQUERY_CLIENT.dataset(DATASET, project=PROJECT_ID)
        table_ref = dataset_ref.table(analytics_table)

        extract_job = BIGQUERY_CLIENT.extract_table(
            table_ref,
            destination_uri,
            location=REGION)
        extract_job.result()  # Waits for job to complete
        print(f"Exported '{query_file}' query result as a CSV file in '{destination_uri}'")
    return "Success"
