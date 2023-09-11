## This repository was created to solve the following challenge

```
Your task involves retrieving football (soccer) data from an API, storing it within a database of your preference, and crafting analytical queries for insights.

Guidelines:
1. API Access and Key Retrieval:
Retrieve `events` from `2022-08-11` to `2023-05-29` using REST API from https://apifootball.com/

2. Database Design:
  Load extracted data into a database.

3. SQL Queries.
Craft SQL queries that yield the following outputs:
  ○ Final League Table: Generate a query outputting the following columns:
  position, team_name, matches_played, won, draw, lost, goals_scored,
  goals_conceded, and points.
  Points allocation:
    ■ Victories: 3 points each
    ■ Draws: 1 point each
    ■ Defeats: 0 points
    ■ In case of points tie, utilize these tiebreakers:
      1. Superior goal difference
      2. Higher goals scored
      3. Fewer goals conceded
      4. More victories
  ○ Display all teams sorted by Away goals scored. The query must output
  the following columns: team_name, goals. Use team name in case of tie.
  ○ Top 5 Referees with Most Cards: Design a query listing referee_name
  and cards (sum of red and yellow cards). In case of a tie, arrange names in
  ascending order.
  ○ Top 3 Goal Scorers by Match Round 14: Develop a query providing
  player_name, team_name, and goals for the top three goal scorers up until
  match round 14. Sort by player name in case of a tie.

4. Data Export:
○ Export query results to CSV files: query_a.csv, query_b.csv, query_c.csv,
and query_d.csv.

5. Readme File:
○ Create a comprehensive readme file.
```


## local dev setup
```bash
# virtual env python
# installing virtualenv
pip install virtualenv

# creating a virtual environment
python3 -m venv env

# activating a virtual environment
source env/bin/activate

# installing packages
python3 -m pip install -r requirements.txt
```


## google cloud platform resouces setup
```bash
# # setup GCS project
# create a google cloud platform account

# install gcp sdk

# set a billing account

# init sdk
gcloud init

# create a gcp project
PROJECT_ID=football-analysis-demo
gcloud projects create $PROJECT_ID

# enable a billing account for this project

# # setup GCS resources

BUCKET_REGION=US
BUCKET=football-files
APP_NAME=football-analysis-app
REGION=us-east1
SECRET_NAME="API_FOOTBALL_KEY"

# # Setup Google Cloud Storage
# Create a GCS Bucket
gsutil mb -l $BUCKET_REGION gs://$BUCKET

# # Setup service account
# Create a service account
SERVICE_ACCOUNT_NAME=my-service-account
gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME --display-name "My Service Account"
SERVICE_ACCOUNT_EMAIL=$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com

# Grant required privileges for this project to the service account
gcloud projects add-iam-policy-binding $PROJECT_ID --member serviceAccount:$SERVICE_ACCOUNT_EMAIL --role roles/storage.admin
gcloud projects add-iam-policy-binding $PROJECT_ID --member serviceAccount:$SERVICE_ACCOUNT_EMAIL --role roles/storage.objectViewer
gcloud projects add-iam-policy-binding $PROJECT_ID --member serviceAccount:$SERVICE_ACCOUNT_EMAIL --role roles/storage.objectCreator
gcloud projects add-iam-policy-binding $PROJECT_ID --member serviceAccount:$SERVICE_ACCOUNT_EMAIL --role roles/bigquery.dataOwner
gcloud projects add-iam-policy-binding $PROJECT_ID --member serviceAccount:$SERVICE_ACCOUNT_EMAIL --role roles/bigquery.jobUser

# Grant cloud function access to the api key
gcloud secrets add-iam-policy-binding $SECRET_NAME \
  --role roles/secretmanager.secretAccessor \
  --member serviceAccount:$PROJECT_ID@appspot.gserviceaccount.com

# Generate a key for the service account
gcloud iam service-accounts keys create ./credentials.json --iam-account $SERVICE_ACCOUNT_EMAIL

# # Storing the football API key securely using GCP Secret Manager
# Enable the Secret Manager API
gcloud services enable secretmanager.googleapis.com
# Store API key
echo -n "your-football-api-secret" | gcloud secrets create API_FOOTBALL_KEY --data-file=- --replication-policy automatic

# # Deploy cloud functions
gcloud functions deploy $APP_NAME \
--gen2 \
--entry-point=main \
--runtime=python311 \
--source . \
--trigger-http \
--region=$REGION \
--memory=512MB \
--timeout=1200 \
--set-secrets $SECRET_NAME=$SECRET_NAME:1 \
--allow-unauthenticated

# Call cloud functions
gcloud functions call $APP_NAME --region=$REGION

# # Deleting GCP resources
gcloud functions delete $APP_NAME --region=$REGION
gsutil rm -r gs://$BUCKET
gcloud secrets delete $SECRET_NAME
gcloud iam service-accounts delete $SERVICE_ACCOUNT_EMAIL

# Delete GCP project
gcloud projects delete $PROJECT_ID
```
