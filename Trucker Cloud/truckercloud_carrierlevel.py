# -*- coding: utf-8 -*-
"""
Created on Fri Sep 20 15:27:04 2024

@author: donar
"""

import requests
import pandas as pd
import json
from datetime import datetime, timedelta


# Elasticsearch configuration
es_endpoint = "https://search-elk-soqgola6bhb67co64hiqigfsjq.us-east-1.es.amazonaws.com"
es_username = "elastic"
es_password = "Acce$$ElastiC7747/"
index_name = "eos_hdo_tc_carrier_index"  # Set the name of your index

# Read the Excel file
df = pd.read_excel(r"D:\hdo\EDL Elastic Uploads\EDL Elastic Uploads\Output\risk_scores_carrier_level.xlsx")
df.columns = df.columns.str.strip()
df = df.astype(str)
df['telematics'] = 'Trucker Cloud'
df['account_id'] = '1835'
df['org_id'] = '1835'
df['current_date'] = datetime.now().strftime('%Y-%m-%d')


# Convert numeric columns to the appropriate data types
df['score'] = pd.to_numeric(df['score'], errors='coerce')
df['harsheventsindex'] = pd.to_numeric(df['harsheventsindex'], errors='coerce')
df['speedindex'] = pd.to_numeric(df['speedindex'], errors='coerce')
df['geographicalindex'] = pd.to_numeric(df['geographicalindex'], errors='coerce')
df['harsheventsindexdetails_eventscount'] = pd.to_numeric(df['harsheventsindexdetails_eventscount'], errors='coerce')
df['harsheventsindexdetails_eventsper1kmiles'] = pd.to_numeric(df['harsheventsindexdetails_eventsper1kmiles'], errors='coerce')
df['speedindexdetails_totalmilesrecorded'] = pd.to_numeric(df['speedindexdetails_totalmilesrecorded'], errors='coerce')
df['speedindexdetails_pingscount'] = pd.to_numeric(df['speedindexdetails_pingscount'], errors='coerce')
df['speedindexdetails_pingsover75mph'] = pd.to_numeric(df['speedindexdetails_pingsover75mph'], errors='coerce')
df['speedindexdetails_pingsoverspeedlimitplus5mph'] = pd.to_numeric(df['speedindexdetails_pingsoverspeedlimitplus5mph'], errors='coerce')
df['speedindexdetails_pingsoverspeedlimitplus10mph'] = pd.to_numeric(df['speedindexdetails_pingsoverspeedlimitplus10mph'], errors='coerce')
df['speedindexdetails_pingsoverspeedlimitplus15mph'] = pd.to_numeric(df['speedindexdetails_pingsoverspeedlimitplus15mph'], errors='coerce')
df['speedindexdetails_pingsoverspeedlimitplus10percent'] = pd.to_numeric(df['speedindexdetails_pingsoverspeedlimitplus10percent'], errors='coerce')
df['speedindexdetails_pingsoverspeedlimitplus25percent'] = pd.to_numeric(df['speedindexdetails_pingsoverspeedlimitplus25percent'], errors='coerce')
df['speedindexdetails_pingsoverspeedlimitplus50percent'] = pd.to_numeric(df['speedindexdetails_pingsoverspeedlimitplus50percent'], errors='coerce')
df['geographicalindexdetails_totalmilesrecorded'] = pd.to_numeric(df['geographicalindexdetails_totalmilesrecorded'], errors='coerce')
df['geographicalindexdetails_milesdrivenbyriskzipcode_high'] = pd.to_numeric(df['geographicalindexdetails_milesdrivenbyriskzipcode_high'], errors='coerce')
df['geographicalindexdetails_milesdrivenbyriskzipcode_medium'] = pd.to_numeric(df['geographicalindexdetails_milesdrivenbyriskzipcode_medium'], errors='coerce')
df['geographicalindexdetails_milesdrivenbyriskzipcode_low'] = pd.to_numeric(df['geographicalindexdetails_milesdrivenbyriskzipcode_low'], errors='coerce')

# Convert columns with date-time data
df['updated'] = pd.to_datetime(df['updated'], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
df['periodstart'] = pd.to_datetime(df['periodstart'], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
df['periodend'] = pd.to_datetime(df['periodend'], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

# Ensure that columns are string where necessary
str_columns = [
    'carriername', 'eldprovidername', 'carriercodes_codetype', 
    'carriercodes_carriercode', 'ratingvariables_finalratingfactor', 
    'ratingvariables_geographicalfactor', 'ratingvariables_harsheventsfactorcapped',
    'ratingvariables_harsheventsfactoruncapped', 'ratingvariables_speedfactoruncapped', 
    'ratingvariables_speedfactorcapped'
]
df[str_columns] = df[str_columns].astype(str)

# Ensure all numerical columns are properly handled as floats or integers
numeric_columns = [
    'harsheventsindex', 'speedindex', 'geographicalindex',
    'harsheventsindexdetails_severity', 'speedindexdetails_totalmilesrecorded',
    'geographicalindexdetails_milesdrivenbyriskzipcode_high',
    'geographicalindexdetails_milesdrivenbyriskzipcode_medium',
    'geographicalindexdetails_milesdrivenbyriskzipcode_low','avg_indexscore','speedindex_index'
]
df['avg_indexscore'] = df[['harsheventsindex', 'speedindex', 'geographicalindex']].mean(axis=1)
df['speedindex_index'] = df[['speedindexdetails_pingsoverspeedlimitplus15mph', 'speedindexdetails_pingsoverspeedlimitplus25percent', 'speedindexdetails_pingsoverspeedlimitplus50percent']].sum(axis=1)
df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
# Find the latest period_start date
df['periodstart'] = pd.to_datetime(df['periodstart'], errors='coerce')  # Ensure period_start is in datetime format
df['periodstart'] = df['periodstart'].dt.tz_localize(None)

latest_period_start = df['periodstart'].max()  

# Get the current date
current_date = datetime.now()

# Extract year, month, and week from the latest periodstart
latest_year = latest_period_start.year
latest_month = latest_period_start.month
latest_week = latest_period_start.isocalendar().week

# Extract year, month, and week from each periodstart
df['year'] = df['periodstart'].dt.year
df['month'] = df['periodstart'].dt.month
df['week'] = df['periodstart'].dt.isocalendar().week

# Add 'latest_year' flag: 'yes' if the year matches the latest periodstart's year, else 'no'
df['latest_year'] = df['year'].apply(lambda x: 'yes' if x == latest_year else 'no')

# Add 'latest_month' flag: 'yes' if the year and month match the latest periodstart's year and month, else 'no'
df['latest_month'] = df.apply(lambda row: 'yes' if (row['year'] == latest_year and row['month'] == latest_month) else 'no', axis=1)

# Add 'latest_week' flag: 'yes' if the year and week match the latest periodstart's year and week, else 'no'
df['latest_week'] = df.apply(lambda row: 'yes' if (row['year'] == latest_year and row['week'] == latest_week) else 'no', axis=1)

# Drop intermediate year, month, week columns before sending to Elasticsearch
df.drop(['year', 'month', 'week'], axis=1, inplace=True)

# Prepare bulk data
bulk_data = ""
for idx, doc in df.iterrows():
    #if idx>1:break
    document = doc.to_dict()
    # Convert NaN to None for JSON compatibility and handle 'nan' strings
    document = {k: (None if pd.isna(v) or v == 'nan' else v) for k, v in document.items()}
    # Ensure datetime fields are properly formatted as strings
    for field in ['updated', 'periodstart', 'periodend']:
        if field in document and isinstance(document[field], pd.Timestamp):
            document[field] = document[field].strftime('%Y-%m-%dT%H:%M:%S.%fZ') if pd.notna(document[field]) else None
    # Add the 'id' field, which is a numerical version of _id
    document['id'] = idx + 1 
    bulk_data += f'{{"index":{{"_index":"{index_name}","_id":"{idx + 1}"}}}}\n'
    bulk_data += json.dumps(document) + "\n"

# Create a new index in Elasticsearch (if needed)
create_index_url = f"{es_endpoint}/{index_name}"
create_index_response = requests.put(
    create_index_url,
    auth=(es_username, es_password),
    headers={"Content-Type": "application/json"},
    json={}
)

if create_index_response.status_code not in [200, 201]:
    print(f"Failed to create index: {create_index_response.status_code} {create_index_response.text}")
else:
    print("Index created successfully.")

# Upload the JSON data to Elasticsearch
bulk_upload_url = f"{es_endpoint}/_bulk"
response = requests.post(
    bulk_upload_url,
    auth=(es_username, es_password),
    headers={"Content-Type": "application/x-ndjson"},
    data=bulk_data
)

# Check the response for details
if response.status_code != 200:
    print(f"Failed to index documents: {response.status_code} {response.text}")
else:
    response_json = response.json()
    if 'errors' in response_json and response_json['errors']:
        print("Errors occurred during bulk indexing:")
        for item in response_json['items']:
            if 'index' in item and 'error' in item['index']:
                print(item['index']['error'])
    else:
        print("Documents indexed successfully.")