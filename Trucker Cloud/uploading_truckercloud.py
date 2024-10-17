# -*- coding: utf-8 -*-
"""
Created on Wed Sep 11 12:20:39 2024

@author: donar
"""

import requests
import pandas as pd
import json
from datetime import datetime


# Elasticsearch configuration
es_endpoint = "https://search-elk-soqgola6bhb67co64hiqigfsjq.us-east-1.es.amazonaws.com"
es_username = "elastic"
es_password = "Acce$$ElastiC7747/"
index_name = "eos_hdo_tc_index"  # Set the name of your index

# Read the Excel file
df = pd.read_excel(r"D:\hdo\Mastersheet.xlsx")
df.columns = df.columns.str.strip()
df = df.astype(str)
df['telematics'] = 'Trucker Cloud'
df['account_id'] = '1836'
df['org_id']  = '3'
df['current_date'] = datetime.now().strftime('%Y-%m-%d')

# Convert columns to the appropriate data types
df['score'] = pd.to_numeric(df['score'], errors='coerce')
df['se_gforceinfo_gforce'] = pd.to_numeric(df['se_gforceinfo_gforce'], errors='coerce')
df['harsh_events_index'] = pd.to_numeric(df['harsh_events_index'], errors='coerce')
df['speed_index'] = pd.to_numeric(df['speed_index'], errors='coerce')
df['geographical_index'] = pd.to_numeric(df['geographical_index'], errors='coerce')
df['avg_indexscore'] = pd.to_numeric(df['avg_indexscore'], errors='coerce')
df['speedindex_details_totalmilesrecorded'] = pd.to_numeric(df['speedindex_details_totalmilesrecorded'], errors='coerce')
df['vehicle_model_year'] = pd.to_numeric(df['vehicle_model_year'], errors='coerce')
df['driver_zipcode'] = pd.to_numeric(df['driver_zipcode'], errors='coerce')
df['se_start_timeevent'] = pd.to_datetime(df['se_start_timeevent'], errors='coerce')
df['se_gforceInfo_event_datetime'] = pd.to_datetime(df['se_gforceInfo_event_datetime'], errors='coerce')
df['updated'] = pd.to_datetime(df['updated'], errors='coerce')
df['updated'] = df['updated'].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
df['period_start'] = pd.to_datetime(df['period_start'], errors='coerce')
df['period_start'] = df['period_start'].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
df['period_end'] = pd.to_datetime(df['period_end'], errors='coerce')
df['period_end'] = df['period_end'].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
# Ensure that the columns are string where necessary
str_columns = [
    'carrier_name', 'vehicle_vin', 'vehicle_make', 'vehicle_model',
    'driver_firstname', 'se_eventtype', 'asset_eldId','geographical_indexdetails_milesdrivenbyrisk_zipcode_high',
    'geographical_indexdetails_milesdriven_byriskzipcode_medium', 'geographical_indexdetails_milesdrivenby_riskzipcode_low',
    'speedindexdetails_pings_overspeedlimitplus5mph', 'speedindexdetails_pingsoverspeedlimitplus10mph',
    'speedindex_details_pingsover75mph', 'at_asset_gps_miles', 'se_rawdata_location_latitude',
    'se_rawdata_location_longitude', 'speedindexdetails_pingsoverspeedlimitplus15mph',
    'speedindexdetails_pingsoverspeedlimitplus25percent', 'speedindexdetails_pingsoverspeedlimitplus50percent'
]

df[str_columns]= df[str_columns].astype(str)

# Prepare bulk data
bulk_data = ""
for idx, doc in df.iterrows():
    #if idx>10:break
    document = doc.to_dict()
    # Convert NaN to null for JSON compatibility
    document = {k: (v if not pd.isna(v) else None) for k, v in document.items()}
    document['se_start_timeevent']=str(document['se_start_timeevent'])
    document['se_gforceInfo_event_datetime']=str(document['se_gforceInfo_event_datetime'])
    document['updated']=str(document['updated'])
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
