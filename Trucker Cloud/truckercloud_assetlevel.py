# -*- coding: utf-8 -*-
"""
Created on Mon Sep 23 13:02:51 2024

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
index_name = "eos_hdo_tc_asset_index"  # Set the name of your index

# Read the Excel file
df = pd.read_excel(r"D:\hdo\EDL Elastic Uploads\EDL Elastic Uploads\Output\combined_data_asset.xlsx")
df.columns = df.columns.str.strip()
df = df.astype(str)
df['telematics'] = 'Trucker Cloud'
df['account_id'] = '1835'
df['org_id'] = '1835'
df['current_date'] = datetime.now().strftime('%Y-%m-%d')


# List of datetime columns
datetime_columns = [
    'updated', 'periodstart', 'periodend', 'createdtime', 'updatedtime',
    'start_date', 'end_date', 'starttimeevent', 'rawdata_time', 'gforceinfo_eventdatetime',
    'licenseexpiry'
]

# Handle NaN values: fill with a default date or drop rows
df[datetime_columns] = df[datetime_columns].fillna('1970-01-01')

# Convert datetime columns
df[datetime_columns] = df[datetime_columns].apply(pd.to_datetime, errors='coerce')


# Ensure that columns are string where necessary
str_columns = [
    'carriername_risk_score', 'eldprovidername', 'carriercodes_codetype_risk_score',
    'carriercodes_carriercode_risk_score', 'assetid_risk_score', 'asseteldid',
    'licenseplate_risk_score', 'identificationno', 'assetnumber_risk_score',
    'vehicleid_risk_score', 'deviceid_risk_score', 'carrierid', 'carriername_vehicle',
    'type', 'carriercodes_codetype_vehicle', 'carriercodes_carriercode_vehicle',
    'eldvendors_eldvendor', 'assetid_vehicle', 'licenseplate_vehicle', 'assetnumber_vehicle',
    'vehicleid_vehicle', 'assettype', 'vehicleclass', 'bodyclass', 'make', 'model',
    'modelyear', 'vin', 'deviceid_vehicle', 'driverid', 'drivername', 'eventtype',
    'vehicleid', 'rawdata_id', 'rawdata_driver_id', 'rawdata_driver_name', 
    'rawdata_vehicle_id', 'rawdata_vehicle_name', 'rawdata_downloadforwardvideourl',
    'rawdata_downloadinwardvideourl', 'rawdata_downloadtrackedinwardvideourl',
    'rawdata_behaviorlabels_label', 'rawdata_behaviorlabels_source',
    'rawdata_behaviorlabels_name', 'rawdata_driver', 'carriername', 'codetype',
    'carriercode', 'eldvendor', 'elddriverid', 'firstname', 'middlename', 'lastname',
    'licensenumber', 'licensestate', 'licenseclass', 'phonenumber', 'emailid', 
    'address1', 'address2', 'city', 'state', 'zipcode', 'status'
]
df[str_columns] = df[str_columns].astype(str)


# Ensure all numerical columns are properly handled as floats or integers
numeric_columns = [
    'score', 'harsheventsindex', 'speedindex', 'geographicalindex', 
    'harsheventsindexdetails_severity', 'harsheventsindexdetails_eventscount',
    'harsheventsindexdetails_eventsper1kmiles', 'speedindexdetails_totalmilesrecorded',
    'speedindexdetails_pingscount', 'speedindexdetails_pingsover75mph', 
    'speedindexdetails_pingsoverspeedlimitplus5mph', 'speedindexdetails_pingsoverspeedlimitplus10mph',
    'speedindexdetails_pingsoverspeedlimitplus15mph', 'speedindexdetails_pingsoverspeedlimitplus10percent',
    'speedindexdetails_pingsoverspeedlimitplus25percent', 'speedindexdetails_pingsoverspeedlimitplus50percent',
    'geographicalindexdetails_totalmilesrecorded', 'geographicalindexdetails_milesdrivenbyriskzipcode_high',
    'geographicalindexdetails_milesdrivenbyriskzipcode_medium', 'geographicalindexdetails_milesdrivenbyriskzipcode_low',
    'geographicalindexdetails_milesdrivenbyzipcode', 'geographicalindexdetails_milesdrivenbyroadtype_roadtypeid',
    'geographicalindexdetails_milesdrivenbyroadtype_milesdriven', 'gpsmiles', 'latitude', 'longitude',
    'rawdata_maxaccelerationgforce', 'rawdata_location_latitude', 'rawdata_location_longitude',
    'gforceinfo_gforce'
]
df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
df['avg_indexscore'] = df[['harsheventsindex', 'speedindex', 'geographicalindex']].mean(axis=1)
df['speedindex_index'] = df[['speedindexdetails_pingsoverspeedlimitplus15mph', 'speedindexdetails_pingsoverspeedlimitplus25percent', 'speedindexdetails_pingsoverspeedlimitplus50percent']].sum(axis=1)


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


#Add 'latest_year' flag: 'yes' if the year matches the latest periodstart's year, else 'no'
df['latest_year'] = df['year'].apply(lambda x: 'yes' if x == latest_year else 'no')

#Add 'latest_month' flag: 'yes' if the year and month match the latest periodstart's year and month, else 'no'
df['latest_month'] = df.apply(lambda row: 'yes' if (row['year'] == latest_year and row['month'] == latest_month) else 'no', axis=1)

#Add 'latest_week' flag: 'yes' if the year and week match the latest periodstart's year and week, else 'no'
df['latest_week'] = df.apply(lambda row: 'yes' if (row['year'] == latest_year and row['week'] == latest_week) else 'no', axis=1)


# To defragment the DataFrame and improve performance after adding multiple columns
df = df.copy()
# Drop intermediate year, month, week columns before sending to Elasticsearch
df.drop(['year', 'month', 'week'], axis=1, inplace=True)

# Prepare bulk data
bulk_data = ""
for idx, doc in df.iterrows():
    if idx>4000:break
    document = doc.to_dict()
    # Convert NaN to None for JSON compatibility and handle 'nan' strings
    document = {k: (None if pd.isna(v) or v == 'nan' else v) for k, v in document.items()}

    # Ensure datetime fields are properly formatted as strings
    for field in datetime_columns:
        if field in document and isinstance(document[field], pd.Timestamp):
            document[field] = document[field].strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        elif field in document and pd.isna(document[field]):
            document[field] = None  # Explicitly set to None if still NaN
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