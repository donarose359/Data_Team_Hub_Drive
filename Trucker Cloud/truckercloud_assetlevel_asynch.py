# -*- coding: utf-8 -*-
"""
Created on Wed Sep 25 15:26:40 2024

@author: donar
"""

import asyncio
import aiohttp
import pandas as pd
import json
from datetime import datetime
import nest_asyncio

# Apply nest_asyncio to allow nested event loops
nest_asyncio.apply()

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

# Extract periodstart data
df['periodstart'] = pd.to_datetime(df['periodstart'], errors='coerce')
latest_period_start = df['periodstart'].max()

# Extract year, month, and week
latest_year = latest_period_start.year
latest_month = latest_period_start.month
latest_week = latest_period_start.isocalendar().week

# Add 'latest_year', 'latest_month', 'latest_week' flags
df['latest_year'] = df['periodstart'].dt.year.apply(lambda x: 'yes' if x == latest_year else 'no')
df['latest_month'] = df.apply(lambda row: 'yes' if (row['periodstart'].year == latest_year and row['periodstart'].month == latest_month) else 'no', axis=1)
df['latest_week'] = df.apply(lambda row: 'yes' if (row['periodstart'].year == latest_year and row['periodstart'].isocalendar().week == latest_week) else 'no', axis=1)

# Drop intermediate columns
#df.drop(['year', 'month', 'week'], axis=1, inplace=True)

# Asynchronous function to handle requests
async def create_index(session, url):
    async with session.put(url, auth=aiohttp.BasicAuth(es_username, es_password)) as response:
        if response.status in [200, 201]:
            print("Index created successfully.")
        else:
            print(f"Failed to create index: {response.status} {await response.text()}")

async def send_bulk_data(session, bulk_data):
    bulk_upload_url = f"{es_endpoint}/_bulk"
    headers = {"Content-Type": "application/x-ndjson"}
    async with session.post(bulk_upload_url, auth=aiohttp.BasicAuth(es_username, es_password), headers=headers, data=bulk_data) as response:
        if response.status != 200:
            print(f"Failed to index documents: {response.status} {await response.text()}")
        else:
            response_json = await response.json()
            if 'errors' in response_json and response_json['errors']:
                print("Errors occurred during bulk indexing:")
                for item in response_json['items']:
                    if 'index' in item and 'error' in item['index']:
                        print(item['index']['error'])
            else:
                print("Documents indexed successfully.")

async def prepare_bulk_upload(session, df, datetime_columns):
    bulk_data = ""
    for idx, doc in df.iterrows():
        #if idx > 2000: break
        document = doc.to_dict()
        
        # Handle NaN and datetime conversion
        document = {k: (None if pd.isna(v) or v == 'nan' else v) for k, v in document.items()}
        for field in datetime_columns:
            if field in document and isinstance(document[field], pd.Timestamp):
                document[field] = document[field].strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            elif field in document and pd.isna(document[field]):
                document[field] = None  # Explicitly set to None if still NaN
        
        # Add an 'id' field
        document['id'] = idx + 1
        bulk_data += f'{{"index":{{"_index":"{index_name}","_id":"{idx + 1}"}}}}\n'
        bulk_data += json.dumps(document) + "\n"
        
        if (idx + 1) % 100 == 0:  # Send bulk data in batches of 100
            await send_bulk_data(session, bulk_data)
            bulk_data = ""
    
    # Send any remaining bulk data
    if bulk_data:
        await send_bulk_data(session, bulk_data)

async def main():
    async with aiohttp.ClientSession() as session:
        create_index_url = f"{es_endpoint}/{index_name}"
        await create_index(session, create_index_url)
        await prepare_bulk_upload(session, df, datetime_columns)

# Run the asynchronous main function
asyncio.run(main())
