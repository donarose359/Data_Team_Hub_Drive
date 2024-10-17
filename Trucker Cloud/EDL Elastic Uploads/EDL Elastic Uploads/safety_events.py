# -*- coding: utf-8 -*-
"""
Created on Fri Jun  7 13:33:20 2024

@author: shrihari
"""

import requests
from authentication import get_authtoken
import pandas as pd
from date_range import get_date_ranges
from datetime import date
import json 
from flatten_data import flatten_dict

df=pd.read_excel('Input/carriers.xlsx')
ddf = df[df['codeType'] == 'DOT']

start_date = date(2024, 1, 1)
today = date.today()
date_ranges = get_date_ranges(start_date, today)

authToken = get_authtoken()
headers = {
    "accept": "application/json",
    "Authorization": authToken,
    "content-type": "application/json"
}




flat_safety_events_li = []
for c in range(len(ddf)):
    for d in range(len(date_ranges)):
        if date_ranges[d][1] > date_ranges[d][0]:
            safety_events_url = "https://api.truckercloud.com/api/v4/safety-events?carrierCode="+str(ddf['carrierCode'].iloc[c])+"&codeType=DOT&eldVendor="+str(ddf['eldVendor'].iloc[c])+"&endTime="+str(date_ranges[d][1])+"T00%3A00%3A00.000Z&page=1&size=100&startTime="+str(date_ranges[d][0])+"T00%3A00%3A00.000Z"
            response = requests.post(safety_events_url, headers=headers).json()
            total_pages = response['pagination']['totalPages']
            print(total_pages)
            for p in range(total_pages):
                safety_events_url = "https://api.truckercloud.com/api/v4/safety-events?carrierCode="+str(ddf['carrierCode'].iloc[c])+"&codeType=DOT&eldVendor="+str(ddf['eldVendor'].iloc[c])+"&endTime="+str(date_ranges[d][1])+"T00%3A00%3A00.000Z&page="+str(p+1)+"&size=100&startTime="+str(date_ranges[d][0])+"T00%3A00%3A00.000Z"
                response = requests.post(safety_events_url, headers=headers).json()
                safety_events_li = response['content']
                
                for item in safety_events_li:
                    
                    flat_data = flatten_dict(item)
                    flat_safety_events_li.append(flat_data)
            
# Create DataFrame from flattened data
df = pd.DataFrame(flat_safety_events_li)
df.to_excel('Input/safety_events.xlsx',index=False)
    