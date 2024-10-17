# -*- coding: utf-8 -*-
"""
Created on Fri Sep 20 12:21:14 2024

@author: donar
"""

import pandas as pd



df_risk_score_asset_level = pd.read_excel(r"D:\hdo\EDL Elastic Uploads\EDL Elastic Uploads\Output\risk_scores_asset_level.xlsx")
df_vehicles = pd.read_excel(r"D:\hdo\EDL Elastic Uploads\EDL Elastic Uploads\Output\Vehicles.xlsx")
df_asset_travel = pd.read_excel(r'D:\hdo\EDL Elastic Uploads\EDL Elastic Uploads\Input\asset_travel.xlsx')
df_safety_events = pd.read_excel(r'D:\hdo\EDL Elastic Uploads\EDL Elastic Uploads\Input\safety_events.xlsx')
df_drivers = pd.read_excel(r'D:\hdo\EDL Elastic Uploads\EDL Elastic Uploads\Input\drivers.xlsx')

# Set column headers to lowercase for all dataframes
df_asset_travel.columns = [col.lower() for col in df_asset_travel.columns]
df_risk_score_asset_level.columns = [col.lower() for col in df_risk_score_asset_level.columns]
df_vehicles.columns = [col.lower() for col in df_vehicles.columns]
df_safety_events.columns = [col.lower() for col in df_safety_events.columns]
df_drivers.columns = [col.lower() for col in df_drivers.columns]

#merge risk_score asset level with vehicles using assetEldid
combined_data = df_risk_score_asset_level.merge(df_vehicles, on='asseteldid', how='outer',suffixes=('_risk_score', '_vehicle'))
#merge combined data to asset_travel using assetEldId
combined_data = combined_data.merge(df_asset_travel, on='asseteldid', how='outer',suffixes=('_combined', '_asset_travel'))
#merge combined data to safety events using rawdata_vehicle_id
combined_data = combined_data.merge(df_safety_events, left_on='asseteldid',right_on='rawdata_vehicle_id', how='outer',suffixes=('_combined', '_safety_events'))
# Merge combined data to drivers using driverid from safety events and elddriverid from drivers
combined_data = combined_data.merge(df_drivers, left_on='driverid', right_on='elddriverid', how='outer', suffixes=('_safety_event', '_driver'))



# Save the final combined data to an Excel file
output_path = r'D:\hdo\EDL Elastic Uploads\EDL Elastic Uploads\Output\combined_data_asset.xlsx'
combined_data.to_excel(output_path, index=False)

#print(f"Combined data has been saved to {output_path}")
#,'avg_indexscore','speedindex_index'