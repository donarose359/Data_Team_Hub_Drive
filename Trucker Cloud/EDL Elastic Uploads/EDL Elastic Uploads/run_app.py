# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 15:33:50 2024

@author: shrihari
"""

import os
import zipfile
from datetime import datetime

import carriers
import vehicles
import elds
import drivers
import safety_events
import risk_scores
import risk_scores_assest_level
import asset_travelled_distance
import vehicle_asset_driver

def zip_and_move_excel_files(folder_path):
  """
  Zips all excel files in a folder, moves the zip to a new folder named with today's date,
  and deletes the original excel files after successful archiving.

  Args:
      folder_path: Path to the folder containing excel files.
  """

  # Get today's date in YYYY-MM-DD format
  today_date = datetime.now().strftime('%Y-%m-%d')

  # Create a new folder with today's date
  #new_folder_path = os.path.join(folder_path, today_date)
  #try:
   # os.makedirs(new_folder_path)
  #except FileExistsError:
    #print(f"Folder '{today_date}' already exists.")

  # Create a zip file name with today's date
  zip_filename = f"{today_date}.zip"
  zip_filepath = os.path.join("Input", zip_filename)

  # Open the zip file in write mode
  with zipfile.ZipFile(zip_filepath, 'w') as zip_file:
    # Loop through all files in the folder
    for filename in os.listdir(folder_path):
      # Check if it's an excel file (modify extension if needed)
      if filename.endswith(".xlsx") or filename.endswith(".xls"):
        file_path = os.path.join(folder_path, filename)
        zip_file.write(file_path, filename)  # Add file to the zip with original name

        # Delete the original excel file after successful addition
        os.remove(file_path)

  print(f"Excel files zipped to '{zip_filepath}'. Originals deleted.")

zip_and_move_excel_files(r'Input')
