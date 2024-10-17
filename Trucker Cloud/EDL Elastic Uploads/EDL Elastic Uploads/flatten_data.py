# -*- coding: utf-8 -*-
"""
Created on Mon Jun 10 14:42:36 2024

@author: shrihari
"""
import json

def flatten_dict(data):
    """Flattens a dictionary with arbitrarily nested dictionaries and lists of dictionaries, joining key names with '_' for nested structures."""
    flat_data = {}
    parent_key = ""  # Track the current parent key for nested structures
    
    def flatten_helper(data, current_key):
      for key, value in data.items():
        try:
            if isinstance(value, str):
                value = json.loads(value)
        except:
            pass
        combined_key = f"{current_key}_{key}" if current_key else key  # Combine keys with '_'
        if isinstance(value, dict):
          flatten_helper(value, combined_key)  # Recursively flatten nested dictionaries
        elif isinstance(value, list):
          for item in value:
            if isinstance(item, dict):
              flatten_helper(item, combined_key)  # Flatten nested dictionaries in list
            else:
              # Add non-dictionary elements from the list directly (adjust as needed)
              flat_data[f"{combined_key}_list"] = value
        else:
          flat_data[combined_key] = value
    
    flatten_helper(data, parent_key)  # Start flattening with empty parent key
    return flat_data