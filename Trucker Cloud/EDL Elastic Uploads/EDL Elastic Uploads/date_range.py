# -*- coding: utf-8 -*-
"""
Created on Fri Jun  7 13:42:18 2024

@author: shrihari
"""

from datetime import date, timedelta

def get_date_ranges(start_date, today):
  """
  This function generates a list of tuples containing the start date and the last day of the month,
  including the last record with today's date.

  Args:
      start_date: The starting date (inclusive) in YYYY-MM-DD format.
      today: The end date (inclusive) in YYYY-MM-DD format.

  Returns:
      A list of tuples containing (start_date, end_date) for each period.
  """
  date_ranges = []
  while start_date <= today:
    # Get the year and month from the start date
    year = start_date.year
    month = start_date.month

    # Calculate the last day of the month
    if month == 12:
      end_date = date(year, month, 31)  # Handle December specifically
    else:
      # Use a loop to find the last day of non-December months
      end_date = date(year, month + 1, 1) - timedelta(days=1)

    # Handle the last record to ensure it includes today's date
    if end_date > today:
      end_date = today

    date_ranges.append((start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))

    # Move to the next month, handling year change if necessary
    if month == 12:
      start_date = date(year + 1, 1, 1)
    else:
      start_date = date(year, month + 1, 1)

  return date_ranges


