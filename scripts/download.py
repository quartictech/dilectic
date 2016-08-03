#!/bin/env python
from datetime import date, timedelta
import subprocess

date_start = date(2015, 1, 1)
date_end = date(2016, 7, 1)

if __name__ == "__main__":

  dt = date_start
  while dt < date_end:
    url = "http://download.companieshouse.gov.uk/Accounts_Monthly_Data-%s.zip" % dt.strftime("%B%Y")
    print(url)

    subprocess.call(['wget', '-c', '-P', 'accounts/zip', url])
    new_year = dt.year
    if dt.month < 12:
      new_month = dt.month + 1
    else:
      new_month = 1
      new_year = dt.year + 1
    dt = date(new_year, new_month, 1)
