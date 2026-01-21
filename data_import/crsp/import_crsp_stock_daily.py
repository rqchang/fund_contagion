# ================================================================= #
# import_crsp_mf.R ####
# ================================================================= #
# Description:
# ------------
#     This file downloads and saves CRSP Daily Stock data and risk-free rate
#     It uses RPostgres direct download.
#
# Input(s):
# ---------
#     WRDS connection
#
# Output(s):
# ----------
#     CRSP Stock raw data in cluster:
#     Data/raw/CRSP/crsp_rf_daily.csv
#     Data/raw/CRSP/crsp_stock_daily.csv
#
# Dependency:
# ----------
#     fire_pytools by Lira Mota
#     utils/pk_integrity.py
#     import_wrds/crsp_sf.py
#     https://bitbucket.org/liramota/fire_pytools/src/master/
#
# Date:
# ----------
#     2024-02-13
#
# Author(s):
# ----------
#     Ruiquan Chang, rqchang@mit.edu
#
# Additional note(s):
# ----------
#     CRSP Treasury database guide: 
#     https://www.crsp.org/wp-content/uploads/guides/CRSP_US_Treasury_Database_Guide_for_SAS_ASCII_EXCEL_R.pdf
#     
# ================================================================= #

import wrds
import pandas as pd
import numpy as np
import datetime
import os
from pandas.tseries.offsets import MonthEnd

from pk_integrity import *
from crsp_sf import *

# set up
os.chdir('/nfs/sloanlab003/projects/systematic_credit_proj/')
db = wrds.Connection(wrds_username='rqchang')

""" Daily stock return """
# parameters
varlist = ['dlret', 'dlretx', 'ret', 'retx', 'prc', 'shrout', 'exchcd', 'naics', 'permco', 'shrcd', 'siccd', 'ticker']
start_date = '2002-01-01' 
end_date = datetime.date.today().strftime("%Y-%m-%d") # '2023-02-01' #
freq = 'daily' 
permno_list = None 
shrcd_list = None # [10, 11] #
exchcd_list = None # [1, 2, 3] #

# query from crsp stock files
crspd = crsp_sf(varlist,
                start_date,
                end_date,
                freq=freq,
                permno_list=permno_list,
                shrcd_list=shrcd_list,
                exchcd_list=exchcd_list,
                db=db)

""" Risk-free rate """
# data dictionary: https://www.crsp.org/wp-content/uploads/guides/CRSP_US_Treasury_Database_Guide_for_SAS_ASCII_EXCEL_R.pdf
# monthly: 1-month treasury bill rate
query = "SELECT caldt as date, t30ret as rf FROM crspq.mcti"
rf = db.raw_sql(query, date_cols=['date'])
# rf['rf_d'] = (rf['rf']+1)**(1/30)-1

# alternative monthly
#query = "select * from crspq.tfz_mth_rf"
#rf3 = db.raw_sql(query, date_cols=['caldt'])
#rf3 = rf3.loc[rf3['kytreasnox']==2000001].copy()
#rf3['rf_d'] = rf3['tmytm']/365/100

# daily: 4-week treasury bill rate
query = "select * from crspq.tfz_dly_rf2"
rf2 = db.raw_sql(query, date_cols=['caldt'])
rf2 = rf2.loc[rf2['kytreasnox']==2000061].copy()


""" Market return """
query = "select * from crsp_a_stock.dsi where date >= '01/01/2002'"
dsi = db.raw_sql(query, date_cols=['date'])


""" Save file """
# stock daily
crspd.to_csv('./data/raw/CRSP/crsp_dsf.csv')
print("Saved CRSP daily stock data with %s observations."%(len(crspd)))

# save sample file
crspd.sample(n=1000000).to_csv('./data/raw/CRSP/crsp_dsf_small.csv')
print("Saved sample file with 1000000 observations.")

# rf daily
rf2.to_csv('./data/raw/CRSP/crsp_drf.csv')
print("Saved CRSP daily risk-free rate with %s observations."%(len(rf2)))

# market return daily
dsi.to_csv('./data/raw/CRSP/crsp_dsi.csv')
print("Saved CRSP daily market return with %s observations."%(len(dsi)))
