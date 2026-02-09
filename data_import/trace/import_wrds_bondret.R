# ================================================================= #
# import_wrds_bondret.R ####
# ================================================================= #
# Description :
# ------------
#   This code reads WRDS bond returns data downloaded from WRDS.
#
# Input(s) :
# ------------
#   WRDS connection
#
# Output(s) :
# ------------
#   Dropbox Trace raw data:
#     data/trace/wrds_bondret.rds
#
# Date:
# ------------
#   2022-05-11
#   update: 2026/01/22
#
# Author(s):
# ------------
#   Lira Mota Mertens, liramota@mit.edu
#   Ruiquan Chang, chang.2590@osu.edu
#
# Additional note(s):
# ------------
#     
# ================================================================= #


# ================================================================= #
# Environment ####
# ================================================================= #
# Clear workspace
rm(list = ls())

# Import libraries
library(data.table)
library(RPostgres)
library(zoo)
library(lubridate)

# Source helper scripts
source('utils/setPaths.R')
source('utils/wrds_credentials.R')

# Create database connections
creds <- get_wrds_credentials()
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  user = creds$username,
                  password = creds$password,
                  sslmode='require',
                  dbname='wrds')
                  dbname ='wrds')


# ================================================================= #
# Download Data ####
# ================================================================= #
print('Successfully stated WRDS bond returns download.')

# Bond Returns 
sql <- sprintf("SELECT * FROM wrdsapps.bondret")
ret <- dbGetQuery(wrds, sql)
ret <- as.data.table(ret)

# Bond-CRSP link
sql <- sprintf("SELECT * FROM wrdsapps.bondcrsp_link")
bondcrsp_link <- dbGetQuery(wrds, sql)
bondcrsp_link <- as.data.table(bondcrsp_link)


# ================================================================= #
# Data Type Fix ####
# ================================================================= #
# Treasury Maturity: warning is ok
ret[, treasury_maturity := as.numeric((gsub("*YEAR", "", treasury_maturity)))]

# Create issuer cusip
ret[,icusip := substring(cusip,1,6)]


# ================================================================= #
# Save Data ####
# ================================================================= #
if(any(duplicated(ret[, .(issue_id, date)]))){
  stop('CRSP data primary key violated.')
}

# save files
saveRDS(ret, file = paste0(RAWDIR, 'trace/wrds_bondret.rds'))
saveRDS(bondcrsp_link, file = paste0(RAWDIR, 'trace/wrds_bondcrsp_link.rds'))

dbDisconnect(wrds)
print('WRDS bond returns tables have been downloaded successfully.')





