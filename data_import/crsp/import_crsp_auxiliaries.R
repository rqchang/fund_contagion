# ================================================================= #
# import_crsp_auxiliaries.R ####
# ================================================================= #
# Description:
# ------------
#   This downloads and saves CRSP auxiliary tables.
#   It uses RPostgres direct download.
#
# Input(s):
# ---------
#   WRDS connection
#
# Output(s):
# ----------
#   CRSP raw data:
#     data/raw/crsp/crspq_ccmxpf_lnkhist.rds
#     data/raw/crsp/stocknames.rds
#
# Date:
# ----------
#   2026-01-22
#   update:
#
# Author(s):
# ----------
#   Ruiquan Chang, chang.2590@osu.edu
#
# Additional note(s):
# ----------
#     
# ================================================================= #


# ================================================================= #
# Environment ####
# ================================================================= #
# Clear workspace
rm(list = ls())
print('Successfully started downloading CRSP auxiliaries from WRDS.')

# Import libraries
library(data.table)
library(RPostgres)

# Source helper scripts
source('utils/setPaths.R')

# Create database connections
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  user= wrds_username,
                  password= wrds_password,
                  sslmode='require',
                  dbname='wrds')


# ================================================================= #
# Read data ####
# ================================================================= #
# Download link table CRSP/Compustat
link_cc <- dbGetQuery(wrds,"SELECT gvkey, lpermno as permno, lpermco as permco, linkdt, linkenddt
                   FROM crspq.ccmxpf_lnkhist
                   WHERE linktype IN ('LC', 'LU', 'LS')") 
link_cc <- as.data.table(link_cc)

# Download CRSP names table
names <- dbGetQuery(wrds, "SELECT * FROM crspq.stocknames") 
names <- as.data.table(names)


# ================================================================= #
# Write data ####
# ================================================================= #
saveRDS(link_cc, paste0(RAWDIR, 'CRSP/crspq_ccmxpf_lnkhist.rds'))
saveRDS(names, paste0(RAWDIR, 'CRSP/stocknames.rds'))
dbDisconnect(wrds)

print('We are in import_crsp_auxiliaries. 
       CRSP auxiliaries have been downloaded successfully.')








