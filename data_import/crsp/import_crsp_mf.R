# ================================================================= #
# import_crsp_mf.R ####
# ================================================================= #
# Description:
# ------------
#   This file downloads and saves CRSP Mutual Fund data from 2000 till now.
#   It uses RPostgres direct download.
#
# Input(s):
# ---------
#   WRDS connection
#
# Output(s):
# ----------
#   CRSP MF raw data:
#     data/raw/crsp/crsp_mf_holdings_cusip.rds
#     data/raw/crsp/crsp_mf_sum.rds
#     data/raw/crsp/crsp_mf_hdr.rds
#     data/raw/crsp/crsp_mf_mret.rds
#     data/raw/crsp/crsp_mf_mtna.rds
#     data/raw/crsp/
#     data/raw/crsp/crsp_portno_link.rds
#
# Date:
# ----------
#   2026-01-21
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

# Import libraries
library(data.table)
library(RPostgres)
library(zoo)
library(lubridate)

# Source helper scripts
source('utils/setPaths.R')

# Create database connections
wrds <- dbConnect(Postgres(),
                  host = 'wrds-pgdata.wharton.upenn.edu',
                  port = 9737,
                  user = 'rqchang99',
                  password = 'Crq-19990711',
                  sslmode ='require',
                  dbname ='wrds')


# ================================================================= #
# Read data ####
# ================================================================= #
print('Successfully started downloading CRSP Mutual Fund data from WRDS.')

#-----------------------------------------#
# 1. Get data from holdings
#-----------------------------------------#
varlist_holdings <- c('crsp_portno', 'cusip', 'eff_dt', 'market_val', 'nbr_shares', 'permco', 'permno', 'report_dt')
cmf <- dbGetQuery(wrds, 
                  sprintf("SELECT %s FROM crsp.holdings 
                           WHERE eff_dt >= '01/01/2000'",
                          paste(c(varlist_holdings), collapse = ',')))
cmf <- as.data.table(cmf)

#-----------------------------------------#
# 2. Get data from fund summary
#-----------------------------------------#
# fund summary
varlist_sum <- c('caldt', 'crsp_fundno', 'tna_latest', 'summary_period', 'maturity', 'maturity_dt')
fund_sum <- dbGetQuery(wrds, "SELECT * FROM crsp.fund_summary
                                WHERE caldt >= '01/01/2000'")
fund_sum <- as.data.table(fund_sum)

# fund hdr
hdr <- dbGetQuery(wrds, "SELECT * FROM crsp.fund_hdr") |> as.data.table()

#-----------------------------------------#
# 3. Get data from monthly tna
#-----------------------------------------#
varlist_tna <- c('caldt', 'crsp_fundno', 'mtna')
fund_tna <- dbGetQuery(wrds, 
                       sprintf("SELECT %s FROM crsp.monthly_tna
                                WHERE caldt >= '01/01/2000'",
                               paste(c(varlist_tna), collapse = ',')))
fund_tna <- as.data.table(fund_tna)

#-----------------------------------------#
# 4. Get data from monthly returns
#-----------------------------------------#
varlist_ret <- c('caldt', 'crsp_fundno', 'mret')
fund_ret <- dbGetQuery(wrds, 
                       sprintf("SELECT %s FROM crsp.monthly_returns
                                WHERE caldt >= '01/01/2000'",
                               paste(c(varlist_ret), collapse = ',')))
fund_ret <- as.data.table(fund_ret)

#-----------------------------------------#
# 5. Get merged data of tna, ret, and nav
#-----------------------------------------#
fundm <- dbGetQuery(wrds, "SELECT * FROM crsp.monthly_tna_ret_nav WHERE caldt >= '01/01/2000'")
fundm <- as.data.table(fundm)

#-----------------------------------------#
# 5. Get data from portno map
#-----------------------------------------#
varlist_map <- c('crsp_fundno', 'crsp_portno', 'fund_name', 'begdt', 'enddt', 'cusip8', 'ncusip')
link <- dbGetQuery(wrds, 
                   sprintf("SELECT %s FROM crsp.portnomap
                           WHERE begdt >= '01/01/2000'",
                           paste(c(varlist_map), collapse = ',')))
link <- as.data.table(link)


# ================================================================= #
# Write data ####
# ================================================================= #
#-----------------------------------------#
# 1. MF fund-bond-month level holdings
#-----------------------------------------#
# only keep if there is a cusip
vcmf <- cmf[!is.na(cusip), ]
print(sprintf("After dropping null CUSIPs, crsp holdings: %.2f%%", nrow(vcmf)/nrow(cmf)*100))
print(sprintf("Number of unique crsp_portno funds from CRSP: %.0f", uniqueN(vcmf$crsp_portno)))

# create month column
rm(cmf)
vcmf[, date_m := format(as.Date(report_dt), '%Y-%m')]

# save files
saveRDS(vcmf, file = paste0(RAWDIR, 'crsp/crsp_mf_holdings_cusip.rds'))
write.csv(vcmf, paste0(RAWDIR, 'crsp/crsp_mf_holdings_cusip.csv'), row.names = FALSE)

#-----------------------------------------#
# 2. MF fund level summary
#-----------------------------------------#
# save files
saveRDS(fund_sum, file = paste0(RAWDIR, 'crsp/crsp_mf_sum.rds'))
saveRDS(hdr, file = paste0(RAWDIR, 'crsp/crsp_mf_hdr.rds'))
saveRDS(fund_tna, file = paste0(RAWDIR, 'crsp/crsp_mf_mtna.rds'))
saveRDS(fund_ret, file = paste0(RAWDIR, 'crsp/crsp_mf_mret.rds'))
saveRDS(fundm, file = paste0(RAWDIR, 'crsp/crsp_mf_fundm.rds'))
saveRDS(link, file = paste0(RAWDIR, 'crsp/crsp_portno_link.rds'))






