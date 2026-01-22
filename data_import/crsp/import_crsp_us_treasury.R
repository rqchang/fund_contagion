# ================================================================= #
# import_crsp_us_treasury.R ####
# ================================================================= #
# Description:
# ------------
#     This script downloads US Treasury fixed-term rates from CRSP WRDS server.
#     It uses RPostgres direct download.
#
# Input(s):
# ---------
#     WRDS connection
#
# Output(s):
# ----------
#     CRSP raw data:
#     Data/raw/CRSP/treasury_fixed_rates/tfz.rds
#     Data/raw/CRSP/treasury_fixed_rates/ycurve.rds
#
# Date:
# ----------
#     2022-05-12
#     update: 2023-11-07
#
# Author(s):
# ----------
#     Lira Mota Mertens, liramota@mit.edu
#
# Additional note(s):
# ----------
#
#   -----------------------------------------------------#
#     A valid issue that best represents each term is chosen at the end of each month and held 
#     through the next month for each of the fixed-term periods. 
#     Valid issues are at least 6 months from, but closest to the target maturity date.
#     They are fully taxable, non-callable, and non-flower bonds. 
#     When more than one issue meets the criteria, the one most recently issued is used. 
#     If no issue meets the criteria, a second pass is made that allows flower bonds
#     https://wrds-www.wharton.upenn.edu/documents/407/CRSP_US_Treasury_Database_Guide.pdf (Page 12)
#
#     US Treasury Fixed-Term Indexes: Variable Description  
#   -----------------------------------------------------#
#     (KY)TREASNOX See mappings below for TERMTYPE 9d 9d
#     CALDT Quotation Date 8d 8d
#     RMTREASNO Monthly Series of Related TREASNOs 8d 8d
#     RMCRSPID Monthly Series of Related CRSPIDs 16s 16s
#     TMYEARSTM Monthly Series of Years to Maturity 6.3lf 20.12e
#     TMDURATN Monthly Series of Macaulay’s Duration 6.1lf 20.12e
#     TMRETADJ Monthly Series of Return Adjusted (TMRETNUA * 100) 9.4lf 20.12e
#     TMYTM Monthly Series of Yield to Maturity (TMYLD * 36500) 9.4lf 20.12e
#     TMBID Monthly Bid 11.6lf 20.12e
#     TMASK Monthly Ask 11.6lf 20.12e
#     TMNOMPRC Monthly Nominal Price 11.6lf 20.12e
#     TMNOMPRC_FLG Monthly Nominal Price Flag 1c 1c
#     TMACCINT Monthly Series of Total Accrued Interest 11.6lf 20.12e
#     MAPPINGS TREASNOX 2000003 (contains TERMTYPE 0112 ), 2000004 (0212), 
#                       2000005 (0512) , 2000006(0712)
#     TREASNOX 2000007 (contains TERMTYPE 1012 ), 2000008 (2012), 2000009 (3012) 
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

# Source helper scripts
source('utils/setPaths.R')
source('utils/wrds_credentials.R')

# Create database connections
wrds <- dbConnect(Postgres(),
                  host = 'wrds-pgdata.wharton.upenn.edu',
                  port = 9737,
                  user = 'rqchang99',
                  password = 'Crq-19990711',
                  sslmode ='require',
                  dbname ='wrds')


#================================================================== #
# Read data ####
# ================================================================= #
#-----------------------------------------#
# 1. US Treasury monthly
#-----------------------------------------#
print('Successfully started US Treasury yield curve download.')

# Key Rates
sql1 <- "select * from crspq.tfz_mth_rf"
res1 <- dbSendQuery(wrds, sql1)
tfz1 <- as.data.table(dbFetch(res1, n = -1))
dbClearResult(res1)
rm(res1)

# Key Rates 2
sql2 <- "select * from crspq.tfz_mth_ft"
res2 <- dbSendQuery(wrds, sql2)
tfz2 <- as.data.table(dbFetch(res2, n = -1))
dbClearResult(res2)
rm(res2)

tfz1 <- tfz1[,.(date=mcaldt, kytreasnox, tmytm)]
tfz2 <- tfz2[,.(date=mcaldt, kytreasnox, tmytm)]

tfz <- rbind(tfz1, tfz2)
rm(tfz1,tfz2, sql1, sql2)

# Create a readable maturity type
tfz[kytreasnox==2000001, term_type := 1/12]
tfz[kytreasnox==2000002, term_type := 3/12]
tfz[kytreasnox==2000003, term_type := 1]
tfz[kytreasnox==2000004, term_type := 2]
tfz[kytreasnox==2000005, term_type := 5]
tfz[kytreasnox==2000006, term_type := 7]
tfz[kytreasnox==2000007, term_type := 10]
tfz[kytreasnox==2000008, term_type := 20]
tfz[kytreasnox==2000009, term_type := 30]
#tfz[,kytreasnox:=NULL]

# Create mdate
tfz[,mdate:=year(date)*100+month(date)]

ycurve <- dcast.data.table(tfz[,.(mdate, term_type, tmytm)], 
                           'mdate~term_type', 
                           value.var = 'tmytm')
setkey(ycurve, "mdate")

#-----------------------------------------#
# 2. US Treasury daily
#-----------------------------------------#
# Key Rates
sql <- "select * from crspq.tfz_dly_ft"
res <- dbSendQuery(wrds, sql)
tfz_d <- as.data.table(dbFetch(res, n = -1))
dbClearResult(res)
rm(res)

tfz_d <- tfz_d[, .(date = caldt, kytreasnox, tdytm)]
# Create a readable maturity type
tfz_d[kytreasnox==2000003, term_type := '1y']
tfz_d[kytreasnox==2000004, term_type := '2yr']
tfz_d[kytreasnox==2000005, term_type := '5yr']
tfz_d[kytreasnox==2000006, term_type := '7yr']
tfz_d[kytreasnox==2000007, term_type := '10yr']
tfz_d[kytreasnox==2000008, term_type := '20yr']
tfz_d[kytreasnox==2000009, term_type := '30yr']

ycurve_d <- dcast.data.table(tfz_d[,.(date, term_type, tdytm)], 
                             'date~term_type', 
                             value.var = 'tdytm')
setkey(ycurve_d, "date")


# ================================================================= #
# Write Data ####
# ================================================================= #
# UST monthly
saveRDS(tfz, paste0(RAWDIR, 'CRSP/treasury_fixed_rates/tfz.rds'))
saveRDS(ycurve, paste0(RAWDIR, 'CRSP/treasury_fixed_rates/ycurve.rds'))

# UST daily
saveRDS(tfz_d, paste0(RAWDIR, 'CRSP/treasury_fixed_rates/tfz_d.rds'))
saveRDS(ycurve_d, paste0(RAWDIR, 'CRSP/treasury_fixed_rates/ycurve_d.rds'))

dbDisconnect(wrds)
print('US Treasury yield tables have been downloaded successfully.')




