#=======================================================================================#
# import_markit_cds.R ####
#=======================================================================================#
# Description:
# -------------
#   This script downloads and saves Markit CDS data.
#   It uses RPostgres direct download.
#
# Input(s):
# ---------
#   WRDS connection
#   
# Output(s) :
# ----------
#   Dropbox data/raw/markit:
#     1. markit.cds2001-markit.cds2026 (26 batches)
#     2. markit.cdslookup
#     3. markit.cdxcomps
#     4. markit.cdxcomps_chars
#     5. markit.cdxconst
#     6. markit.cdxconst_chars
#     7. markit.chars
#     8. markit.isdatt
#     9. markit.red_chars
#     10. markit.redent
#     11. markit.redentaucccy
#     12. markit.redentcorpact
#     13. markit.redentcredauc
#     14. markit.redentisda
#     15. markit.redentrating
#     16. markit.redentsuccession
#     17. markit.redindex
#     18. markit.redlookup
#     19. markit.redobl
#     20. markit.redoblindxconst
#     21. markit.redobllookup
#     22. markit.redoblprospectus
#     23. markit.redoblrefentity
#
# Date:
# ------
#   2022-05-12
#   update: 2026-01-23
#
# Author:
# --------
#   Lira Mota Mertens, liramota@mit.edu
#   Ruiquan Chang, chang.2590@osu.edu
#
# Additional note(s):
# -----------------------------
# Description Markit CDS ####
# https://wrds-www.wharton.upenn.edu/documents/726/CDS_Glossary.pdf?_ga=2.91136082.992415377.1539273319-1500651139.1501534854
#
# docclause - CR, MR, MM or XR:
# Restructuring Credit Event – One of the types of credit events which trigger settlement under the CDS
# contract. Restructuring is a “soft” event, whereby the loss to the owner of the reference obligation is not
# obvious. In addition, Restructuring often retains a complex maturity structure, so that debt of different maturities
# may remain outstanding with significant differences in value. The following are the different types of
# Restructuring clauses:
#   *  Full Restructuring (CR): This allows the Protection Buyer to deliver bonds of any maturity after
#       restructuring of debt in any form occurs. This type of clause is more prevalent in Asia.
#   *  Modified Restructuring (MR): limits deliverable obligations to bonds with maturities of less than 30
#       months after a credit event.
#   *  Modified Modified Restructuring (MM): This is a “modified” version of the Modified Restructuring
#       clause whereby deliverable obligations can mature up to 60 months (5 years) following the credit
#       event. This type of clause if more prevalent in Europe.
#   *  No Restructuring (XR): This option excludes restructuring altogether from the CDS contract,
#       eliminating the possibility that the Protection Seller suffers a “soft” Credit Event that does not
#       necessarily result in losses to the Protection Buyer. No-R protection typically trades cheaper than
#       Mod-R protection. Following the implementation of SNAC, this clause is mainly traded in North America.
# 
# Recovery Rate:
# An estimate of percentage of par value that bondholders will receive after a credit event.
# CDS for investment grade bonds generally assume a 40% recovery rate when valuing CDS trades. However,
# CDS for lower rated bonds are more dynamic and often reflect lower estimated recovery rates.
#
# ================================================================= #

# ================================================================= #
# Environment ####
# ================================================================= #
# Clear workspace
rm(list = ls())

# Helper functions
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


# ================================================================= #
# Download and Save Data ####
#================================================================== #
print('Successfully started CDS prices download.')

res <- dbSendQuery(wrds, "select distinct table_name
                   from information_schema.columns
                   where table_schema='markit'
                   order by table_name")
data <- dbFetch(res, n=-1)
dbClearResult(res)
data <- data[,1]

# Do not download itrax 
data <- data[!like(data, 'itrax')]
N <- length(data)

# Download all data from Markit
#-------------------------------------------------------------------#
for(n in c(1:N)){
  if(like(data[n],'cds') & data[n]!="cdslookup"){
    sql <- sprintf("SELECT redcode,batch, date, tier, ticker, shortname, docclause, sector, region, country, currency,
                            tenor, parspread, convspreard, upfront, cdsrealrecovery, cdsassumedrecovery, compositepricerating,
                            compositedepth5y, compositecurverating, avrating, idxmember, quotescountcurve, 
                            upfrontbaspreadaverage, convbaspreadaverage, estimatednotional,
                            curveliquidityscore, gnxtrd, wklynetnotional, dp
                   FROM markit.%s 
                   WHERE (currency = 'USD' OR currency = 'EUR') AND tier='SNRFOR'
                   ", data[n])
    sort_cols <- c("redcode", "date", "ticker", "shortname", "docclause", "tenor","upfront", "dp", "parspread", "convspreard")
    } else{  
    sql <- sprintf("SELECT * FROM markit.%s ", data[n])
    sort_cols = NULL
    }
  print("Downloading from WRDS.") 
  data_n <- dbGetQuery(wrds, sql)
  data_n <- as.data.table(data_n)
  
  print("Uploading to Dropbox.")
  print(paste0("NRows: ", nrow(data_n)))
  out_file <- file.path(RAWDIR, "markit", paste0(data[n], ".rds"))
  saveRDS(data_n, out_file)
  
  print(sprintf('We just downloaded %s.', data[n]))
  rm(data_n)
  gc()
}

dbDisconnect(wrds)
print('CDS tables have been downloaded successfully.')


