#=======================================================================================#
# import_markit_cds.R ####
#=======================================================================================#
# Description:
# -------------
#     This script downloads and saves Markit CDS data.
#     It uses RPostgres direct download.
#
# Input(s):
# ---------
#     WRDS connection
#   
# Output(s) :
# ----------
#   SQL tables at sts-liramota.mit.edu:
#     1. markit.cds2001-markit.cds2023 (23 batches)
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
#     2022-05-12
#     update: 2023-11-13
#
# Author:
# --------
#     Lira Mota Mertens, liramota@mit.edu
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
source("utilities/utilities.R")

# Create database connections
print("Connecting to WRDS and SQL Server.")
wrds <- wrds_con()
iswindows <- Sys.info()["sysname"] == "Windows"
db <- db_con("markit", windows_auth = iswindows)
connectArgs <- bcputility::makeConnectArgs(server = Sys.getenv("mssql_server"), database = "markit")

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
  if(like(data[n],'cds')&data[n]!="cdslookup"){
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
  #May disconnect if upload to SQL takes too long, so reconnect:
  wrds <- wrds_con() 
  data_n <- dbGetQuery(wrds, sql)
  data_n <- as.data.table(data_n)
  
  print("Uploading to SQL Server.")
  print(paste0("NRows: ", nrow(data_n)))
  db_write(db, connectArgs, data[n], data_n)
  
  if (like(data[n], "cds") & data[n] != "cdslookup"){
    # Pick couple random portfolios to check
    rand_dates <- unique(data_n$date) |> sample(size = 1)
    test_local <-  data_n[date %in% rand_dates]
    check_db(
      db,
      data[n],
      test_local,
      sort_cols = sort_cols,
      subset_cmd = paste0(
        "SELECT * FROM ",
        data[n],
        " WHERE date BETWEEN '",
        rand_dates, "' AND '", rand_dates, "'"))
    
  } else {
    check_db(db, data[n], data_n, sort_cols = sort_cols)
  }
  
  print(sprintf('We just downloaded %s.', data[n]))
  rm(data_n)
  gc()
}

dbDisconnect(db)
dbDisconnect(wrds)
print('CDS tables have been downloaded successfully.')
