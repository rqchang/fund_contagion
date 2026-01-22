#=======================================================================================#
# cds_clean_us.R ####
#=======================================================================================#
# Description:
# ---------- -
#    First clean in the CDS Data
#
# Input(s) :
# ------- -
#   All markit.cds2001-markit.cds2023
#
# Output(s) :
# ------- -
#   SQL table markit.cds_us at sts-liramota.mit.edu
#
# Date:
# --- -
#     2019-08-28
#     update: 2023-11-13
#
# Author(s):
# ----- -
#     Lira Mota, liramota@mit.edu
#
# Additional note(s):
# ------------ -
# Read and select US securities
#
# The CDS curve primary key is entity/tier/currency/doc
# "The curve display of the credit spread for a unique reference 
# entity/tier/currency/doc- clause combination over different nodes or tenors."
# https://wrds-www.wharton.upenn.edu/documents/726/CDS_Glossary.pdf
#
# Data entry details:
# Country: The country of the reference entity.
#
# ================================================================= #

# ================================================================= #
# Environment ####
# ================================================================= #

# Clear workspace
rm(list = ls())

# Import libraries

# Helper functions
source("utilities/utilities.R")

# Create database connections
db <- db_con("markit")
connectArgs <- bcputility::makeConnectArgs(server = Sys.getenv("mssql_server"), database = "markit")

# ================================================================= #
# Load Selected CDS Data ####
# ================================================================= #
# ----------------------------------------------------------------- #
# CDS Data Load ####

#  Load CDS
rfiles <- dbListTables(db)
rfiles <- rfiles[grepl("cds\\d{4}", rfiles)]

# I cannot load everything in memory.
# Load and clean at the same time
cds <- data.table()

for(r in rfiles){
  print(paste0('We are downloading year ',gsub(".*[CDS/]([^.]+)", "\\1", r), '.'))
  cds_r <- db_read(db, r)
  
  # Missing identifier
  cds_r <- cds_r[!is.na(redcode)]
  
  # Only consider senior
  cds_r <- cds_r[tier == "SNRFOR"]
  
  # Only consider US and dollar
  cds_r <- cds_r[currency == 'USD']
  cds_r <- cds_r[country == 'United States']
  
  # We consider only XR, the standard in the US and MM and MR, the standard in Europe
  print(sprintf('We select %.2f%% of XR restructure.',
                cds_r[,mean(docclause%in%c('XR', 'XR14'))]*100))
  cds_r <- cds_r[docclause%in%c('XR', 'XR14')]
  
  # Modified Modified Restructuring (MM): This is a â€œmodifiedâ€ version of the Modified Restructuring
  #                                       clause whereby deliverable obligations can mature up to 60 months (5 years) following the credit
  #                                       event. This type of clause if more prevalent in Europe.
  # No Restructuring (XR): This option excludes restructuring altogether from the CDS contract,
  #                        eliminating the possibility that the Protection Seller suffers a â€œsoftâ€ Credit Event that does not
  #                        necessarily result in losses to the Protection Buyer. No-R protection typically trades cheaper than
  #                        Mod-R protection. Following the implementation of SNAC, this clause is mainly traded in North America.
  
  # Duplicated
  cds_r <- cds_r[order(redcode, date, tenor, docclause)]
  cds_r <- unique(cds_r, by = c('redcode', 'date', 'tenor'), fromLast = TRUE)
  
  # Save monthly data
  cds_r[,mdate:=year(date)*100+month(date)]
  cds_r <- unique(cds_r, by = c('redcode', 'tenor', 'mdate'), fromLast = TRUE)
  
  cds <- rbind(cds, cds_r)
  rm(cds_r)
  gc()
}
rm(rfiles, r)

# ================================================================= #
# Save Cleaned CDS Data for the US ####
# ================================================================= #
# Save US CDS Data 
db_write(db, connectArgs, "cds_us", cds)
dbDisconnect(db)
print('Successfully created cds')


