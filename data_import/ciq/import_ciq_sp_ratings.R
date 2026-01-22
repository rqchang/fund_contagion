#=======================================================================================#
# import_ciq_sp_ratings.R ####
#=======================================================================================#
# Description:
# ---------- -
#     This downloads and saves S&P ratings data from WRDS server.
#     It uses RPostgres direct download.
#
# Input(s):
# --------
#     WRDS connection
#
# Output(s) :
# ------- -
#     SQL tables at sts-liramota.mit.edu: 
#       1. ciq.sp_ratings_erating
#       2. ciq.sp_ratings_irating
#       3. ciq.sp_ratings_srating
#       4. ciq.sp_ratings_sassessment
#       5. ciq.sp_ratings_gvkey
#       6. ciq.sp_ratings_ticker
#       7. ciq.sp_ratings_cusip
#       8. ciq.sp_ratings_cik
#       9. ciq.sp_ratings_isin
#
# Date:
# --- -
#     2022-05-12
#    
# Author:
# ----- -
#     Lira Mota Mertens, liramota@mit.edu
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
wrds <- wrds_con()
db <- db_con("ciq")
connectArgs <- bcputility::makeConnectArgs(server = Sys.getenv("mssql_server"), database = "ciq")

# ================================================================= #
# Parameters ####
# ================================================================= #
print('Successfully started S&P ratings tables download.')

# Tables to be downloaded
tables <- c("wrds_erating", # entity rating
            "wrds_irating", # instrument rating
            "wrds_srating", # security rating
            "wrds_sassessment", # security assesment 
            "wrds_gvkey",
            "wrds_ticker",
            "wrds_cusip",
            "wrds_cik",
            "wrds_isin"
            )
pks <- list("ratingdetailid", 
        "ratingdetailid", 
        "ratingdetailid", 
        "assessmentdetailid", 
        "", # Duplicates present
        "", # Duplicates present
        "", # Duplicates present
        "", # Duplicates present
        "" # Duplicates present
)


# ================================================================= #
# Download and Save Data ####
# ================================================================= #

N <- length(tables)
for(n in c(1:N)){
  sql <- sprintf("SELECT * FROM ciq.%s", tables[n])
  data_n <- dbGetQuery(wrds, sql)
  data_n <- as.data.table(data_n)
  
  db_write(db, connectArgs, tables[n], data_n, pk = pks[[n]])
  check_db(db, tables[n], data_n, sort_cols = pks[[n]])
  
  print(sprintf('We just downloaded %s.', tables[n]))
  rm(data_n)
}

dbDisconnect(wrds)
dbDisconnect(db)
print('S&P ratings tables has been downloaded successfully.')




