#=======================================================================================#
# cds_clean_us_daily.R ####
#=======================================================================================#
# Description:
# ---------- -
#    Daily version of CDS cleaning
#
# Input(s) :
# ------- -
#   All markit.cds2001-markit.cds2023
#
# Output(s) :
# ------- -
#   SQL table markit.cds_us_daily at sts-liramota.mit.edu
#
# Date:
# --- -
#     2023-07-05
#
# Author(s):
# ----- -
#     Nick von Turkovich
#
# Additional note(s):
# ------------ -
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
# Read and clean data ####
# ================================================================= #

# Get filepaths
cds_files <- DBI::dbListTables(db)
cds_files <- cds_files[grepl("cds2", cds_files)]

# Clean data
dfs <- cds_files |>
  purrr::map(.f = function(df_name) {
    
    # Read file
    df <- DBI::dbReadTable(db, df_name) |> as.data.table()
    
    # No missing redcode, only senior, USD and US
    df <- df[!is.na(redcode) & tier == "SNRFOR" & currency == "USD" & country == "United States"]
    
    # Standard docclause
    df <- df[docclause %in% c("XR", "XR14")]
    
    # Remove if parspread is NA
    df <- df[!is.na(parspread)]
    
    # Check for duplicates
    if (any(duplicated(df, by = c("date", "redcode", "tenor")))){
      
      # Take the XR14 if available
      df <- unique(df[order(redcode, date, tenor, -docclause)], by = c("date", "redcode", "tenor"))
      
    }
    
    # Subset columns that are necessary
    df <- df[,.(redcode, date, ticker, shortname, tenor, parspread, avrating)]
    
    print(paste0("Year ", lubridate::year(df$date[1]), " done"))
    return(df)
  })

# Collapse and sort files
cds_daily <- dfs |> purrr::reduce(dplyr::bind_rows) |> as.data.table()
cds_daily <- cds_daily[order(redcode, date, tenor)]

# ================================================================= #
# Save data ####
# ================================================================= #

# Save US CDS Data 
db_write(db, connectArgs, "cds_us_daily", cds_daily)
check_db(db, "cds_us_daily", cds_daily)
dbDisconnect(db)
print('Successfully created cds daily')



