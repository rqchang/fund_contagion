# ================================================================= #
# import_opensource_trace.R ####
# ================================================================= #
# Description :
# ---------- -
#     This code reads in daily Trace data
#
# Input(s) :
# ------- -
#   https://openbondassetpricing.com/data/
#
# Output(s) :
# ------- -
#   trace.dbo.trace_daily
#
# Date:
# --- -
#     2024-02-28
#
# Author(s):
# --------- -
#     Nick von Turkovich
#
# ================================================================= #

# ================================================================= #
# Environment ####
# ================================================================= #

# Clear workspace
rm(list = ls()); gc()

# Import libraries

# Helper functions
source("utilities/utilities.R")

# Create database connections
dbs <- list(trace = db_con("trace"))
connectArgs <- list(trace = db_conArgs("trace"))

# ================================================================= #
# Download Data ####
# ================================================================= #

# Download
download.file(url = "https://openbondassetpricing.com/wp-content/uploads/2023/12/BondDailyDataPublic.zip",
              destfile = "./data/trace/BondDailyDataPublic.zip",
              method = "curl")
unzip("./data/trace/BondDailyDataPublic.zip", 
      overwrite = TRUE,
      exdir = "./data/trace/")

# Read in file
trace_daily <- readr::read_csv("./data/trace/BondDailyDataPublic.csv.gzip")

# Drop rownum column
trace_daily <- trace_daily |> 
  dplyr::select(-`...1`) 

trace_daily <- trace_daily |>
  as.data.table()

# ================================================================= #
# Save Data ####
# ================================================================= #
db_write(dbs$trace, connectArgs$trace, "trace_daily", trace_daily, c('cusip_id', 'trd_exctn_dt'))

lapply(dbs, DBI::dbDisconnect)

file.remove("./data/trace/BondDailyDataPublic.zip")
file.remove("./data/trace/BondDailyDataPublic.csv.gzip")




