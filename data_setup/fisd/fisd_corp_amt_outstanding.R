# ================================================================= #
# fisd_corp_amt_outstanding.R ####
# ================================================================= #
# Description:
# ------------
#   FISD amt_out_hist table reports changes in the amount outstanding. This code generates
#   the amount outstanding for each bond from offering data to maturity.
#
# Input(s):
# ------------
#   Dropbox data/raw/fisd/:
#     1. fisd_issue.rds
#     2. fisd_amt_out_hist.rds
#     3. fisd_amt_out.rds
#
# Output(s) :
# --------
#   Dropbox FISD data:
#     1. data/raw/fisd/amt_outstanding/fisd_amt_outstanding_batch (139 tables, approx 50 mins)
#     2. data/processed/fisd/fisd_corp_amt_out_standing.rds
# 
# Date:
# -----
#   2023-02-07
#   update: 2026-01-23
#
# Author(s):
# -------
#   Lira Mota, liramota@mit.edu
#   Ruiquan Chang, chang.2590@osu.edu
#   Ziqi Li, ziqili7@illinois.edu
#
# Additional note(s):
# ----------
#     We only consider offerting dates bigger than '1901-01-01'.
#     In 1994 there were some 100 year maturity bonds outstading.
#
# ================================================================= #


# ================================================================= #
# Environment ####
# ================================================================= #
# Clear workspace
rm(list = ls())

# Import libraries
# install.packages("RQuantLib", type="binary") # In Windows
# install.packages("RQuantLib")
library(RQuantLib)
library(data.table)

# Source helper scripts
source('utils/setPaths.R')


# ================================================================= #
# Read Data ####
# ================================================================= #
issue_data <- readRDS(paste0(RAWDIR, "fisd/fisd_issue.rds"))
issue_data <- issue_data[, .(issue_id, issuer_id, offering_date, maturity, offering_amt)]
issue_data <- issue_data[!is.na(offering_amt)]

current <- readRDS(paste0(RAWDIR, "fisd/fisd_amt_out.rds"))
hist <- readRDS(paste0(RAWDIR, "fisd/fisd_amt_out_hist.rds"))


# ================================================================= #
# Process Historical Outstanding Data ####
# ================================================================= #
# append data 
hist <- rbind(hist, current, fill=TRUE)
hist[duplicated(hist, by = colnames(current))]
hist <- hist[order(issue_id, effective_date, transaction_id)]

# check NAs
hist[is.na(amount_outstanding)&(action_type!="I")]

hist[issue_id==648857]
hist[issue_id==612831]
# View(hist[issue_id==201053])
# View(hist[issue_id==408673])
# View(hist[issue_id==72329])

# Drop all NAs in amount_outstanding 
# From the checks above, the NAs resulting from partial bond calls should not be counted
# For the NAs from the initial offering, we can drop them for now. 
# Since we're going to combine the issue table later, we won't lose these initial offering obs anyway.
hist <- hist[!is.na(amount_outstanding)]  # 186 observations

# check neagtive outstanding
hist[amount_outstanding<0]
hist[issue_id==1184910]

# Replace negative amount outstanidng with 0
# The negative outstanding comes from the action amount being greater than the total outstanding (the firm called or repurchased the bonds)
# So, the date on which the outstanding becomes negative should be treated as the end date of the bond.
hist <- hist[amount_outstanding < 0, amount_outstanding := 0]  # 22 observations

# Historical Data: we need the issue_id and effective date to be a primary key.
# Based on the Mergent Fisd documentation, "transaction is a Mergent-generated number unique 
# to each record, highest number will be the most current".
# We then select only the last transaction id.
hist <- hist[order(issue_id, effective_date, transaction_id)]
hist <- unique(hist, by = c('issue_id', 'effective_date'), fromLast = TRUE)
range(hist[!is.na(effective_date)]$effective_date)  # "1895-10-01" "2025-07-17"


# ================================================================= #
# Build Calendar ####
# ================================================================= #
# Start date
start_cal <- issue_data[, min(offering_date, na.rm = T)]; start_cal
# biz days only work for dates bigger than '1901-01-01'
if(year(start_cal) <= 1915){start_cal = as.Date('1901-01-01')}
issue_data <- issue_data[offering_date >= start_cal]

# End date
end_cal <- issue_data[, max(maturity, na.rm = T)]; end_cal
# biz days only work for dates smalles than '2199-12-31'
if(year(end_cal) > 2199){end_cal = as.Date('2100-12-31')}

# Build calendar
biz_days <- data.table(dates = seq(start_cal, end_cal, by="1 day"))
biz_days <- biz_days[isBusinessDay("UnitedStates", dates)]
biz_days[, ym := format(dates, "%Y-%m")]
dates <- biz_days[, .(dates = max(dates)), by = ym][, dates]

# Fix maturity missing: Exclude missing maturity 
issue_data[, mean(is.na(maturity))]
issue_data <- issue_data[!is.na(maturity)]

# sanity check
all(dates == as.Date(as.character(dates)))


# ================================================================= #
# Create amt_outstanding from amt_outstanding history ####
# ================================================================= #
# complete hist with first issues
amt_hist <- hist[, .(issue_id, date = effective_date, amt_outstanding = amount_outstanding)]

# check all issues in hist are present in the issue_data table: TRUE (not really necessary for join to work)
# all(amt_hist[,unique(issue_id)]%in%issue_data[,unique(issue_id)])

# Combine hist and issue to build esqueleton
amt_hist <- rbind(amt_hist, 
                  issue_data[, .(issue_id, date = offering_date, amt_outstanding = offering_amt)])
setkey(amt_hist, issue_id, date)

# amt_hist[,.N, by=.(issue_id, date)][order(-N)][N>=2]
# obs that amount outstanding changes in the day of the offering.
# Keep the value in the hist table - it is the most recent one!
amt_hist <- unique(amt_hist, by = c('issue_id', 'date'))

amt_hist[!is.na(date), range(date)] # "1895-10-01" "2026-09-26"


#========================================================================#
# Build Batches Table ####
#========================================================================#
issues <- issue_data$issue_id

create_dates_table <- function(issue, end_cal = end_cal){ 
  start_date <- issue_data[issue_id==issue, offering_date]  
  # end date mingh be missing when it is a perpetual bond
  end_date <- issue_data[issue_id==issue, maturity]
  issue_life <- dates[dates >= start_date & dates <= end_date]
  
  if(length(issue_life)>0){
    issue_data <- data.table(issue_id = issue, date = issue_life)    
  } else {
    issue_data <- data.table(issue_id = numeric(), date = as.Date(character()))
  }
  issue_data
}

# run create_dates_table on batches of issues (for memory concerns)
batch_size <- 5000
batches <- seq(0, floor(length(issues) / batch_size)) * batch_size
batches <- lapply(batches, FUN=function(x){x + 1:batch_size})
batches[[length(batches)]] <- seq(min(batches[[length(batches)]]), length(issues)) # trim to avoid NA
num_batches <- length(batches)

counters <- list()
counters$num_issues <- numeric()
counters$num_lines <- numeric()

system.time({ 
  for(n in 1:num_batches){ # n <- 1
    tables <- lapply(issues[batches[[n]]], create_dates_table)
    batch_issue_data <- Reduce(rbind, tables)
    setkey(batch_issue_data, issue_id, date)
    res <- amt_hist[batch_issue_data, roll=TRUE]
    
    ## log some stats
    counters$num_issues[n] <- res[,length(unique(issue_id))]
    counters$num_lines[n] <- nrow(res)
    
    fileName <- paste0(RAWDIR, "fisd/amt_outstanding/batch_", n, ".rds") # file name
    saveRDS(res, fileName)
    print(paste0("batch ", n, " complete, ", format(counters$num_lines[n] / 10^6, digits = 4), "M lines."))
  }
}) # takes ~ 50 minutes

# counters stats 
length(issues) - sum(counters$num_issues) # 146 issues with no data! 
sum(counters$num_lines) # Total base has 43 Million lines (43115940)


#========================================================================#
# Combine all Betches ####
#========================================================================#
# compose batches into final table
tables <- list()

for(n in 1:num_batches){
  fileName <- paste0(RAWDIR, "/fisd/amt_outstanding/batch_",n,".rds") # fileName
  tables[[n]] <- readRDS(fileName)  
}  
final_data <- Reduce(rbind, tables)  

## sanity check
nrow(final_data) == sum(counters$num_lines)
final_data[, length(unique(issue_id))] == sum(counters$num_issues)
final_data[, class(date)]

# Manual Fix
final_data[issue_id==682639 & amt_outstanding > 3e8, amt_outstanding := amt_outstanding/1000]


#========================================================================#
# Write Data ####
#========================================================================#
if(any(duplicated(final_data[, .(issue_id, date)]))){
  stop('Data primary key violated.')
}
range(final_data$date)  # "1902-03-31" "2122-04-15"

saveRDS(final_data, file = paste0(PROCDIR, 'fisd/corp_amt_outstanding.rds'))
print('Successfully created final amount outstanding table.')


