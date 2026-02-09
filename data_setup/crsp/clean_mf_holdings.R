# ================================================================= #
# clean_mf_holdings.R ####
# ================================================================= #
# Description:
# ------------
#   This file is to clean CRSP MF holdings at the fund-bond-month level.
#   Conditional on: corporate bonds, bond ttm >= 1, positive AUM, non-null flows.
#   Compute: market value, paramt, market cap, totparamt, portfolio weight.
#
# Input(s):
# ------------
#   Dropbox:
#     data/processed/crsp/crsp_mf_flows_m.rds
#     data/raw/crsp/crsp_mf_holdings_cusip.rds
#     data/processed/fisd/corp_amt_outstanding.rds
#     data/raw/trace/wrds_bondret.rds
#
# Output(s):
# ------------
#   Dropbox:
#     data/processed/crsp/mf_corp_holdings_m.rds
#
# Date:
# ------------
#   2026-01-28
#   update:
#
# Author(s):
# ------------
#   Ruiquan Chang, chang.2590@osu.edu
#
# Additional note(s):
# ------------
#     
# ================================================================= #


# ================================================================= #
# Environment ####
# ================================================================= #
# Clear workspace
rm(list = ls())

# Import libraries
library(data.table)
library(parallel)
library(zoo)
library(lubridate)

# Source helper scripts
source('utils/setPaths.R')

# Function
remove_outliers <- function(DT, var) {
  stopifnot(is.data.table(DT))
  v <- DT[[var]]
  p1  <- as.numeric(quantile(v, probs = 0.01, na.rm = TRUE, type = 7))
  p99 <- as.numeric(quantile(v, probs = 0.99, na.rm = TRUE, type = 7))
  DT[get(var) < p1,  (var) := p1]
  DT[get(var) > p99, (var) := p99]
  invisible(DT)
}


# ================================================================= #
# Read data ####
# ================================================================= #
# -------------------------------------------- #
# CRSP data
# -------------------------------------------- #
# Fund-month CRSP MF flows
crsp_flow <- readRDS(paste0(PROCDIR, 'crsp/crsp_mf_flows_m.rds'))
cat("Read fund-month level CRSP MF flow data:", dim(crsp_flow))

crsp_flow[, year := year(as.IDate(caldt))]
crsp_flow[, date_m_id := year(caldt) * 100L + month(caldt)]
check_flow <- crsp_flow[
  ,
  .(
    crsp_portno_count   = .N,
    crsp_portno_nunique = uniqueN(crsp_portno),
    mtna_count          = sum(!is.na(mtna)),
    mtna_lag_count      = sum(!is.na(mtna_lag)),
    mret_count          = sum(!is.na(mret)),
    mret_lag_count      = sum(!is.na(mret_lag)),
    flow_count          = sum(!is.na(flow))
  ),
  by = year
]
print(check_flow)

# Fund-bond-month CRSP holdings
crsp_ho <- readRDS(paste0(RAWDIR, 'crsp/crsp_mf_holdings_cusip.rds'))
crsp_ho[, c('permno', 'permco') := NULL]
setnames(crsp_ho, "cusip", "cusip8")
crsp_ho[, date_m_id := as.integer(gsub("-", "", substr(date_m, 1, 7)))]
cat("Read CRSP MF holdings data:", dim(crsp_ho), range(crsp_ho$date_m_id))

# -------------------------------------------- #
# FISD + WRDS Bond Returns data
# -------------------------------------------- #
# Bond-month level amount outstanding
df_fisd <- readRDS(paste0(PROCDIR, 'fisd/corp_amt_outstanding.rds'))
df_fisd[, date := as.IDate(date)]
df_fisd[, date_m_id := year(date) * 100L + month(date)]
df_fisd <- df_fisd[date_m_id >= 200001]
cat("Read bond-month level bond type and amt outstanding data:", dim(df_fisd), range(df_fisd$date_m_id))

# WRDS bond returns
df_wbr <- readRDS(paste0(RAWDIR, 'trace/wrds_bondret.rds'))
df_wbr[, date := as.IDate(date)]
df_wbr[, date_m_id := year(date) * 100L + month(date)]
setnames(df_wbr, "date", "date_wbr")
cat("Read bond-month level WBR data:", dim(df_wbr), range(df_wbr$date_m_id))

# merge FISD + WBR
df_bm <- merge(
  df_fisd,
  df_wbr,
  by = c("issue_id", "date_m_id")
)
df_bm[, cusip8 := substr(cusip, 1, 8)]
cat("After merging FISD and WBR, bond-month level data:", dim(df_bm))

# deal with duplicates at cusip8-month: sum amt_outstanding, take "last" for others
setorder(df_bm, cusip8, date_m_id, amt_outstanding)
df_bm <- df_bm[
  ,
  .(
    date           = date[.N],
    issue_id       = issue_id[.N],
    cusip          = cusip[.N],
    maturity       = maturity[.N],
    amt_outstanding= sum(amt_outstanding, na.rm = TRUE),
    cusip8         = cusip8[.N],
    price_eom      = price_eom[.N]
  ),
  by = .(cusip8, date_m_id)
]
cat("After dealing with duplicates at cusip8, bond-month level characteristics:", dim(df_bm), range(df_bm$date_m_id))
rm(df_fisd, df_wbr)


# ================================================================= #
# Merge and clean data at fund-bond-month level ####
# ================================================================= #
# keys for fast joins (no sorting of the big table required)
setDT(crsp_ho); setDT(df_bm); setDT(crsp_flow)
setkey(df_bm, cusip8, date_m_id)
setkey(crsp_flow, crsp_portno, date_m_id)

# filter ttm >= 1
bm <- df_bm[, .(cusip8, date_m_id, date, maturity = as.IDate(maturity), price_eom, amt_outstanding)]
bm[, maturity_id := to_IDate_safe(get("maturity"))]
bm[, date_id     := to_IDate_safe(get("date"))]
bm <- bm[!is.na(maturity_id) & !is.na(date_id)]

bm[, ttm := as.numeric(maturity_id - date_id) / 365.25]
bm <- bm[ttm >= 1]

# filter positive TNA and non-null flow
flow <- crsp_flow[, .(crsp_portno, date_m_id, mtna, mret, flow_dollar, flow)]
flow <- flow[mtna > 0 & !is.na(flow)]

# make key types consistent
crsp_ho[, crsp_portno := as.integer(crsp_portno)]
crsp_ho[, date_m_id   := as.integer(date_m_id)]
crsp_ho[, cusip8      := as.character(cusip8)]

bm[, date_m_id := as.integer(date_m_id)]
bm[, cusip8    := as.character(cusip8)]

flow[, crsp_portno := as.integer(crsp_portno)]
flow[, date_m_id   := as.integer(date_m_id)]

# helper functions
dbg_step <- function(tag, DT) {
  cat("\n==================== ", tag, " ====================\n", sep = "")
  if (is.null(DT)) { cat("DT is NULL\n"); return(invisible()) }
  cat("nrow:", nrow(DT), " ncol:", ncol(DT), "\n")
  cat("cols (first 20): ", paste(head(names(DT), 20), collapse = ", "), "\n", sep = "")
  # lightweight memory print (Linux)
  cat("gc(): "); print(gc()[,1:2])
  invisible()
}

dbg_cols <- function(tag, DT, cols) {
  miss <- setdiff(cols, names(DT))
  if (length(miss)) {
    stop(sprintf("[%s] Missing columns: %s", tag, paste(miss, collapse = ", ")))
  }
  invisible()
}

dbg_class <- function(tag, DT, col) {
  if (!col %in% names(DT)) {
    cat(sprintf("[%s] Column '%s' does not exist\n", tag, col))
    return(invisible())
  }
  cat(sprintf("[%s] class(%s): %s\n", tag, col, paste(class(DT[[col]]), collapse = "/")))
  # show a few values safely
  cat(sprintf("[%s] head(%s): ", tag, col))
  print(head(DT[[col]], 3))
  invisible()
}

dbg_na <- function(tag, DT, cols) {
  cols <- intersect(cols, names(DT))
  if (!length(cols)) return(invisible())
  nas <- sapply(cols, function(cc) sum(is.na(DT[[cc]])))
  cat(sprintf("[%s] NA counts (subset):\n", tag))
  print(nas)
  invisible()
}

to_IDate_safe <- function(x) {
  # list column -> atomic
  if (is.list(x)) x <- unlist(x, use.names = FALSE)
  
  if (inherits(x, "IDate")) return(x)
  if (inherits(x, "Date"))  return(as.IDate(x))
  
  if (is.factor(x)) x <- as.character(x)
  
  if (is.numeric(x)) {
    # treat as YYYYMMDD if it looks like it
    ok <- !is.na(x) & x > 19000101 & x < 21001231
    out <- rep(as.IDate(NA), length(x))
    if (any(ok)) {
      s <- sprintf("%08d", as.integer(x[ok]))
      out[ok] <- as.IDate(paste0(substr(s,1,4), "-", substr(s,5,6), "-", substr(s,7,8)))
    }
    return(out)
  }
  
  if (is.character(x)) {
    # as.IDate() returns NA for unparseable entries (no error)
    return(as.IDate(x))
  }
  
  # fallback: all NA instead of error
  rep(as.IDate(NA), length(x))
}

# Process month-by-month
months <- sort(unique(crsp_ho$date_m_id))
months <- months[months >= 200207]
out_list <- vector("list", length(months))

for (k in seq_along(months)) {
  m <- months[k]
  cat(sprintf("\n\n[%d/%d] Processing date_m_id = %s\n", k, length(months), m))
  
  ## Prevent parallel TMP allocations during by=
  setDTthreads(1)
  
  ## ---- SUBSET
  DT <- copy(crsp_ho[date_m_id == m])
  dbg_step("SUBSET", DT)
  
  if (nrow(DT) == 0L) {
    cat("[SUBSET] no rows for this month, skipping\n")
    out_list[[k]] <- data.table()
    next
  }
  
  ## ---- (A) aggregate holdings with within-month chunking
  K <- 50L  # increase if a month is still too big; 50-200 typical
  DT[, chunk := crsp_portno %% K]
  
  parts <- vector("list", K)
  for (j in 0:(K-1L)) {
    DTj <- DT[chunk == j]
    if (!nrow(DTj)) next
    
    parts[[j+1L]] <- DTj[
      ,
      .(
        report_dt  = max(report_dt, na.rm = TRUE),
        eff_dt     = max(eff_dt,    na.rm = TRUE),
        nbr_shares = sum(nbr_shares, na.rm = TRUE),
        market_val = sum(market_val, na.rm = TRUE)
      ),
      by = .(crsp_portno, cusip8, date_m_id)
    ]
  }
  DT <- rbindlist(parts, use.names = TRUE)
  dbg_step("AFTER AGG", DT)
  
  ## ---- (B) add bond characteristics
  DT <- bm[DT, on = .(cusip8, date_m_id), nomatch = 0L]
  dbg_step("AFTER JOIN bm", DT)

  # match diagnostics (choose a column likely non-NA if matched)
  bm_match <- if ("amt_outstanding" %in% names(DT)) mean(!is.na(DT$amt_outstanding)) else 0
  cat(sprintf("[JOIN bm] match rate: %.4f\n", bm_match))
  dbg_na("AFTER JOIN bm", DT, c("date", "maturity", "price_eom", "amt_outstanding"))
  
  ## ---- (C) add fund flows
  DT <- flow[DT, on = .(crsp_portno, date_m_id), nomatch = 0L]
  dbg_step("AFTER JOIN flow", DT)
  
  flow_match <- if ("flow" %in% names(DT)) mean(!is.na(DT$flow)) else 0
  cat(sprintf("[JOIN flow] match rate (flow non-NA): %.4f\n", flow_match))
  dbg_na("AFTER JOIN flow", DT, c("mtna", "mret", "flow", "flow_dollar"))
  
  ## ---- (D) compute quantities
  # market value in thousands
  DT[, mkt_val := market_val / 1000]
  DT[(mkt_val == 0 | is.na(mkt_val)) & !is.na(price_eom),
     mkt_val := (nbr_shares / 1000) * (price_eom / 100)]
  DT[(mkt_val == 0 | is.na(mkt_val)) &  is.na(price_eom),
     mkt_val := (nbr_shares / 1000) * 1]
  
  # paramt in thousands
  DT[, paramt := mkt_val / (price_eom / 100)]
  DT[is.na(price_eom), paramt := mkt_val]
  
  # mkt_cap in thousands
  DT[, mkt_cap := amt_outstanding * (price_eom / 100)]
  DT[is.na(price_eom), mkt_cap := amt_outstanding]
  
  # AUM in thousands (mtna in millions)
  DT[, aum := mtna * 1000]
  DT[, w := mkt_val / aum]
  
  DT[, totparamt := sum(paramt, na.rm = TRUE), by = .(crsp_portno, date_m_id)]
  DT[, share_corporate := totparamt / aum]
  
  # flow_dollar scaling (if your flow_dollar is in millions)
  if ("flow_dollar" %in% names(DT)) {
    setnames(DT, "flow_dollar", "flow_dollar_mm")
    DT[, flow_dollar := flow_dollar_mm * 1000]
  } else {
    DT[, flow_dollar := NA_real_]
  }
  
  dbg_step("AFTER COMPUTATIONS", DT)
  
  ## ---- (E) select columns
  base_m <- tryCatch({
    keep <- c("crsp_portno", "cusip8", "date_m_id", "report_dt",
              "paramt", "mkt_val", "w", "mkt_cap", "amt_outstanding", "price_eom",
              "totparamt", "aum", "share_corporate", "mret", "flow_dollar", "flow")
    miss <- setdiff(keep, names(DT))
    if (length(miss)) stop(sprintf("Missing columns at select: %s", paste(miss, collapse=", ")))
    DT[, ..keep]
  }, error = function(e) stop(sprintf("[SELECT] %s", e$message)))
  
  dbg_step("FINAL base_m", base_m)
  
  out_list[[k]] <- base_m
  rm(DT, base_m); gc()
}

# bind all months together
base <- rbindlist(out_list, use.names = TRUE, fill = TRUE)

# Optional: reconstruct "YYYY-MM" date_m string if you want
#base[, date_m := sprintf("%04d-%02d", date_m_id %/% 100, date_m_id %% 100)]


# ================================================================= #
# Save down data ####
# ================================================================= #
# sanity check: no duplicates at fund-bond-month
cat("No duplicates fundid-cusip8-date_m_id: ",
    nrow(base) == uniqueN(base, by = c("crsp_portno","cusip8","date_m_id")))
range(base$date_m_id)

saveRDS(base, file = paste0(PROCDIR, 'crsp/mf_corp_holdings_m.rds'))





