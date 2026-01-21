# ================================================================= #
# Compute MF flows ####
# ================================================================= #
# Description:
# ------------
#     This script computes CRSP mutual fund flows, including
#     1. percentage flows: (aum - (1+ret)*aum_lag) / aum_lag
#     2. orthogonalized flows: regress flows on fund returns and fund FE
#     At crsp_portno-month and crsp_portno-quarter level
#     Conditional on fund's weighted average time-to-maturity >= 1yr
#
# Input(s):
# ---------
#     crsp.crsp_mf_mtna
#     crsp.rsp_mf_mret
#     crsp.crsp_mf_sum
#     crsp.crsp_portno_link
#
# Output(s):
# ----------
#     1. Fund-month level flows: data/processed/crsp/crsp_mf_flows_m_all.csv
#     2. Fund-quarter level flows: data/processed/crsp/crsp_mf_flows_q_all.csv
#
# Date:
# ----------
#     2026-01-21
#     update: 
#
# Author(s):
# ----------
#     Ruiquan Chang, chang.2590@osu.edu
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
library(lmtest)
library(haven)
library(zoo)
library(lfe)
library(DBI)
library(RPostgres)

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

print("Started compute_mf_flows.R")


# ================================================================= #
# Read data ####
# ================================================================= #
# fund-month aum
fund_tna <- db_read(db_crsp, 'crsp_mf_mtna')
fund_tna <- fund_tna[caldt <= '2023-12-31']

# fund-month returns
fund_ret <- db_read(db_crsp, 'crsp_mf_mret')
fund_ret <- fund_ret[caldt <= '2023-12-31']

# fund-quarter maturity
fund_sum <- db_read(db_crsp, 'crsp_mf_sum')
fund_sum <- fund_sum[caldt <= '2023-12-31']

# portno-fundno link
link <- db_read(db_crsp, 'crsp_portno_link')

# read fund_hdr at crsp_fundno level
hdr <- dbGetQuery(wrds, "SELECT * FROM crsp.fund_hdr") |> as.data.table()

# time series data
vix_m <- fread(paste0(RAWDIR, 'websource/vix_m.csv'))
mkt_m <- fread(paste0(RAWDIR, 'websource/mkt_m.csv'))


# ================================================================= #
# Data cleaning ####
# ================================================================= #
#-------------------------------------------------#
# 1. Merge fund_tna and fund_ret
#-------------------------------------------------#
# create month, quarter columns
fund_tna[, date_m := format(as.Date(caldt), '%Y-%m')]
fund_ret[, date_m := format(as.Date(caldt), '%Y-%m')]

# get mtna_lag
fund_tna[, date_m_merge := format(as.Date(as.yearmon(date_m) - 1/12), '%Y-%m')]
cfund_tna <- copy(fund_tna)

vfund_tna <- merge(fund_tna,
                   cfund_tna[, .(crsp_fundno, date_m_merge = date_m, mtna_lag = mtna)],
                   by = c('crsp_fundno', 'date_m_merge'),
                   all.x = TRUE)

# merge fund tna with fund returns
mdata <- merge(vfund_tna[, .(crsp_fundno, date_m, caldt, mtna, mtna_lag)],
               fund_ret[, .(crsp_fundno, date_m, mret)],
               by = c('crsp_fundno', 'date_m'),
               all.x = TRUE)

# merge with VIX and mkt
mdata <- merge(mdata, vix_m[, .(date_m, vix)], by = c('date_m'), all.x = TRUE)
mdata <- merge(mdata, mkt_m[, .(date_m, mkt)], by = c('date_m'), all.x = TRUE)

#-------------------------------------------------#
# 2. Add maturity
#-------------------------------------------------#
mdata[, date_q := as.yearqtr(caldt)]
fund_sum[, date_q := as.yearqtr(caldt)]

mdata <- merge(mdata,
               fund_sum[, .(crsp_fundno, date_q, tna_latest, maturity, maturity_dt)],
               by = c('crsp_fundno', 'date_q'),
               all.x = TRUE)

# conditional on time to maturity >= 1 year
vdata <- mdata[!is.na(crsp_fundno) & (is.na(maturity) | maturity >= 1), ]
#vdata <- mdata[!is.na(crsp_fundno) & maturity >= 1, ]

print(sprintf('Drop %.2f%% observations due to weighted average maturity < 1 year.', 
              (1-nrow(vdata)/nrow(mdata))*100))  # 11.30%
vdata <- vdata[order(crsp_fundno, date_m)]

#-------------------------------------------------#
# 3. Compute share retail
#-------------------------------------------------#
# link crsp_portno
vdata[, date_link := caldt]
vdata_portno <- vdata[link[, .(crsp_fundno, crsp_portno, fund_name, begdt, enddt)],
                      on = .(crsp_fundno = crsp_fundno, date_link >= begdt, date_link <= enddt),
                      nomatch = 0]

# check relationship between portno:fundno - 1:m
#vdata_portno[, num_fundno := uniqueN(crsp_fundno), by = .(crsp_portno, date_m)]
#vdata_portno[, num_portno := uniqueN(crsp_portno), by = .(crsp_fundno, date_m)]
#unique(vdata_portno$num_fundno)
#unique(vdata_portno$num_portno): 1

# add retail_fund
vdata_portno <- merge(vdata_portno, vhdr[, .(crsp_fundno, is_retail_fund)], by = c('crsp_fundno'), all.x = TRUE)
vdata_portno[is.na(is_retail_fund), is_retail_fund := 0]
vdata_portno[is.na(mtna), mtna := 0]
vdata_portno[, sum_tna := sum(mtna), by = .(crsp_portno, date_m)]

share_im <- vdata_portno[sum_tna != 0, 
                         .(caldt = last(caldt),
                           sum_tna = last(sum_tna),
                           mtna = sum(mtna)),
                         by = .(crsp_portno, date_m, is_retail_fund)]
share_im[, share_tna := mtna / sum_tna]

# dcast long to wide
share_im <- dcast(share_im, crsp_portno + date_m ~ is_retail_fund, value.var = "share_tna")
setnames(share_im, c("0", "1"), c("share_tna_retail", "share_tna_inst"))

# check share_tna_sum
share_im[is.na(share_tna_retail), share_tna_retail := 0]
share_im[is.na(share_tna_inst), share_tna_inst := 0]
share_im[, share_tna_sum := share_tna_retail + share_tna_inst]
summary(share_im$share_tna_sum)
summary(share_im$share_tna_retail)
hist(share_im$share_tna_retail)

# add back to vdata_portno
vdata_portno <- merge(vdata_portno, share_im[, .(crsp_portno, date_m, share_tna_retail)], 
                      by = c('crsp_portno', 'date_m'), all.x = TRUE)

#-------------------------------------------------#
# 4. Aggregate to fund-month level (portno-month)
#-------------------------------------------------#
# conditional on share_tna_retail >= 0.5, or is_retail_fund == 1
# get fund-month (portno-month) level data
vdata_im <- vdata_portno[is_retail_fund == 1, 
                         .(date_q = last(date_q), 
                           caldt = last(caldt),
                           mtna = sum(mtna),
                           mtna_lag = sum(mtna_lag, na.rm = TRUE),
                           mret = weighted.mean(mret, mtna, na.rm = TRUE),
                           vix = last(vix),
                           mkt = last(mkt),
                           share_retail = last(share_tna_retail)),
                         by = .(crsp_portno, date_m)]

# get mret_lag
vdata_im[, date_m_last := format(as.Date(as.yearmon(date_m) - 1/12), '%Y-%m')]
cdata_im <- copy(vdata_im)

vdata_im <- merge(vdata_im,
                  cdata_im[, .(crsp_portno, date_m_last = date_m, mret_lag = mret)],
                  by = c('crsp_portno', 'date_m_last'),
                  all.x = TRUE)

# compute fund-month flows
flow_im <- copy(vdata_im)
flow_im[, flow_unscaled := mtna - (1 + mret) * mtna_lag]
flow_im[mtna_lag != 0, flow := flow_unscaled / mtna_lag]

# keep non-null flows
vflow_im <- flow_im[!is.na(flow_unscaled)]


# ================================================================= #
# Write data ####
# ================================================================= #
#-------------------------------------------------#
# fund-monthly
#-------------------------------------------------#
if(any(duplicated(vflow_im[, .(crsp_portno, date_m)]))){
  stop('CRSP data primary key violated.')
}

saveRDS(vflow_im, file = paste0(PROCDIR, 'crsp/crsp_mf_flows_m_retail2.rds'))
write.csv(vflow_im, paste0(PROCDIR, 'crsp/crsp_mf_flows_m_retail2.csv'), row.names = FALSE)




