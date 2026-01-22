# ================================================================= #
# Compute MF flows ####
# ================================================================= #
# Description:
# ------------
#     This script computes CRSP mutual fund net flows, as
#     percentage flows: (aum - (1+ret)*aum_lag) / aum_lag
#     (conditional on fund's weighted average time-to-maturity >= 1yr)
#
# Input(s):
# ---------
#     Dropbox:
#     data/raw/crsp/crsp_mf_mtna
#     data/raw/crsp/crsp_mf_mret
#     data/raw/crsp/crsp_mf_sum
#     data/raw/crsp/crsp_portno_link
#     data/processed/crsp/mf_retail_fm
#     
#
# Output(s):
# ----------
#     1. Fund-month level flows: data/processed/crsp/crsp_mf_flows_m.rds
#
# Date:
# ----------
#     2026-01-22
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
library(RPostgres)

# Source helper scripts
source('utils/setPaths.R')


# ================================================================= #
# Read data ####
# ================================================================= #
# fund-month aum
fund_tna <- readRDS(paste0(RAWDIR, "crsp/crsp_mf_mtna.rds")) |> as.data.table()

# fund-month returns
fund_ret <- readRDS(paste0(RAWDIR, "crsp/crsp_mf_mret.rds")) |> as.data.table()

# fund-quarter maturity
fund_sum <- readRDS(paste0(RAWDIR, "crsp/crsp_mf_sum.rds")) |> as.data.table()

# crsp_portno-month level retail identifiers
retail <- readRDS(paste0(PROCDIR, "crsp/mf_retail_fm.rds")) |> as.data.table()

# crsp_portno - crsp_fundno link, 1:m
link <- readRDS(paste0(RAWDIR, "crsp/crsp_portno_link.rds")) |> as.data.table()


# ================================================================= #
# Data cleaning ####
# ================================================================= #
# ----------------------------------------------- #
# 1. Merge fund_tna and fund_ret
# ----------------------------------------------- #
# create month, quarter columns
fund_tna[, date_m := format(as.Date(caldt), '%Y-%m')]
fund_ret[, date_m := format(as.Date(caldt), '%Y-%m')]

# merge fund tna with fund returns
mdata <- merge(fund_tna[, .(crsp_fundno, date_m, caldt, mtna)],
               fund_ret[, .(crsp_fundno, date_m, mret)],
               by = c('crsp_fundno', 'date_m'),
               all.x = TRUE)

# ----------------------------------------------- #
# 2. Add maturity
# ----------------------------------------------- #
mdata[, date_q := as.yearqtr(caldt)]
fund_sum[, date_q := as.yearqtr(caldt)]

mdata <- merge(mdata,
               fund_sum[, .(crsp_fundno, date_q, tna_latest, maturity, maturity_dt)],
               by = c('crsp_fundno', 'date_q'),
               all.x = TRUE)

# conditional on time to maturity >= 1 year
#vdata <- mdata[!is.na(crsp_fundno) & (is.na(maturity) | maturity >= 1), ]
#vdata <- mdata[!is.na(crsp_fundno) & maturity >= 1, ]

#print(sprintf('Drop %.2f%% observations due to weighted average maturity < 1 year.', 
#              (1-nrow(vdata)/nrow(mdata))*100))  # 11.30%
#vdata <- vdata[order(crsp_fundno, date_m)]

# ----------------------------------------------- #
# 3. Aggregate to fund-month level (crsp_portno)
# ----------------------------------------------- #
# add crsp_portno to fundm
mdata[, date_link := caldt]
vdata <- mdata[link[, .(crsp_fundno, crsp_portno, fund_name, begdt, enddt)],
               on = .(crsp_fundno = crsp_fundno, date_link >= begdt, date_link <= enddt),
               nomatch = 0]

# get fund-month (portno-month) level data
data_im <- vdata[!is.na(mtna), 
                 .(date_q = last(date_q), 
                   caldt = last(caldt),
                   mtna = sum(mtna),
                   mret = weighted.mean(mret, mtna, na.rm = TRUE)),
                 by = .(crsp_portno, date_m)]

# get mret_lag and mtna_lag
data_im[, date_m_last := format(as.Date(as.yearmon(date_m) - 1/12), '%Y-%m')]
cdata_im <- copy(data_im)

vdata_im <- merge(data_im,
                  cdata_im[, .(crsp_portno, date_m_last = date_m, mtna_lag = mtna, mret_lag = mret)],
                  by = c('crsp_portno', 'date_m_last'),
                  all.x = TRUE)

# compute fund-month flows
flow_im <- copy(vdata_im)
flow_im[, flow_dollar := mtna - (1 + mret) * mtna_lag]
flow_im[mtna_lag != 0, flow := flow_dollar / mtna_lag]

# keep non-null flows
mflow_im <- flow_im[!is.na(flow_dollar)]
hist(mflow_im$flow_dollar)
hist(mflow_im$flow)

# ----------------------------------------------- #
# 3. add retail identifiers
# ----------------------------------------------- #
vflow_im <- merge(mflow_im, retail, by = c('crsp_portno', 'date_m'), all.x = TRUE)
table(vflow_im$retail_fund)
hist(vflow_im$share_tna_retail)
hist(vflow_im$share_tna_inst)


# ================================================================= #
# Write data ####
# ================================================================= #
# ----------------------------------------------- #
# fund-monthly
# ----------------------------------------------- #
if(any(duplicated(vflow_im[, .(crsp_portno, date_m)]))){
  stop('CRSP data primary key violated.')
}

saveRDS(vflow_im, file = paste0(PROCDIR, 'crsp/crsp_mf_flows_m.rds'))
#write.csv(vflow_im, paste0(PROCDIR, 'crsp/crsp_mf_flows_m.csv'), row.names = FALSE)




