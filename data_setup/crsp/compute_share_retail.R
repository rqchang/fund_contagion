# ================================================================= #
# Compute MF retail shares ####
# ================================================================= #
# Description:
# ------------
#     This script computes the fund-level identifier for retail / institutional MFs,
#     based on two criteria:
#     1. retail_fund indicator in CRSP data
#     2. share of TNA for retail/inst at the crsp_portno level
#     the identifiers are aggregated from crsp_fundno to crsp_portno level
#
# Input(s):
# ---------
#     Dropbox data/raw/crsp:
#     crsp_mf_hdr
#     crsp_mf_fundm
#     crsp_portno_link
#
# Output(s):
# ----------
#     Dropbox data/processed/crsp:
#     1. Fund-month level retail identifiers: data/processed/crsp/mf_retail_fm.rds
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
library(zoo)
library(ggplot2)

# Source helper scripts
source('utils/setPaths.R')


# ================================================================= #
# Read data from WRDS ####
# ================================================================= #
# read fund_hdr at crsp_fundno level
hdr <- readRDS(paste0(RAWDIR, "crsp/crsp_mf_hdr.rds")) |> as.data.table()

# read monthly_tna_ret_nav at crsp_fundno-month level
fundm <- readRDS(paste0(RAWDIR, "crsp/crsp_mf_fundm.rds")) |> as.data.table()

# crsp_portno - crsp_fundno link, 1:m
link <- readRDS(paste0(RAWDIR, "crsp/crsp_portno_link.rds")) |> as.data.table()


# ================================================================= #
# Clean data ####
# ================================================================= #
# exclude NA crsp_portno: relationship between crsp_portno:crsp_fundno is 1:m
vhdr <- hdr[!is.na(crsp_portno)]

# fill NA in retail_fund == 0
vhdr[, is_retail_fund := 0]
vhdr[retail_fund == "Y", is_retail_fund := 1]
table(vhdr$is_retail_fund)

# add crsp_portno to fundm
fundm[, date_link := caldt]
fundm_portno <- fundm[link[, .(crsp_fundno, crsp_portno, fund_name, begdt, enddt)],
                      on = .(crsp_fundno = crsp_fundno, date_link >= begdt, date_link <= enddt),
                      nomatch = 0]

# add retail_fund
data <- merge(fundm_portno, vhdr[, .(crsp_fundno, is_retail_fund)], by = c('crsp_fundno'), all.x = TRUE)
data[, date_m := format(as.Date(caldt), '%Y-%m')]
vdata <- data[!is.na(is_retail_fund), 
              .(crsp_fundno, crsp_portno, caldt, date_m, mtna, 
                mret, mnav, is_retail_fund, fund_name)]


# ================================================================= #
# Create identifiers ####
# ================================================================= #
# --------------------------------------------- #
# Method 1: retail_fund indicator at crsp_fundno level
# --------------------------------------------- #
# use max(retail_fund) as the indicator at crsp_portno level
# one issue is that suppose there is one crsp_fundo has retail_fund == 1,
# even though its share_tna is very small at the crsp_portno level,
# we still identify it as a retail MF
retail_fund <- vdata[, .(retail_fund = max(is_retail_fund)), by = .(crsp_portno, date_m)]

# --------------------------------------------- #
# Method 2: compute share_tna at crsp_portno level
# --------------------------------------------- #
# compute share_tna based on fund type (retail / inst), sum up to 1 per fund-month
vdata[is.na(mtna), mtna := 0]
vdata[, sum_tna := sum(mtna), by = .(crsp_portno, date_m)]
vdata_icm <- vdata[sum_tna != 0, 
                   .(caldt = last(caldt),
                     sum_tna = last(sum_tna),
                     mtna = sum(mtna)),
                   by = .(crsp_portno, date_m, is_retail_fund)]
vdata_icm[, share_tna := mtna / sum_tna]

# dcast long to wide
share_im <- dcast(vdata_icm, crsp_portno + date_m ~ is_retail_fund, value.var = "share_tna")
setnames(share_im, c("0", "1"), c("share_tna_inst", "share_tna_retail"))

# check share_tna_sum
share_im[is.na(share_tna_retail), share_tna_retail := 0]
share_im[is.na(share_tna_inst), share_tna_inst := 0]
share_im[, share_tna_sum := share_tna_retail + share_tna_inst]
summary(share_im$share_tna_sum)
summary(share_im$share_tna_retail)

# plot distribution
ggplot(share_im, aes(x = share_tna_retail)) + 
  geom_histogram(fill = "seagreen")

# --------------------------------------------- #
# merge two identifiers
# --------------------------------------------- #
id <- merge(retail_fund, share_im, by = c('crsp_portno', 'date_m'), all.x = TRUE)
summary(id$share_tna_sum)
id[, share_tna_sum := NULL]


# ================================================================= #
# Save data ####
# ================================================================= #
if(any(duplicated(id[, .(crsp_portno, date_m)]))){
  stop('CRSP data primary key violated.')
}

saveRDS(id, file = paste0(PROCDIR, 'crsp/mf_retail_fm.rds'))
#write.csv(id, paste0(PROCDIR, 'crsp/mf_retail_fm.csv'), row.names = FALSE)



