# ================================================================= #
# Compute MF retail shares ####
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
source('utils/db_utilities.R')

# Create database connections
db_crsp <- db_con("crsp")
connectArgs <- bcputility::makeConnectArgs(server = Sys.getenv("mssql_server"), database = "crsp")

wrds <- dbConnect(Postgres(),
                  host = 'wrds-pgdata.wharton.upenn.edu',
                  port = 9737,
                  user = 'rqchang',
                  password = 'Crq-19990711',
                  sslmode ='require',
                  dbname ='wrds')


# ================================================================= #
# Read data from WRDS ####
# ================================================================= #
# read fund_hdr at crsp_fundno level
hdr <- dbGetQuery(wrds, "SELECT * FROM crsp.fund_hdr") |> as.data.table()

# read monthly_tna_ret_nav at crsp_fundno-month level
fundm <- dbGetQuery(wrds, "SELECT * FROM crsp.monthly_tna_ret_nav WHERE caldt >= '01/01/2002'") |> as.data.table()

# crsp_portno - crsp_fundno link, 1:m
link <- db_read(db_crsp, 'crsp_portno_link')


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
vdata <- data[!is.na(is_retail_fund), .(crsp_fundno, crsp_portno, caldt, date_m, mtna, is_retail_fund)]

# compute share_tna_retail and share_tna_inst, sum up to 1 per fund-month
vdata[is.na(mtna), mtna := 0]
vdata[, sum_tna := sum(mtna), by = .(crsp_portno, date_m)]
vdata_im <- vdata[sum_tna != 0, 
                  .(caldt = last(caldt),
                    sum_tna = last(sum_tna),
                    mtna = sum(mtna)),
                  by = .(crsp_portno, date_m, is_retail_fund)]
vdata_im[, share_tna := mtna / sum_tna]

# dcast long to wide
share_im <- dcast(vdata_im, crsp_portno + date_m ~ is_retail_fund, value.var = "share_tna")
setnames(share_im, c("0", "1"), c("share_tna_retail", "share_tna_inst"))

# check share_tna_sum
share_im[is.na(share_tna_retail), share_tna_retail := 0]
share_im[is.na(share_tna_inst), share_tna_inst := 0]
share_im[, share_tna_sum := share_tna_retail + share_tna_inst]
summary(share_im$share_tna_sum)
summary(share_im$share_tna_retail)

# plot distribution
ggplot(share_im, aes(x = share_tna_retail)) + 
  geom_histogram(fill = "seagreen")


# ================================================================= #
# Save data ####
# ================================================================= #
if(any(duplicated(share_im[, .(crsp_portno, date_m)]))){
  stop('CRSP data primary key violated.')
}

saveRDS(share_im, file = paste0(PROCDIR, 'crsp/mf_share_retail.rds'))
#write.csv(vflow_im, paste0(PROCDIR, 'crsp/crsp_mf_flows_m_all.csv'), row.names = FALSE)



