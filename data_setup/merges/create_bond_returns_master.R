# ================================================================= #
# create_bond_returns_master.R ####
# ================================================================= #
# Description:
# ------------
#   Create monthly bond returns table 
#
# Input(s):
# ------------
# Fisd 
# --------
#   1. merges.corp_amt_c_cdsus_merged
#   2. fisd.issue
#
# Bond Returns
# --------
#   1. trace.bondret
#
# Markit CDS
# --------
#   1. markit.cds_us
#
# Treasury
# --------
#   1. crsp.treasury_fixed_rates_ycurve
# 
# Compustat
# --------
#   1. merges.compustat_quarterly
#
# Output(s) :
# ------------
#   SQL table merges.bond_returns_master at sts-liramota.mit.edu
#
# Date:
# ------------
#   2023-05-31
#   update: 2026-01-22
#
# Author(s):
# ------------
#   Lira Mota
#   Ruiquan Chang, chang.2590@osu.edu
#
# Additional note(s):
# ------------
# Rate Interporlation Reference:
# http://web.math.ku.dk/~rolf/HaganWest
#
# ================================================================= #


# ================================================================= #
# Environment ####
# ================================================================= #
# Clear workspace
rm(list = ls())

# Import libraries
library(ggplot2)
library(stringdist)
library(stringr)
library(zoo)

# Source helper scripts
source('utils/setPaths.R')
sprintf('Started create_bond_Returns_master.R')


# ================================================================= #
# Parameters ####
# ================================================================= #
byear <- 2000
eyear <- 2025


# ================================================================= #
# Functions ####
# ================================================================= #
# Rates Interpolation 
rate_interpolation <- function(t, ut, lt, ur, lr, type = 'linear') {
  
  # t : time to be matched 
  # ut: upper maturity
  # lt: lower maturity
  # ur: upper rate
  # lr: lower rate
  
  if(type == 'linear'){
    
    # linear interpolation
    rate_diff <- ur - lr
    time_diff <- ut - lt
    time <- t - lt
    inter_rate <- lr + (rate_diff/time_diff)*time
    return(inter_rate)
    
  }else if(type == 'l_cte_fwd'){
    # Locally constant forward rate 
    d_a <- (ut - t)/(ut - lt)
    d_b <- (t - lt)/(ut - lt)
    inter_rate <- (d_a * lr * lt + d_b * ur * ut)/t
    return(inter_rate)
    
  }
}

rate_extention <- function(t, ut, lt, ur, lr, flat = FALSE){
  if(!flat){
    # locally constant forward rate estimated from lt to ut
    f <-  (ut * ur - lt * lr) / (ut - lt)
    
    # Extent to t
    rate <- (ut/t * ur) + (1 - ut/t) * f}
  else{
    rate <-  ur
  }
  
  return(rate)
  
}


# ================================================================= #
# Load Data ####
# ================================================================= #
# Load corporate bond amount 0utstanding 
caout <- readRDS(paste0(PROCDIR, 'fisd/corp_amt_outstanding.rds'))

# Load issue information to get issue_name
issue <- readRDS(paste0(RAWDIR, 'fisd/fisd_issue.rds'))
issue <- issue[, issue_id, issue_name]

# Load WRDS Bond Returns 
ret <- readRDS(paste0(RAWDIR, 'trace/wrds_bondret.rds'))
print(sprintf('We exclude %.2f%% of bond returns data due to missing yield.',
              ret[, mean(is.na(yield))]*100))  # 0.89%
ret <- ret[!is.na(yield)]

print(sprintf('We exclude %.2f%% of bond returns data due to missing duration.',
              ret[, mean(is.na(duration))]*100))  # 0.53%
ret <- ret[!is.na(duration)]
ret[, range(date)]  # "2002-07-31" "2024-08-31"

# Primary key check
nac <- any(is.na(ret$issue_id)) | any(is.na(ret$date))
dupc <- any(duplicated(ret, by = c('issue_id', 'date')))

if(nac | dupc){
  stop('Bond returns primary key integrity violated.')
}
rm(nac, dupc)

# ----------------------------------------------------------------- #
# Load CDS
cds <- db_read(markit, 'cds_us')
cds[, range(date)]
cds[, table(tier)]
cds[, table(currency)]

# ----------------------------------------------------------------- #
# Load Treasury
ycurve <- readRDS(paste0(RAWDIR, "crsp/treasury_rates/ycurve.rds"))
#ycurve[is.na(`term_type_30`)][order(mdate)]
ycurve <- ycurve[!is.na(`term_type_30`)]
ycurve[, range(mdate)]  # 194111 202412

# ----------------------------------------------------------------- #
# Load Compustat
compq <- readRDS(paste0(PROCDIR, "compustat/compustat_quarterly.rds"))
compq[, sic1 := substr(sicf, 1, 1)]
compq[, sic2 := substr(sicf, 1, 2)]
compq <- compq[, .(gvkey, cyqtr, datadate, sicf, sic1, sic2, atq, ndiq, icr, 
                   tdebtq, dlttq, dlcq, dltisq, dltisy, dlcchq, dlcchy, xintq, oiadpq)]


# ================================================================= #
# Merge bond returns and amount oustanding ####
# ================================================================= #
# Create mdate
ret[, mdate := year(date)*100 + month(date)]
caout[, mdate := year(date)*100 + month(date)]
caout <- caout[order(issue_id, date)]

## TODO: check why there is missing amt_out.
# Left join to maintain all returns info!
mdata <- merge(ret[, .(date, mdate, issue_id, isin, company_symbol, duration, 
                       security_level, conv, offering_amt, rating_class, r_sp, r_mr, r_fr, rating_cat, 
                       tmt, t_volume, ret_eom, ret_ldm, ret_l5m, yield = yield * 100, t_yld_pt, 
                       t_spread, price_eom, price_l5m, price_ldm)],
               caout[, .(issue_id, mdate, issuer_id, cusip, issuer_cusip, cusip_name, gvkey, hname, conm,  
                         redcode, red_name, amt_outstanding)],
               by=c('mdate', 'issue_id'),
               all.x = TRUE)
print('We have just merged bond returns data and amount outstanding.')

print(sprintf('We will later exclude %.2f%% of bond returns data because of failure in matching with amount_outstanding.',
              mdata[,mean(is.na(amt_outstanding))]*100))

# Add issue name 
mdata <- merge(mdata, 
               issue, 
               by = 'issue_id', all.x = TRUE)




# Check merge ---
test <- mdata[year(date)>=byear & year(date)<=eyear & amt_outstanding>0]
test[is.na(gvkey), unique(cusip_name)]
test[,mean(is.na(gvkey))]

test[is.na(gvkey), mstatus:='NotMerged']
test[!is.na(gvkey), mstatus:='Merged']
test <- test[, .(out=sum(amt_outstanding)/1e6), by=.(date, mstatus)]

plot_corp_merge <- ggplot(test,
                          aes(x=date, y=out))+ 
  geom_area(aes(colour = mstatus, fill=factor(mstatus)), position = 'stack', alpha = 0.7) + 
  scale_fill_brewer(name="", palette="Set2", labels=c("Merged", "Not Merged"))+
  scale_colour_brewer(name="", palette="Set2", labels=c("Merged", "Not Merged"))+
  scale_x_date(date_breaks = "2 year", date_labels = "%Y")+
  #scale_y_continuous(breaks = seq(-0.2, 0.4, 0.02)*100)+
  labs(x = "", y = "Amount Outstanding (Face Value Bi $)")+
  annotate("rect", fill = "gray", alpha = 0.4, xmin = as.Date("2007-12-31"), xmax = as.Date("2009-06-30"), ymin = -Inf, ymax = Inf)+
  theme_bw()+ theme(legend.position="bottom")

plot_corp_merge

test <- dcast.data.table(test, date~mstatus, value.var = 'out')
print(sprintf('On average we are able to merge %.2f%% of amount out of bonds in WRDS bond returns.',
              test[,mean(Merged/(Merged+NotMerged))]*100))
rm(caout, test)

# ================================================================= #
# Compute Yield Curve Interpolation ####
# ================================================================= #
### Step1: extend key rates
key_yc_durations <- c(0, 1/12, 3/12, 1, 2, 5, 7, 10, 20, 30, 50)

# I am not computing the yc for values lower than 1/12
# extending so I always have points to interpolate
ycurve[,`term_type_0`:=`term_type_008`] 
ycurve[,`term_type_50`:=(`term_type_30`*30-`term_type_20`*20)/10 *(1-30/50)+`term_type_30`*30/50]

keytab_yc_rates <- melt(ycurve, id.vars = "mdate")
setnames(keytab_yc_rates, "value", "rate")

# If the variable starts with "term_type_0"(i.e."term_type_008", "term_type_025"),extract the numeric part and multiplies it by 0.01
# for other values in variable, directly extract numeric part
keytab_yc_rates[, duration := fifelse(startsWith(as.character(variable), "term_type_0"), as.numeric(str_extract(as.character(variable), "\\d+")) * 0.01,
                                      as.numeric(str_extract(variable, "\\d+")))]
keytab_yc_rates[abs(duration-0.083)<0.01, duration:=1/12] # problem with float  

# it's a really bad idea to use (fractional) numbers as names. 
keytab_yc_rates[,variable:=NULL]
ggplot(keytab_yc_rates, aes(x = as.yearmon(as.character(mdate), format = '%Y%m'), 
                            y = rate, 
                            colour = as.factor(duration)))+geom_line()

# --------------------------------------------------------------- #
### Step2: Interpolate
#rolling join
interpolated_rates <- mdata[,.(mdate, issue_id, duration)]

interpolated_rates <- keytab_yc_rates[,.(mdate, duration, lower_duration_yc = duration, lower_yield = rate)][
  interpolated_rates, 
  on = .(mdate = mdate,  duration = duration), 
  roll = Inf]

interpolated_rates <- keytab_yc_rates[,.(mdate, duration, upper_duration_yc = duration, upper_yield = rate)][
  interpolated_rates, 
  on = .(mdate = mdate,  duration = duration), 
  roll = -Inf]

# calculate us yield
interpolated_rates[lower_duration_yc != upper_duration_yc,
                   yc_yield := rate_interpolation(duration, 
                                                  lower_duration_yc,
                                                  upper_duration_yc, 
                                                  lower_yield, 
                                                  upper_yield,
                                                  type = 'l_cte_fwd')]

interpolated_rates[lower_duration_yc == upper_duration_yc, yc_yield := lower_yield]

# merge to mdata
mdata <- merge(mdata,
               interpolated_rates[,.(mdate, issue_id, yc_yield)], 
               by = c("mdate", "issue_id"), 
               all.x = TRUE)

rm(interpolated_rates)


# check
temp <- mdata[, .(yc_yield = mean(yc_yield)), by = .(mdate)]
temp[,mdate:=as.yearmon(as.character(mdate), format = '%Y%m')]
ggplot(temp,
       aes(x = mdate, y = yc_yield)) + geom_line()

rm(temp)

# ================================================================= #
# Compute CDS Interpolation ####
# ================================================================= #
# Clean the CDS Data
cds <- cds[!is.na(parspread)&parspread<1] # exclude missing cds spread
cds[,table(tenor)]

key_cds <- c(paste0(c(0, 3, 6, 9),'M') , paste0(c(1, 2, 3, 4, 5, 7, 10, 15, 20, 30),'Y'))

cds_ttm <- data.table(tenor = key_cds,
                      ttm = c(c(0, 3, 6, 9)/12, c(1, 2, 3, 4, 5, 7, 10, 15, 20, 30))
)

# Variables tenor specific: estimated notional, compositepricerating, compositecurverating
keytab_cds_rates <- merge(cds[,.(mdate, redcode, tenor, cds_spread = parspread * 100, 
                                 estimatednotional, compositepricerating, compositecurverating)], 
                          cds_ttm, 
                          by = 'tenor', all.x = TRUE)

# ---------------------------------------------------------------- #
### Step1: extend key rates
keytab_cds_rates <- keytab_cds_rates[order(redcode, mdate, ttm)]

# get last 2 tenors with valid cds spread and extend using linear interpolation
lastcds <- keytab_cds_rates[, tail(.SD, 2), by = .(mdate, redcode), .SDcols = c('ttm', 'cds_spread')]
lastcds <- lastcds[order(redcode, mdate, ttm)]

lupper <- setnames(unique(lastcds, by = c('mdate', 'redcode'), fromLast = TRUE),
                   c('ttm', 'cds_spread'),
                   c('uttm', 'ucds'))

llower <- setnames(unique(lastcds, by = c('mdate', 'redcode')),
                   c('ttm', 'cds_spread'),
                   c('lttm', 'lcds'))

# curve tail
tail_cds <- merge(llower, lupper)
tail_cds[,cds50 := rate_extention(50, uttm, lttm, ucds, lcds, flat = TRUE)] # Markit assumes flat, error if we do not!
tail_cds[is.na(cds50), cds50:=ucds] # if only one observation, make it flat
rm(lastcds, lupper, llower)

# get first tenor and make it the first tenor (implied by the locally cte fwd model)
firstcds <- unique(keytab_cds_rates, by = c('mdate', 'redcode'))
firstcds <- firstcds[ttm != 0]

# add rows to keytab
keytab_cds_rates <- rbind(keytab_cds_rates[,.(mdate, redcode, ttm, cds_spread)],
                          tail_cds[,  .(mdate, redcode, ttm = 50, cds_spread = cds50)],
                          firstcds[, .(mdate, redcode, ttm = 0,  cds_spread)])
keytab_cds_rates <- keytab_cds_rates[order(mdate, redcode, ttm)]

# ---------------------------------------------------------------- #
### Step2: Interpolate - on year to maturity 
interpolated_rates <- mdata[!is.na(redcode),.(mdate, issue_id, redcode, ttm = tmt)]

interpolated_rates <- keytab_cds_rates[,.(mdate, redcode, ttm, lttm = ttm, lcds = cds_spread)][
  interpolated_rates, 
  on = .(mdate = mdate,  redcode = redcode, ttm = ttm), 
  roll = Inf]

interpolated_rates <- keytab_cds_rates[,.(mdate, redcode, ttm, uttm = ttm, ucds = cds_spread)][
  interpolated_rates, 
  on = .(mdate = mdate,  redcode = redcode, ttm = ttm), 
  roll = -Inf]

# calculate cds spread
interpolated_rates[lttm == uttm, cds_spread := lcds]
interpolated_rates[lttm != uttm, 
                   cds_spread := rate_interpolation(ttm, uttm, lttm, ucds, lcds, type = 'linear')]

# merge with mdata
mdata <- merge(mdata,
               interpolated_rates[,.(mdate, issue_id, cds_spread = cds_spread)],
               by = c("mdate", "issue_id"),
               all.x = TRUE)

# there could be missing because of missing cds data
# merge(mdata[!is.na(redcode)&is.na(cds_spread)],
#       cds,
#       by = c('mdate', 'redcode'))


temp <- mdata[, .(cds_spread = mean(cds_spread, na.rm = TRUE)), by = mdate]
temp[, mdate := as.yearmon(as.character(mdate), format = '%Y%m')]
ggplot(temp, aes(x = mdate, y = cds_spread)) + geom_line()

# ================================================================= #
# Add cds curve information ####
# ================================================================= #
temp <- cds[, lapply(.SD, uniqueN), by = .(mdate, redcode), 
            .SDcols = c('compositedepth5y', 'avrating', 'estimatednotional')]

# add curve average curve ratings 
ratings_rank <- data.table(avrating = c('AAA', 'AA', 'A', 'BBB', 'BB', 'B', 'CCC', 'D'),
                           rating_rank = c(1:8)
)

cds <- merge(cds, ratings_rank, by = 'avrating', all.x = TRUE)

temp <- cds[, lapply(.SD, mean, na.rm = TRUE), 
            by = .(mdate, redcode), 
            .SDcols = c('compositedepth5y', 'rating_rank', 'estimatednotional')]

temp <- ratings_rank[temp,
                     roll = Inf,
                     on = 'rating_rank']
temp[, rating_rank := NULL]

# merge
mdata <- merge(mdata, temp, by = c('mdate', 'redcode'), all.x = TRUE)
setnames(mdata, 'avrating', 'cds_avrating')

# check merge
test <- mdata[year(date)>=byear&year(date)<=eyear&amt_outstanding>0]
test[, mstatus:='NotMerged']
test[!is.na(redcode)&!is.na(gvkey), mstatus:='Merged']
test <- test[, .(out=sum(amt_outstanding)/1e6), by=.(date, mstatus)]
plot_corp_merge <- ggplot(data=test,
                          aes(x=date, y=out))+ 
  geom_area(aes(colour = mstatus, fill=factor(mstatus)), position = 'stack', alpha = 0.7) + 
  scale_fill_brewer(name="", palette="Set2", labels=c("Merged", "Not Merged"))+
  scale_colour_brewer(name="", palette="Set2", labels=c("Merged", "Not Merged"))+
  scale_x_date(date_breaks = "2 year", date_labels = "%Y")+
  #scale_y_continuous(breaks = seq(-0.2, 0.4, 0.02)*100)+
  labs(x = "", y = "Amount Outstanding (Face Value Bi $)")+
  annotate("rect", fill = "gray", alpha = 0.4, xmin = as.Date("2007-12-31"), xmax = as.Date("2009-06-30"), ymin = -Inf, ymax = Inf)+
  theme_bw()+ theme(legend.position="bottom")

plot_corp_merge
test <- dcast.data.table(test, date~mstatus, value.var='out')
test[,Merged/(Merged+NotMerged)]

# ================================================================= #
# Create Duration Buckets ####
# ================================================================= #
d_levels <- c('00y', '01y03y', '3y7y', '7y10y', '10yy')
mdata[duration < 1, db:='00y']
mdata[duration >= 1 & duration<3, db  := '01y3y']
mdata[duration >= 3 & duration<7, db  := '03y07y']
mdata[duration >= 7 & duration<10, db := '07y10y']
mdata[duration >= 10, db := '10yy']
table(mdata$db)

# ================================================================= #
# Fix missing ratings ####
# ================================================================= #
mdata[r_sp=='',r_sp:='NR']
mdata[r_mr=='',r_mr:='NR']
mdata[r_fr=='',r_fr:='NR']

# ================================================================= #
# Create Ratings ####
# ================================================================= #
# Create Meta Ratings: r_sp > r_mr > r_fr
mdata[,mr:=r_sp]
mdata[mr=='NR'|is.na(mr), mr:=r_mr]
mdata[mr=='NR'|is.na(mr), mr:=r_fr]
mdata[mr=='NR'|is.na(mr), mr:='NR']

# Create rating rank
## Do not include D since it means the bond in default
mdata[,table(mr)]
r_rank <- list(c('AAA'),
               c('AA1', 'AA+'),
               c('AA2', 'AA'),
               c('AA3', 'AA-'),
               c('A1', 'A+'),
               c('A2', 'A'),
               c('A3', 'A-'),
               c('BAA1', 'BBB+'),
               c('BAA2', 'BBB'),
               c('BAA3', 'BBB-'),
               c('BA1', 'BB+'),
               c('BA2', 'BB'),
               c('BA3', 'BB-'),
               c('B1', 'B+'),
               c('B2', 'B'),
               c('B3', 'B-'),
               c('CAA1', 'CCC+'),
               c('CAA2', 'CCC'),
               c('CAA3', 'CCC-'),
               c('CA', 'CC', 'C'),
               c('D'),
               c('NR'))

# Higher rating higher ranking
mdata[,rating_rank:=numeric()]
for (i in c(1:length(r_rank))) {
  mdata[mr%in%r_rank[[i]], rating_rank:=i]   #
}
mdata[is.na(rating_rank)]
mdata[,table(rating_rank)]
mdata[rating_rank==22, table(rating_cat)]

# Create Naics Rating Code
naics_labels <- c('NAIC1 (AAA/AA)','NAIC1 (A)', 
                  'NAIC2 (BBB)','NAIC3 (BB)', 
                  'NAIC4 (B)', 'NAIC5 (CCC)', 
                  'NAIC6 (bCC)', 'NR')
naics_names <- c('NAIC1_AA','NAIC1_A', 
                 'NAIC2','NAIC3', 
                 'NAIC4', 'NAIC5', 
                 'NAIC6', 'NR')

mdata[mr%in%c('AAA', 'AA+','AA','AA-', 'AA1', 'AA2', 'AA3'), naics:=naics_names[1]]
mdata[mr%in%c('A+','A', 'A-', 'A1', 'A2', 'A3'),naics:=naics_names[2]]
mdata[mr%in%c("BBB+", "BBB", "BBB-", 'BAA1', 'BAA2',  'BAA3'), naics:=naics_names[3]]
mdata[mr%in%c(c("BB+", "BB", "BB-",  "BA1", "BA2", "BA3")), naics:=naics_names[4]]
mdata[mr%in%c("B+", "B", "B-", "B1", "B2", "B3"), naics:=naics_names[5]]
mdata[mr%in%c("CCC+", "CCC", "CCC-", "CAA1", "CAA2", "CAA3"), naics:=naics_names[6]]
mdata[mr%in%c("CC", "C", "CA", "C", "D"), naics:=naics_names[7]]
mdata[mr=='NR'|is.na(mr), naics:=naics_names[8]]

mdata[,naics:=as.character(factor(mdata$naics, levels = naics_names))]
table(mdata$rating_cat)
table(mdata$naics)

mdata[,table(rating_cat, useNA = 'always')]
mdata[rating_cat=='D',table(r_fr, useNA = 'always')]
mdata[rating_cat=='D',table(r_sp, useNA = 'always')]
mdata[rating_cat=='D',table(naics, useNA = 'always')]

mdata[rating_class!='0.IG', table(naics, useNA = 'always')]
mdata[naics=='NAIC6', table(r_mr, useNA = 'always')]

# Calculate aggregate rating 
rcat_labels <- c('AAA and AA','A', 
                 'BBB','BB', 
                 'B and Below', 'NR')
rcat_names <- c('AAA_AA','A', 
                'BBB','BB', 
                'Bb', 'NR')

mdata[,rcat:='NR']
mdata[naics=='NAIC1_AA', rcat:=rcat_names[1]]
mdata[naics=='NAIC1_A', rcat:=rcat_names[2]]
mdata[naics=='NAIC2', rcat:=rcat_names[3]]
mdata[naics=='NAIC3', rcat:=rcat_names[4]]
mdata[naics%in%c('NAIC4', 'NAIC5', 'NAIC6'), rcat:=rcat_names[5]]
mdata[,rcat:=as.character(factor(rcat, levels=rcat_names))]
mdata[,table(rcat, useNA = 'always')]
mdata[,table(naics, useNA = 'always')]

# Calculate Index Flag Variable 
mdata[rating_class=='0.IG', ig_index:='IG']
mdata[rating_class=='1.HY', ig_index:='HY']
mdata[,rating_class:=NULL]

## Aggregate by coars rating rank
mdata[rating_rank==1, coarse_rating_rank:=1] # AAA
mdata[rating_rank%in%c(2,3,4), coarse_rating_rank:=2] # AA
mdata[rating_rank%in%c(5,6,7), coarse_rating_rank:=3] # A
mdata[rating_rank%in%c(8,9,10), coarse_rating_rank:=4] # BBB
mdata[rating_rank%in%c(11,12,13), coarse_rating_rank:=5] # BB
mdata[rating_rank%in%c(14,15,16), coarse_rating_rank:=6] # B
mdata[rating_rank%in%c(17,18,19), coarse_rating_rank:=7] # CCC
mdata[rating_rank==20, coarse_rating_rank:=8] # CC & C
mdata[rating_rank==21, coarse_rating_rank:=9] # D

# ================================================================= #
# Seniority ####
# ================================================================= #
# mdata[,table(security_level)]
# mdata[,uniqueN(issue_id), by ="security_level"]
mdata[, seniority3 := security_level]
mdata[security_level %in% c("SENS", "JUN", "JUNS"), seniority3:="SUB"]
# Assumption is that secd mean secured
mdata[like(issue_name,"SECD") & security_level=="SEN", seniority3:="SS"]
mdata[security_level == "NON", seniority3:="SEN"]
mdata[is.na(security_level), seniority3:="SEN"]
# mdata[,table(seniority3)]

# ================================================================= #
# Add Compustat Data ####
# ================================================================= #
mdata[, cyqtr:=as.yearqtr(date)]
compq[, mean(is.na(sicf))]
mdata <- merge(mdata,
               compq,
               by = c('gvkey', 'cyqtr'),
               all.x = TRUE)
print(sprintf('We have a failure when merging with compustat in %.2f%% of the data.',
              mdata[,mean(is.na(atq))]*100))

# ================================================================= #
# Calculate Credit Spreads and Basis ####
# ================================================================= #
# Calculate credit spread
mdata[ , cs_yield := yield - yc_yield]

# Calculate CDS-basis 
mdata[,basis_yield:= cds_spread - cs_yield]

# ================================================================= #
# Calculate Liquidity Measures ####
# ================================================================= #
# Bid-ask spread
mdata[price_eom>0, bid_ask:=(pmin(t_spread, 0.2*price_eom) /price_eom)*100]

# Turnover
# t_volume: USD 1
# amt_outstading: USD 10^3
mdata[amt_outstanding>0, turnover := t_volume/(amt_outstanding*10)]
mdata[, cyqtr := as.character(cyqtr)]

# ================================================================= #
# Save Data ####
# ================================================================= #
if(any(duplicated(mdata[,.(issue_id, mdate)]))){
  stop('Mdata primary key violated.')
}

db_write(merges, connectArgs, "bond_returns_master", mdata, pk = c("issue_id", "date"))
check_db(merges, "bond_returns_master", mdata, sort_cols = c("issue_id", "date"))

test <- mdata[duration > 1 & security_level == 'SEN' & basis_yield<0, lapply(.SD, mean, na.rm = TRUE), .SDcols = c('cs_yield', 'cds_spread', 'basis_yield'), by = mdate]
ggplot(test, aes(x = mdate)) + geom_line(aes(y = cs_yield, colour = 'cs')) +
  geom_line(aes(y = basis_yield, colour = 'basis')) +
  geom_line(aes(y = cds_spread, colour = 'cds'))

print('Bond returns master has been created successfully in databse.')


