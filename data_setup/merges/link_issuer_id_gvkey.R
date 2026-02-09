# ================================================================= #
# link_issuer_id_gvkey.R ####
# ================================================================= #
# Description:
# -------------
#   The objective is to add a gvkey to the FISD data set.
#
# Input(s) :
# -------------
# FISD
# ---------
#   1. fisd.issue
#   2. fisd.mergedissuer
#   3. fisd.issuer_cusip
#   4. fisd.agent
#   5. fisd.corp_amt_outstanding
#
# Compustat
# ---------
#   1. compustat.quarterly
#   2. compustat.names
#   3. compustat.histnames
#   4. compustat.conml
#
# S&P Ratings
# ---------
#   1. ciq.wrds_gvkey
#   2. ciq.wrds_cusip
#
# Bond and CRSP Link
# ---------
#   1. trace.wrds_bondcrsp_link
#   2. crsp.ccmxpf_lnkhist
#   3. crsp.stocknames
#
# bond returns
# ---------
#   1. trace.wrds_bondret
#
# Output(s) :
# -------------
#   Dropbox data/processed/:
#   1.merges.corp_amt_outstanding_cmerged
#   2.merges.map_issuer_id_gvkey
#
# Date:
# -------------
#   2022-08-26
#   update: 2026-01-23
#
# Author(s):
# -------------
#   Lira Mota, liramota@mit.edu
#   Kerry Siani. ksiani@mit.edu
#   Ruiquan Chang, chang.2590@osu.edu
#
# Additional note(s):
# -------------
#    Cleaning steps:
#    1. Consider only corporate bonds
#    2. Consider only Dollar denominated bonds
#    4. Consider only industries - not government, etc...
#    
#    ------------------------------------------------------------------------- #
#    1. Try to merge using WRDS bonds return data link - this is a unique link with PERMNO
#    2. Try to merge using CRSP stock names
#    2. Try to merge using Compustat names
#    3. Try to merge using WRDS CIQ bond ret link (from SP ratings)
#    5. After all merges, inconsistency might happen.
#       a.  One bond might be connected to multiple gvkeys during the life of the bond. 
#           It is ok if it is an acquisition - in this case it should stay until the end of the life of the company.
#       b. We should not have "jumping" gvkeys.  A bond connected to one gvkey then a another then back to the first.
#    6. Check consistency with compustat:
#       a. total amount outstanding > total assets?
#       b. total amount outstanding > total debt?
#
#    ------------------------------------------------------------------------- #
#    Notes:
#    One issue that is not taken in consideration is that the a bond might be issued by a subsidiary and the GVKEY is at the parent level.
#    In principle, we could use parent_id from the issuer_id. The problem is that parent_id is an static entry: vide example of Time Warner and AT&T.
#    Padding using parent id would generate false merges before acquisition.
#   
#   # FISD data set description ####
#      A code indicating the industry to which the issuer belongs.
#     ----------------------------------------------------------------------#
#     Possible codes are:
#      Industrial
#      10 - Manufacturing
#      11 - Media/Communications
#      12 - Oil & Gas
#      13 - Railroad
#      14 - Retail
#      15 - Service/Leisure
#      16 - Transportation
#      32 - Telephone
#      Finance
#      20 - Banking
#      21 - Credit/Financing
#      22 - Financial Services
#      23 - Insurance
#      24 - Real Estate
#      25 - Savings & Loan
#      26 - Leasing
#      Utility
#      30 - Electric
#      31 - Gas
#      33 - Water
#      Government
#      40 - Foreign Agencies
#      41 - Foreign
#      42 - Supranationals
#      43 - U.S. Treasuries

#     Bond Type
#     CCOV BOND_TYPE US Corporate Convertible
#     CCPI BOND_TYPE US Corporate Inflation Indexed
#     CDEB BOND_TYPE US Corporate Debentures
#     CLOC BOND_TYPE US Corporate LOC Backed
#     CMTN BOND_TYPE US Corporate MTN
#     CMTZ BOND_TYPE US Corporate MTN Zero
#     CP BOND_TYPE US Corporate Paper
#     CPAS BOND_TYPE US Corporate Pass Thru Trust
#     CPIK BOND_TYPE US Corporate PIK Bond
#     CS BOND_TYPE US Corporate Strip
#     CUIT BOND_TYPE US Corporate UIT
#     CZ BOND_TYPE US Corporate Zero
#     USBN BOND_TYPE US Corporate Bank Note
#     UCID BOND_TYPE US Corporate Insured Debenture
# ================================================================= #

# ================================================================= #
# Environment ####
# ================================================================= #
print('Started script to merge FISD and Compustat.')

# Clear workspace
rm(list = ls()); gc()

# Import libraries
library(data.table)
library(zoo)
library(stringr)
library(stringdist)
library(reticulate)

# Source helper scripts
source("utils/setPaths.R")
source("utils/clean_company_names0.R")
source("utils/clean_company_names1.R")
source("utils/clean_company_names2.R")
source("utils/clean_map.R") 

# ----------------------------------------------------------------------#
# load Python modules
# indicate that we want to use a specific virtualenv
use_virtualenv("r-reticulate") # input  RETICULATE_PYTHON="/c/Users/ksiani/Documents/.virtualenvs/r-reticulate/Scripts/python.exe" to bash to get reticulate to point to correct version of python
#use_python('/opt/homebrew/anaconda3/bin/python') # make sure to update this path. 
#use_python('/usr/local/anaconda3/bin/python')
POLYFUZZ <- import("polyfuzz") # see https://towardsdatascience.com/string-matching-with-bert-tf-idf-and-more-274bb3a95136

# End of sample date
start_sample <- '1999-12-31'
end_sample <- '2023-12-31'

# We need to select only corporate bonds.
# corporate_bond_types <- c("CDEB", "CMTN", "CMTZ") # WRDS bond ret bond types
corporate_bond_types <- c('CCOV','CCPI','CDEB','CLOC','CMTN','CMTZ','CP',
                          'CPAS','CPIK','CS','CUIT','CZ','USBN','UCID')

# FISD have issuance in foreign currency. Should we exclude it?
no_foreign_currency <- TRUE

# There are industry classifications that is clearly not corporations. 
# Should we exclude it?
only_corp <- TRUE
not_corp_naics_code <- c('92') # keep utilities
#not_corp <- c(40,41,42,43,44,45,60,99)

# US firms
only_us <- TRUE

# Link rank
rank <- data.table(merge_type = c('wrds_link', 'ciq', 'crsp_cusip', 'comp_cusip', 'str_link'),
                   merge_rank = c(1, 2, 3, 4, 5))


# ================================================================= #
# Read data ####
# ================================================================= #

# FISD
issue <- db_read(dbs$fisd, "issue")
issuer <- db_read(dbs$fisd, 'mergedissuer')
issuer_cusip <- db_read(dbs$fisd, 'issuer_cusip')
agent <- db_read(dbs$fisd, 'agent')
amtout <- db_read(dbs$fisd, 'corp_amt_outstanding')

# Compustat
compq <- db_read(dbs$merges, 'compustat_quarterly')
comp_names <- db_read(dbs$compustat, 'names')
hcomp_names <- db_read(dbs$compustat, 'histnames')
comp_conml <- db_read(dbs$compustat, 'conml')

# S&P Ratings names tables
wrds_gvkeys <- db_read(dbs$ciq, 'wrds_gvkey')
wrds_cusips <- db_read(dbs$ciq, 'wrds_cusip')

# Import Bond and CRSP Link
link <- db_read(dbs$trace, 'wrds_bondcrsp_link')
cclink <- db_read(dbs$crsp, 'crspq_ccmxpf_lnkhist')
snames <- db_read(dbs$crsp, 'stocknames')

# Import bond returns
bondret <- db_read(dbs$trace, 'wrds_bondret')

#========================================================================#
# Get only valid GVKEYS for all link tables ####
#========================================================================#
# We only want compustat entries with valid total assets and total debt
# It has been cleaned in the step before. 
compq[,mean(is.na(atq))]
compq[,mean(is.na(tdebtq))]
compq <- compq[atq > 0]

# Select data in sample range
compq <- compq[datadate>=start_sample & datadate<=end_sample]

# Make sure to select company identifiers with valid entries in Compustat
compq[ ,mean(is.na(permco))]
gvkey_list <- compq[, unique(gvkey)]
permco_list <- compq[!is.na(permco), unique(permco)]

# I think there is a lot of international companies reported in wrds_gvkeys
wrds_gvkeys[,mean(gvkey%in%gvkey_list)]
wrds_gvkeys <- wrds_gvkeys[gvkey%in%gvkey_list]

comp_names[ ,mean(gvkey%in%gvkey_list)]
comp_names <- comp_names[gvkey%in%gvkey_list]

link[,mean(permco%in%permco_list)]
link   <- link[permco%in%permco_list]

cclink <- cclink[gvkey%in%gvkey_list]

snames <- snames[permco%in%permco_list]

#========================================================================#
# Create Compustat/CRSP range dates ####
#========================================================================#
# Make sure we always have valid links.
# Links that in fact we can merge to compustat merged

# Let's create a compustat linking table that accounts for jumping dates
temp <- copy(compq[,.(gvkey, conm, datafqtr, datacqtr = as.yearqtr(datacqtr), datadate)][order(gvkey, datacqtr)])
temp[, qdiff :=  datacqtr - shift(datacqtr), by = gvkey]
temp[, ncount := 0]
temp[is.na(qdiff) | qdiff > 1, ncount := 1]
temp[, ndateblock := cumsum(ncount), by = gvkey]

comp_range <- temp[!is.na(gvkey), 
                   .(first_cc = min(datadate),
                     last_cc = max(datadate)),
                   by = .(gvkey, conm, ndateblock)]
comp_range[ , ndateblock := NULL]
rm(temp)

# add legal names
comp_range[,conm_clean := clean_company_names0(conm)]
comp_conml[,conml_clean := clean_company_names0(conml)]
comp_range <- merge(comp_range, comp_conml, all.x = TRUE)

# add names with no abbreviation
comp_range[,conml_nab := clean_company_names1(conml)]

#========================================================================#
# Create historical company names to comp_range ####
#========================================================================#
# We add historical names from two sources: 
# crsp provided compustat historical names and crsp historical names
# issue is that comp hist starts in 2007.
# 
# We proceed with a hierarchical order. First try comp hist, if not available use crsp.

# ------------------- #
# STEP 1: COMP HIST   #
# ------------------- #

# Compustat comn is a header - it only reports the most recent company name.
# We will use CRSP historical comp_name to retrieve as of date company name (hcomp_names).
# The issue is that CRSP comp hist only starts in 2007.

## Create clean historical Compustat names table (hist_conm)
# Consider only legal name changes from hcomp_names
hcomp_names[,hconml := clean_company_names1(hconml)] # Although CRSP reports legal names, abbreviations are not standard.
hcomp_names <- hcomp_names[order(gvkey, hchgdt, hchgenddt)]
hcomp_names[, name_change := (hconml != shift(hconml)), by =gvkey]
hcomp_names[is.na(name_change), name_change:=TRUE]
hcomp_names[,hconm_id := cumsum(name_change), by =gvkey]

hist_conm <- hcomp_names[ ,.(start_date = min(hchgdt),
                             end_date = max(hchgenddt),
                             hconml = first(hconml)), 
                          by = .(gvkey, hconm_id)]


# In case end_date is NA, consider the name in comp_names (most recent)
# Exclude this entry from hist_conm
hist_conm <- hist_conm[!is.na(end_date)]
# hist_conm[is.na(start_date)]

# Start date is not relevant in this case
min_date <- hist_conm[,min(start_date)]
hist_conm[,range(end_date)]
hist_conm <- hist_conm[,.(gvkey, hconml, start_date, end_date)]

# Checks
# ------- #
# do we have overlaps?
hist_conm <- hist_conm[order(gvkey, start_date, end_date)]
hist_conm[, lsd:=shift(start_date, n=-1), by = gvkey]
hist_conm[lsd<end_date]

# does it skip dates?
hist_conm[,date_diff:= lsd-end_date]
hist_conm[,max(date_diff, na.rm=T)]

# clean
hist_conm[,c('lsd', 'date_diff'):=NULL]

# ---------------------------- #
# STEP 2: CRSP HIST (snames)   #
# ---------------------------- #
# add snames (stock names from CRSP) to cnames (compustat names) to expand the cnames 
# first use permno as identifier, than change to firm level (gvkey)

## deal with duplicates in snames
snames[,comnam := clean_company_names1(comnam)]
snames <- snames[order(permno,namedt, nameenddt)]
snames2 <- unique(snames, by = c('permno', 'namedt', 'nameenddt','comnam'))

## check if the name changed, and if so, index each name by permno
snames2[,name_change := (comnam!=shift(comnam)), by =permno]
snames2[is.na(name_change), name_change:=TRUE]
snames2[,comnam_id:=cumsum(name_change), by =permno]

hist_snames <- snames2[ ,.(start_date = min(namedt),
                           end_date = max(nameenddt),
                           comnam  = first(comnam)), 
                        by = .(permno, comnam_id)]

# Checks
# ------- #
# do we have overlaps?
hist_snames <- hist_snames[order(permno, start_date, end_date)]
hist_snames[, lsd:=shift(start_date, n=-1), by = permno]
hist_snames[lsd<end_date]
# does it skip dates?
hist_snames[,date_diff:= lsd-end_date]
hist_snames[,max(date_diff, na.rm=T)]

# -------------------------- #
# merge hist_snames to gvkey #
# -------------------------- #
# Need to merge PERMNO with GVKEY
# cclink is at the PERMNO level, it might generate duplicates
cclink[permco==1728] # is this a problem?

lhist_snames <- merge(hist_snames[,.(permno, start_date, end_date, comnam)], 
                      cclink, 
                      by = 'permno')
lhist_snames[,start_date := pmax(start_date, linkdt, na.rm=TRUE)]
lhist_snames[,end_date := pmin(end_date, linkenddt, na.rm=TRUE)]
lhist_snames <- lhist_snames[(start_date<=end_date)]
lhist_snames <- lhist_snames[,.(permno, permco, gvkey, comnam, start_date, end_date)]

# Notice that one GVKEY might be linked with multiple comnan for the same date range. 
# clean duplicated entries
clean_lhist_snames <- unique(lhist_snames[,.(gvkey, comnam, start_date, end_date)])

clean_lhist_snames <- clean_map(clean_lhist_snames, 
                                id_vars = c('gvkey', 'comnam', 'start_date', 'end_date')
)

# We will use crsp hist name only before comp hist min_date 
clean_lhist_snames[,range(end_date)]
clean_lhist_snames[end_date >= min_date, end_date := min_date-1]
clean_lhist_snames <- clean_lhist_snames[start_date < end_date]
rm(lhist_snames, hist_snames, snames2)

# ------------------------ #
# Combine all names data
# ------------------------ #
# hname stands for history name - it comes from crsp and compustat 
temp1 <- rbind(hist_conm[,.(gvkey, hname = hconml, 
                            start_date, end_date, name_source = 'comp')], 
               clean_lhist_snames[,.(gvkey, hname = comnam, 
                                     start_date, end_date, name_source = 'crsp')])

# FIX 1: Add header name when missing
temp2 <- merge(comp_range, temp1, by ='gvkey', all.x = TRUE)
temp2 <- temp2[order(gvkey, start_date, end_date)]

missing_header <- unique(temp2, by='gvkey', fromLast = TRUE)
missing_header <- missing_header[end_date < last_cc | is.na(hname)]

missing_header[!is.na(hname), c('hname', 'start_date', 'end_date', 'name_source') := 
                 .(conml_nab, end_date + 1, last_cc, 'comp_header')]

missing_header[is.na(hname), c('hname', 'start_date', 'end_date', 'name_source') := 
                 .(conml_nab, first_cc, last_cc, 'comp_header')]

missing_header <- missing_header[,.(gvkey, hname, start_date, end_date, name_source)]
#missing_header[gvkey=='007533']

# FIX 2: Add first entry when missing
temp2 <- temp2[order(gvkey, first_cc)]
temp2[, ncount := .N, by = gvkey]
missing_first <- unique(temp2, by='gvkey')

# only add entries that are not already included in missing header
missing_first <- missing_first[(is.na(start_date) & ncount >1) | start_date > first_cc]

missing_first[!is.na(hname), c('end_date', 'start_date') := .(start_date-1, first_cc)]
missing_first[is.na(hname), c('hname', 'start_date', 'end_date') := .(conml_nab, first_cc, last_cc)]
missing_first[, name_source := 'first_pad']
missing_first <- missing_first[,.(gvkey, hname, start_date, end_date, name_source)]
rm(temp2)

# FIX 3: Add entries when there no smooth pasting between crsp and compustat
temp3 <- copy(temp1[order(gvkey, start_date, end_date)])
temp3[, nextlink := shift(start_date, -1), by = gvkey]
temp3[, nextns := shift(name_source, -1), by = gvkey]
temp3[, nextname := shift(hname, -1), by = gvkey]

# select entry with gap in pasting
temp3 <- temp3[(nextlink - end_date > 1) & (name_source != nextns)]
temp3[,table(name_source)]
temp3[,table(nextns)]
temp3 <- temp3[ ,.(gvkey, hname = nextname, 
                   start_date = end_date + 1,  end_date = nextlink - 1,
                   name_source = 'pad_mid')]

# Combine all
names_hist <- rbind(temp1, 
                    missing_header,
                    missing_first,
                    temp3
)

names_hist <- clean_map(names_hist, 
                        id_vars = c('gvkey', 'hname', 'start_date', 'end_date'),
                        link_source = 'name_source')
rm(temp1, temp3, missing_first, missing_header, clean_lhist_snames, hist_conm)

#========================================================================#
# Date ranges for wrds_gvkeys (link between gvkey and company_id) ####
#========================================================================#
# Note that company name is also a header variable. Saves only the most recent company name.
wrds_gvkeys <- merge(wrds_gvkeys, comp_range[,.(gvkey, first_cc, last_cc)], by = 'gvkey')
wrds_gvkeys[startdate < first_cc | is.na(startdate), startdate:=first_cc]
wrds_gvkeys[enddate > last_cc | is.na(enddate), enddate:=last_cc]
wrds_gvkeys[,c('first_cc', 'last_cc'):=NULL]
wrds_gvkeys <- wrds_gvkeys[startdate < enddate]

#========================================================================#
# Create Base Table ####
#========================================================================#
# Our base table comes from FISD amt outstanding
# Base is **issue_id** centered. Later we will make **issuer_id** centered.
# -----------------------------------------------------------------------#

# Identify valid issue_id
# -----------------------------------------------------------------------#
# We clean to include only corporate bonds.
# Merge with issuer info
vissue <- merge(issue, issuer) # does not loose any observation
vissue <- vissue[, .(issue_id, issuer_id, 
                     agent_id, parent_id,
                     prospectus_issuer_name, legal_name, cusip_name,
                     cusip = complete_cusip, issuer_cusip, isin,
                     offering_date, offering_amt, maturity,
                     bond_type, security_level, foreign_currency, 
                     country_domicile, country,
                     industry_code, sic_code, naics_code)]
vissue[, fisd_name := clean_company_names1(legal_name)]

# Cleaning vissue: select valid issue_id
# ------------------------------------------------------------------------- #
# Select Corporate
print(sprintf('Exclude %.2f%% because does not belong to corporate bond classification.',
              vissue[,mean(!bond_type%in%corporate_bond_types)]*100))
vissue <-  vissue[bond_type %in% corporate_bond_types]
table(vissue$bond_type)

# No Foreign Currency
any(vissue[ , is.na(foreign_currency)])
print(sprintf('Exclude %.2f%% because issuance in foreign currency.',
              vissue[ , mean(foreign_currency == "Y")]*100))
vissue <- vissue[foreign_currency == "N"]

# clean what is not industry 
vissue[,naics_code_3:=substr(naics_code, 1, 2)]
vissue[,mean(is.na(naics_code_3))]*100

if(only_corp){
  print(sprintf('Exclude %.2f%% because it is in goverment.',
                vissue[ , mean(naics_code_3 %in% not_corp_naics_code)]*100))
  # We are keeping missing NAICS
  vissue <- vissue[!(naics_code_3 %in% not_corp_naics_code)]
}

if(only_us){
  print(sprintf('Exclude %.2f%% because country of domicile is not US.',
                vissue[ , mean(country_domicile!='USA', na.rm=TRUE)]*100))
  # We are keeping missing NAICS
  vissue <- vissue[country_domicile=='USA']
}

# Cleaning Center: select from amtout valid issue_id
# ------------------------------------------------------------------------- #
amtout[,mean(is.na(date))] 
base <- amtout[issue_id %in% vissue$issue_id & amt_outstanding>0 & date>=start_sample & date<=end_sample]
base[,range(date)]
issuer_cusip <- issuer_cusip[issuer_id%in%unique(vissue$issuer_id)]

# Check duplicates 
any(duplicated(base[!is.na(date)], by = c('issue_id', 'date')))

# Merge with important information
base <- merge(vissue[,.(issue_id, issuer_id, 
                        agent_id, parent_id,
                        fisd_name,
                        legal_name, prospectus_issuer_name, 
                        cusip, issuer_cusip, cusip_name, 
                        isin, 
                        offering_date, maturity,
                        bond_type, security_level)],
              base, by = 'issue_id')

# Merge with parent id
base <- merge(base, 
              agent[,.(parent_id = agent_id, parent_legal_name = legal_name)], 
              by = 'parent_id', all.x = TRUE)
base[,mean(is.na(parent_legal_name))]

# Clean memory
rm(amtout, vissue)

# The number of issues we have to track!
issue_num <- base[,uniqueN(issue_id)]
nrows_base <- nrow(base)

#=========================================================================#
# Step 1: Use Compustat names: merge with 6-digit cusip  ####
#=========================================================================#
cnames <- merge(names_hist, 
                comp_names[!is.na(cusip),.(gvkey, issuer_cusip=substr(cusip,1,6))])

# merge 
base[,date_link := date]
base_comp_cusip <- base[cnames[!is.na(issuer_cusip), .(gvkey, issuer_cusip, hname, start_date, end_date)],
                        on = .(issuer_cusip = issuer_cusip, date_link >= start_date, date_link <= end_date),
                        nomatch=0]

# Let's deal with duplicates
mean(duplicated(base_comp_cusip, by = c('issue_id', 'date', 'gvkey')))*100

## created duplicated but linked to the same gvkey - not an issue
base_comp_cusip <- unique(base_comp_cusip, by = c('issue_id', 'date', 'gvkey'))

## created duplicated but linked to different gvkey - ISSUE
mean(duplicated(base_comp_cusip, by = c('issue_id', 'date')))*100

#=========================================================================#
# Step 2: Merge Using CRSP Names #### 
#=========================================================================#
snames[is.na(ncusip), ncusip:=cusip]
# ncusip is the historical cusip 
snames[ , issuer_cusip := substr(ncusip, 1, 6)]
snames[,crsp_name:=clean_company_names1(comnam)]

# many duplicates
mean(duplicated(snames, by = 'issuer_cusip'))

base[,date_link:=date]
base_crsp_cusip_permno <- base[snames[,.(permno,  issuer_cusip, crsp_name, namedt, nameenddt)],
                               on = .(issuer_cusip = issuer_cusip, date_link>=namedt, date_link<=nameenddt),
                               nomatch=0]

# ----------------------------------------------------------#
# Merge with compustat to get gvkey
cclink[is.na(linkdt)]
cclink[is.na(linkenddt), linkenddt:=max(base$date)]
base_crsp_cusip_permno[, date_link := date]
base_crsp_cusip <- base_crsp_cusip_permno[cclink,
                                          on = .(permno = permno, date_link>=linkdt, date_link<=linkenddt),
                                          nomatch=0]

# We created not harmful duplicates - they end up with the same gvkey
sum(duplicated(base_crsp_cusip, by = c('issue_id', 'date', 'gvkey')))
base_crsp_cusip <- unique(base_crsp_cusip, by = c('issue_id', 'date', 'gvkey'))

# We also created harmful duplicates - duplicating issuance
mean(duplicated(base_crsp_cusip, by = c('issue_id', 'date')))*100
rm(base_crsp_cusip_permno)

#=========================================================================#
# Step 3: Merge with WRDS Cusip: S&P ratings ####
#=========================================================================#
# using this merge is tricky because it does not take into account the parent company.
# From this step on we do not pad in parent_id to avoid possible errors due to subsidiary merge.
any(duplicated(wrds_cusips))
wrds_cusips <- unique(wrds_cusips)
wrds_cusips[is.na(enddate), enddate:=as.Date(end_sample)]

# Will still need to deal with duplicates
wrds_cusips[cusip=='057224BA4']
# Difficult to understand merge - looks there is a gap!

# The merge is great
base[, mean(cusip %in% unique(wrds_cusips$cusip))]*100 # 90%

# Merge wrds cusips with base: at company id level
base[,date_link := date]
base_ratings_cid <- base[wrds_cusips[,.(cusip, companyid, startdate, enddate)], 
                         on = .(cusip = cusip, date_link>=startdate, date_link<=enddate),
                         nomatch=0]

# Some case we create duplicates, but they have the same companyid.
# So it is not harmful. Just delete
any(duplicated(base_ratings_cid, by = c('issue_id', 'date')))
base_ratings_cid[duplicated(base_ratings_cid, by = c('issue_id', 'date')), 
                 .(issue_id, cusip, cusip_name, companyid, date)]
base_ratings_cid <- unique(base_ratings_cid, by = c('issue_id', 'companyid', 'date'), fromLast = TRUE)

# Take care of duplicates later, after gvkey merge
# They all financial firms.
base_ratings_cid[duplicated(base_ratings_cid, by = c('issue_id', 'date')), unique(issue_id)]
base_ratings_cid[issue_id == 498472, .(issue_id, cusip, cusip_name, companyid, fisd_name, date)][order(date)]

# Merge base_ratings_cid with WRDS GVKEY
#=========================================================================#
base_ratings_cid[ , mean(companyid%in%unique(wrds_gvkeys$companyid))]

# The merge will not be amazing...
base_ratings_cid[, date_link := date]
base_ratings <- base_ratings_cid[wrds_gvkeys[,.(gvkey, companyid, startdate, enddate)], 
                                 on = .(companyid=companyid, date_link>=startdate, date_link<=enddate),
                                 nomatch=0]

# Sometimes it creates duplicates but they have the same gvkey.
# No harm in deleting them
sum(duplicated(base_ratings, by = c('issue_id', 'date', 'gvkey')))
base_ratings <- unique(base_ratings[order(issue_id, gvkey)], by = c('issue_id', 'date', 'gvkey'))

# We did not create harmful duplicates - duplicating issuance
mean(duplicated(base_ratings, by = c('issue_id', 'date')))*100

#========================================================================-#
# Step 4: Merge with Bond Returns Link ####
#=========================================================================#
link[is.na(link_startdt)]
link[is.na(link_enddt)]

base[,date_link := date]
base_wrds_link_permno <- base[link, 
                              on = .(cusip = cusip, date_link>=link_startdt, date_link<=link_enddt),
                              nomatch = 0]

base_wrds_link_permno[, date_link := date]
base_wrds_link <- base_wrds_link_permno[cclink,
                                        on = .(permno = permno, date_link>=linkdt, date_link<=linkenddt),
                                        nomatch = 0]

# We created not harmful duplicates - they end up with the same gvkey
sum(duplicated(base_wrds_link, by = c('issue_id', 'date', 'gvkey')))
base_wrds_link <- unique(base_wrds_link, by = c('issue_id', 'date', 'gvkey'))

# We also created harmful duplicates - duplicating issuance
mean(duplicated(base_wrds_link, by = c('issue_id', 'date')))*100

#========================================================================-#
# Step 5: Fix duplicates ####
#=========================================================================#
lbase <- rbind(base_wrds_link[,.(date, cusip, issue_id, issuer_id, gvkey, fisd_name, merge_type = 'wrds_link')],
               base_ratings[,.(date, cusip, issue_id, issuer_id, gvkey, fisd_name, merge_type = 'ciq')],
               base_crsp_cusip[,.(date, cusip, issue_id, issuer_id, gvkey, fisd_name, merge_type = 'crsp_cusip')],
               base_comp_cusip[,.(date, cusip, issue_id, issuer_id, gvkey, fisd_name, merge_type = 'comp_cusip')])
lbase[,table(merge_type)]               
lbase <- merge(lbase, rank, by = 'merge_type')

# There links which are equivalent: we can just clean these
lbase <- lbase[order(date, issue_id, merge_rank)]
lbase <- unique(lbase, by = c('date', 'issue_id', 'gvkey'))
lbase[,table(merge_type)]               

# Make sure the link is valid (valid compustat entry)
vbase <- merge(lbase,
               comp_range[, .(gvkey, first_cc, last_cc)],
               all.x = TRUE,
               by = 'gvkey')
vbase <- vbase[date >= first_cc & date <= last_cc]
vbase <- unique(vbase[, .(issue_id, cusip, date, fisd_name, gvkey, merge_type)], by = c('issue_id', 'date', 'gvkey'))

# Add hname to take care of duplicates
print(sprintf('After all merges we created %.2f%% of duplicates.',
              sum(duplicated(vbase, by = c('date', 'issue_id')))/nrow(lbase)*100))

# Add hname and use name distance to exclude duplicates
vbase[, date_link := date]
vbase <- names_hist[vbase, on = .(gvkey, start_date <= date_link, end_date >= date_link)]
vbase[is.na(hname), .(gvkey, date, issue_id, fisd_name, merge_type)]
vbase <- vbase[, .(issue_id, cusip, date, gvkey, fisd_name, hname, name_source, merge_type)]

# Add hname and use name distance to exclude duplicates in case of exact merge
vbase[, dup := .N, by = .(issue_id, date)]

# Exclude entries for which there is at least one perfect merge
vbase[dup > 1, ndist := stringdist(hname, fisd_name)]
vbase[dup > 1, mindist:= min(ndist), by = .(issue_id, date)]
cbase <- copy(vbase[!(dup > 1 & mindist==0 & ndist > 0)])
cbase[, c('dup', 'ndist', 'mindist') := NULL]

# Clean name for suffixes and keep if one perfect merge 
cbase[, dup := .N, by = .(issue_id, date)]
cbase[dup > 1, ndist2 := stringdist(clean_company_names2(hname), clean_company_names2(fisd_name))]
cbase[dup > 1, mindist:= min(ndist2), by = .(issue_id, date)]
cbase <- copy(cbase[!(dup > 1 & mindist==0 & ndist2 > 0)])#[issue_id == 467368 & date == '2008-06-30']
cbase[,table(dup)]
cbase[, c('dup', 'ndist2', 'mindist') := NULL]

# Add hname to take care of duplicates
print(sprintf('After names cleaning we still have %.2f%% of duplicates.',
              sum(duplicated(cbase, by = c('date', 'issue_id')))/nrow(lbase)*100))

# Use merge order to deal with the remaining duplicates
cbase <- cbase[order(issue_id, date, merge_type)]
cbase <- unique(cbase, by = c('issue_id', 'date'))

any(duplicated(cbase, by = c('issue_id', 'date')))
#cbase[issue_id == 467368 & date == '2008-06-30']

# Merge final issue_id merge to base
base <- merge(base,
              cbase[ , .(issue_id, date, gvkey, hname, name_source, merge_type)],
              by = c('date', 'issue_id'),
              all.x = TRUE)

print(sprintf('After using all sources of gvkey merge we are able to merge %.2f%% of the data merge fixes.',
              base[ ,mean(!is.na(gvkey))]*100)) # 62.92%

#=========================================================================#
# Step 6: Merge unmatched using cleaned names ####
#=========================================================================#
# 0. Identify in base the issuer_ids where there is any month unmatched (add issuer_id to)
unmatched_issue_id <- base[is.na(gvkey),
                           .(.N, fi_start = min(date), fi_end = max(date)),
                           by = .(issue_id, issuer_id, fisd_name)]

# check: issuer_id:fisd_name is 1:m if the two dimensions below are equal. they are.
unmatched_issue_id[,.(N = uniqueN(fisd_name)), by =issuer_id][N>1]

# use issuer_ids in the bond issuance data from issuer_ids above: so this is issue-level data
# create an issuer_id - level dataset with start_fi and end_fi
unmatched_fisd <- unmatched_issue_id[ ,.(fi_start = min(fi_start),
                                         fi_end = max(fi_end)),
                                      by = .(issuer_id, fisd_name)]
dim(unmatched_fisd) # 6835 - increased a bit because of the valid matches.

# there are no duplicates by issuer_id in this dataset
unmatched_fisd[, duplink := .N, by = .(issuer_id)]
#unmatched_fisd[,duplink:=.N, by = .(fisd_name)]
unmatched_fisd[duplink > 1, .(issuer_id, fi_start, fi_end, fisd_name)]
# no, we are good to go

# 1. Merge unmatched_fi with hist_names on full fisd_name / hname = string_match0
# -------------------------------------------------------------------------------- #
# create linking key
nrow(unmatched_fisd) # 7467
unmatched_fisd[, link_name := fisd_name]
names_hist[, link_name := hname]
setkey(unmatched_fisd, 'link_name')
setkey(names_hist, 'link_name')

# string_match1 so that start_fi >= first_cc (otherwise, delete) / fi_start <= last_cc (otherwise, delete) - keep a range for valid merges
string_match0 <- unmatched_fisd[,.(issuer_id, link_name, fi_start, fi_end)][
  names_hist[,.(gvkey, start_date, end_date, link_name, hname)],
  on = .(link_name), nomatch=0]
string_match0[, c('link_start', 'link_end') := .(pmax(fi_start, start_date), pmin(fi_end, end_date))]
string_match0 <- string_match0[link_start<=link_end]

# find duplicates, so issuer_id:gvkey is m:1. why are there duplicates created here?
# the same hname can have multiple gvkeys  - these are very few cases
# if there are issuer_id:gvkey is m:1, then keep the issuer_id with the later first_cc
string_match0[, duplink := .N, by = .(issuer_id)]
string_match0[duplink > 1, .(issuer_id, gvkey, fi_start, fi_end, link_name)]
string_match0 <- clean_map(string_match0[,.(issuer_id, gvkey, link_start, link_end)],
                           id_vars = c('issuer_id', 'gvkey', 'link_start', 'link_end'))
# because of padding, clean map might create links that don't really exist. 
# we take care of this in the end.

## 1b. save successful matches, and unmatched_fi2 = leftover
unmatched_fi2 <- unmatched_fisd[!(issuer_id%in%string_match0$issuer_id)]
nrow(unmatched_fi2) # 5903

# 2. Merge unmatched_fi with hnames on full fisd_name / hconml = string_match1
# -------------------------------------------------------------------------------- #
nrow(unmatched_fi2)
unmatched_fi2[,link_name := clean_company_names2(fisd_name)]
names_hist[,link_name := clean_company_names2(hname)]
setkey(unmatched_fi2, 'link_name')
setkey(names_hist, 'link_name')

# string_match1 so that start_fi>=first_cc (otherwise, delete) / fi_start<= last_cc (otherwise, delete) - keep a range for valid merges
string_match1 <- unmatched_fi2[,.(issuer_id, link_name, fisd_name, fi_start, fi_end)][
  names_hist[,.(gvkey, link_name, hname, start_date, end_date)],
  on = .(link_name),
  nomatch=0]
string_match1[, c('link_start', 'link_end') :=.(pmax(fi_start, start_date), pmin(fi_end, end_date))]
string_match1 <- string_match1[link_start <= link_end]

# find duplicates, so issuer_id:gvkey is m:1
string_match1[,duplink:=.N, by = .(issuer_id)]
string_match1[duplink > 1,.(issuer_id, gvkey, fi_start, fi_end, link_name, hname, fisd_name)]
string_match1 <- clean_map(string_match1[,.(issuer_id, gvkey, link_start, link_end)], 
                           id_vars = c('issuer_id', 'gvkey', 'link_start', 'link_end'))

## 2b. save successful matches, and unmatched_fi2 = leftover
unmatched_fi3 <- unmatched_fi2[!(issuer_id%in%string_match1$issuer_id)]
nrow(unmatched_fi3) # 5740

# only unmatched issuer_ids make it to the next step; all gvkeys continue on

# 3. Fuzzy matching (see https://www.youtube.com/watch?v=RibBUG9Zuvs)
# -------------------------------------------------------------------------------- #
# define the from_vec (strings I want to merge: cleaned FISD names) 
from_vec = unique(unmatched_fi3$link_name)

# define the to_vec (strings I want to match to: cleaned Compustat CRSP Names)
to_vec = unique(names_hist$link_name)
from_vec <- from_vec[!(from_vec%in%to_vec)] # these have already been looked at in the previous merge
length(from_vec) # 3511

# find the best merges based on polyfuzz
my_model <- POLYFUZZ$PolyFuzz("EditDistance")$match(from_vec,to_vec)$get_matches()
nrow(my_model) # 3511
head(my_model, 100)

# keep above some threshold
matches_keep <- my_model[my_model$Similarity>0.9,]
matches_keep <- data.table(matches_keep)
matches_keep[nchar(From) <= 4 | nchar(To) <= 4]
colnames(matches_keep)[1] <- "link_name"   

# add the compustat cleaned names to unmatched_fi3
unmatched_fi3_matches <- unmatched_fi3[,.(issuer_id, link_name, fisd_name, fi_start, fi_end)][
  matches_keep[,.(link_name, To, Similarity)],
  on = .(link_name),
  nomatch=0]

# add gvkey
string_match2 <- unmatched_fi3_matches[,.(issuer_id, link_name, fisd_name, fi_start, fi_end, To)][
  names_hist[,.(gvkey, hname, start_date, end_date, To = link_name)],
  on = .(To),
  nomatch=0]
string_match2[, c('link_start', 'link_end') :=.(pmax(fi_start, start_date), pmin(fi_end, end_date))]
string_match2 <- string_match2[link_start <= link_end]
nrow(string_match2) # 449 

# find duplicates, so issuer_id:gvkey is m:1
string_match2[,duplink:=.N, by = .(issuer_id)]
string_match2[duplink>1,.(issuer_id, gvkey, fi_start, fi_end, link_name, hname, fisd_name)]
string_match2 <- clean_map(string_match2[, .(issuer_id, gvkey, link_start, link_end)], 
                           id_vars = c('issuer_id', 'gvkey', 'link_start', 'link_end'))
nrow(string_match2) # 440

## 2b. save successful matches, and unmatched_fi2 = leftover
unmatched_fi4 <- unmatched_fi3[!(issuer_id%in%string_match2$issuer_id)]
nrow(unmatched_fi4) #  5385

# 3. Add the fuzzy string merges
# -------------------------------------------------------------------#
# Add together the merged ones, check that they are issuer_id unique
string_matches <- rbind(string_match0[,.(issuer_id, gvkey, link_start, link_end)], 
                        string_match1[,.(issuer_id, gvkey, link_start, link_end)])
nrow(string_matches) # 2226
string_matches <- rbind(string_matches, string_match2[,.(issuer_id, gvkey, link_start, link_end)])
string_matches[,uniqueN(issuer_id)] #2082
string_matches[,uniqueN(gvkey)] #1641
nrow(string_matches) #2666

string_map <- clean_map(string_matches, 
                        id_vars = c('issuer_id', 'gvkey', 'link_start', 'link_end'))

# Go back to the issue id level and make sure there are no duplicates
# -------------------------------------------------------------------#
strbase <- merge(base,
                 string_map[,.(issuer_id, strgvkey = gvkey, link_start, link_end)],
                 by = c('issuer_id'),
                 allow.cartesian = TRUE)
strbase <- strbase[date >= link_start]
strbase <- strbase[date <= link_end]
strbase <- strbase[,.(issue_id, date, strgvkey)]
nrow(strbase)

# add hname
strbase <- merge(strbase, names_hist, by.x = 'strgvkey', by.y = 'gvkey', allow.cartesian = TRUE)
strbase <- strbase[date >= start_date]
strbase <- strbase[date <= end_date]
nrow(strbase)
strbase[is.na(hname)]

print(sprintf('After the string merge we created %.2f%%  of duplicates.',
              mean(duplicated(strbase, by = c('issue_id', 'date')))*100))
strbase <- unique(strbase, by = c('issue_id', 'date'))





lbase
strbase


# Final merge
# -------------------------------------------------------------------#
base <- merge(base,
              strbase[,.(issue_id, date, strgvkey, shname = hname)],
              by = c('date', 'issue_id'),
              all.x = TRUE)

base[!is.na(strgvkey) & is.na(gvkey), c('gvkey', 'hname', 'merge_type') := .(strgvkey, shname, 'str_link')]

print(sprintf('After merging on cleaned string name and with fuzzy string matching, we were able to merge %.2f%% of entries.',
              base[ ,mean(!is.na(strgvkey))]*100)) # 31.05%
print(sprintf('After merging on cleaned string name and with fuzzy string matching, we were able to merge %.2f%% of entries.',
              base[ ,mean(!is.na(gvkey))]*100)) # 70.21% 

base[, c('strgvkey', 'shname') := NULL]

#=========================================================================#
# Fix wrong merges ####
#=========================================================================#
# all our merge was made at issue_id level.
# We should now consider that one issuer_id should only be linked to one GVKEY at a time.
base <- base[order(issuer_id, date, issue_id)]

# Map has to go from issue_id to issuer_id
ibase <- unique(base[!is.na(gvkey),.(date, issuer_id, gvkey, fisd_name, hname, merge_type)])
ibase <- merge(ibase, rank, by = 'merge_type', all.x = TRUE)
ibase <- ibase[order(issuer_id, date, merge_rank)]

# Some have same gvkey, deleting is harmless 
ibase <- unique(ibase, by = c('issuer_id', 'date', 'gvkey'))
ibase

# Make sure the link is valid (valid compustat entry)
vbase <- merge(ibase,
               comp_range[, .(gvkey, first_cc, last_cc)],
               all.x = TRUE,
               by = 'gvkey')
vbase <- vbase[date >= first_cc & date <= last_cc]
vbase <- unique(vbase[, .(issuer_id, date, gvkey, fisd_name, hname, merge_type, merge_rank)], by = c('issuer_id', 'date', 'gvkey'))

print(sprintf('After all merges we created %.2f%% of duplicates.',
              sum(duplicated(vbase, by = c('date', 'issuer_id')))/nrow(lbase)*100))

#Use name distance to exclude duplicates in case of exact merge
vbase[, dup := .N, by = .(issuer_id, date)]

# Exclude entries for which there is at least one perfect merge
vbase[dup > 1, ndist := stringdist(hname, fisd_name)]
vbase[dup > 1, mindist:= min(ndist), by = .(issuer_id, date)]
cbase <- copy(vbase[!(dup > 1 & mindist==0 & ndist > 0)])
cbase[, c('dup', 'ndist', 'mindist') := NULL]

# Clean name for suffixes and keep if one perfect merge 
cbase[, dup := .N, by = .(issuer_id, date)]
cbase[dup > 1, ndist2 := stringdist(clean_company_names2(hname), clean_company_names2(fisd_name))]
cbase[dup > 1, mindist:= min(ndist2), by = .(issuer_id, date)]
cbase <- copy(cbase[!(dup > 1 & mindist==0 & ndist2 > 0)])#[issue_id == 467368 & date == '2008-06-30']
cbase[,table(dup)]
cbase[, c('dup', 'ndist2', 'mindist') := NULL]

# Add hname to take care of duplicates
print(sprintf('After names cleaning we still have %.2f%% of duplicates.',
              sum(duplicated(cbase, by = c('date', 'issuer_id')))/nrow(lbase)*100))

# Exclude based on rank
cbase <- cbase[order(issuer_id, date, merge_rank)]
cbase <- unique(cbase, by = c('issuer_id', 'date'))

# Clean map
cbase[,chgvkey := (gvkey != shift(gvkey)), by = issuer_id]
cbase[,chtype := (merge_type != shift(merge_type)), by = issuer_id]

cbase[is.na(chgvkey), chgvkey := TRUE]
cbase[is.na(chtype),  chtype := TRUE]
cbase[,id := cumsum(chgvkey|chtype), by = issuer_id]

# NEED TO CHECK THIS! Many ids means that there can be jumping links.
# ASK KERRY FOR A SUGESTION!
cbase[id>10]

temp <- cbase[,.(link_start = min(date), 
                 link_end = max(date)),
              by = .(issuer_id, gvkey, merge_type, id)][order(issuer_id, link_start)]

base_map <- clean_map(temp, 
                      id_vars = c('issuer_id', 'gvkey', 'link_start', 'link_end'),
                      link_source = 'merge_type')
rm(temp)

# Merge back to base
temp <-  merge(base[,.(issue_id, issuer_id, date)], 
               base_map, 
               by = 'issuer_id', 
               allow.cartesian = TRUE)
temp[is.na(link_start)|is.na(link_end)]
temp <- temp[date>=link_start&date<=link_end]
if(any(duplicated(temp[,.(date, issue_id)]))){warning('Duplicate detected. Need to check!')}

# Create final table
base <- merge(base[,.(date, issue_id, issuer_id, agent_id, parent_id,
                      isin, cusip, issuer_cusip, cusip_name, fisd_name, legal_name,
                      parent_legal_name, offering_date, maturity, bond_type, security_level,
                      amt_outstanding)], 
              temp[ ,.(date, issue_id, gvkey, merge_type)],
              by = c('date', 'issue_id'),
              all.x = TRUE)
base <- merge(base, comp_names[,.(gvkey, conm, comp_tic = tic, comp_naics = naics)], by= 'gvkey', all.x = TRUE)
base[,link_date:=date]
base <- names_hist[,.(gvkey, hname, start_date, end_date)][ 
  base, 
  on = .(gvkey = gvkey, start_date<=link_date, end_date>=link_date)]

# Need to fix this!
# base[is.na(hname)&!is.na(gvkey)][,.(date, issue_id, issue_id, gvkey, hname, conm)]
# base[,mean(is.na(hname)&!is.na(gvkey))]
# nrow(base[is.na(hname)&!is.na(gvkey)])/nrow(base)
# names_hist[gvkey=='001013']
# comp_range[gvkey=='001013']
# View(base[gvkey=='001013'][,.(date, issue_id, issue_id, gvkey, hname, conm)])
print(sprintf('After taking care of wrong merges we have valid gvkey for %.2f%% of base.',
              base[,mean(!is.na(gvkey))]*100)) # 85.94%
if(any(duplicated(base[,.(date, issue_id)]))){warning('Duplicate detected. Need to check!')}

#=========================================================================#
# Some Checks ####
#=========================================================================#
issuer[issuer_id%in%base[is.na(gvkey), unique(issuer_id)], table(country_domicile)]
issuer[issuer_id%in%base[is.na(gvkey), unique(issuer_id)], table(country)]

#=========================================================================#
# Write Data ####
#=========================================================================#
if(nrow(base) == nrows_base){
  'We finished with the right number of line.'
}else{'We finished with the WRONG number of line.'}


db_write(dbs$merges, connectArgs$merges, "corp_amt_outstanding_cmerged", base, pk = c("issue_id", "date"))
check_db(dbs$merges, "corp_amt_outstanding_cmerged", base, sort_cols = c("issue_id", "date"))

db_write(dbs$link_tables, connectArgs$link_tables, "map_issuer_id_gvkey", base_map,  pk = c("issuer_id", "gvkey", "link_start"))
check_db(dbs$link_tables, "map_issuer_id_gvkey", base_map, sort_cols = c("issuer_id", "gvkey", "link_start"))

names_hist$link_name[names_hist$link_name == ""] <- NA

db_write(dbs$merges, connectArgs$merges, "names_hist", names_hist)
check_db(dbs$merges, "names_hist", names_hist, sort_cols = c("gvkey", "link_name", "start_date"))

print(sprintf('We have successfully created %.2f%% of issue_id and gvkey valid entries.',
              base[,mean(!is.na(gvkey))]*100))

print('Final amount outstanting table was created.')
print('Final map from issuer id to gvkey was created.')
