#=======================================================================================#
# link_isuer_id_redcode.R ####
#=======================================================================================#
# Description:
# -------------
#  Merge issuer_id to redcode (issuer_id centered)
#  One redcode can have many issuer_ids (for example if the company changes names)
#  But one issuer_id can only have on redcode (one obligation triggers only one CDS)
#
# Input(s):
# -------------
# FISD
# -------------
#   1. fisd/corp_amt_outstanding_cmerged
#   2. fisd_mergedissuer
#
# Markit CDS
# -------------
#   1. markit.cds_us
#   2. markit.redent
#   3. markit.redentcorpact
#   4. markit.redobl
#   5. markit.redobllookup
#   6. markit.redoblrefentit
#   7. markit.missing_cusips_filled
#
#  Link tables
# -------------
#   1. link_tables.redcode_cusip6_manual
#
# Output(s):
# -------------
#   Dropbox data/processed:
#   1. merges.corp_amt_c_cdsus_merged
#   2. merges.map_issuerid_centered_redcode
#
# Date:
# -------------
#   2023-05
#   update: 2026-01-23
#
# Author(s):
# ----------
#   Lira Mota, liramota@mit.edu
#   Nicholas von Turkovich
#   Ruiquan Chang, chang.2590@osu.edu
#
# Additional note(s):
# ---------------------
# Notice that cds con be issued at the subsidiary level.
# Subsidiary have different risk than the parent - important to maintain the correct level.
# 
# The decision tree follows:
# 1. Merge using red obligation map
# 2. Merge using red_cusip and issuer_cusip
# 3. Try company names
#
# ------- -
# CAREFUL:
# -------- -
# This code has some hand fixes. Any thing that change the inputs requires going over the 
# hand fix.
#
# Markit redcode naming convention is very tricky.
# Sometimes a company changes name in compustat, but it does not in Markit
# Ex. AT&T (redcode 001AEC)
# 
# ================================================================= #

# ================================================================= #
# Environment ####
# ================================================================= #
# Clear workspace
rm(list = ls())

# Import libraries
library(here)
library(data.table)
library(lubridate)
library(stringdist)

# Source helper scripts
source(here::here("utils/setPaths.R"))
source("utils/clean_company_names0.R")
source("utils/clean_map.R")


# ================================================================= #
# Parameters ####
# ================================================================= #
# Set date range for linking
byear = 2000
eyear = 2025

# Set seed for random checks
set.seed(2023)

# Manually override CUSIP6s for select redcodes
override_cusip6 = FALSE


# ================================================================= #
# Read Data ####
# ================================================================= #
# Load FISD data on bond issuance
amt_out = readRDS(paste0(DATADIR, 'processed/fisd/corp_amt_outstanding_cmerged.rds'))
issuer = readRDS(paste0(DATADIR, 'raw/fisd/fisd_mergedissuer.rds'))

# Load CDS data from Markit
cds = readRDS(paste0(DATADIR, 'processed/markit/cds/cds_us.rds'))

# Load entity data from Markit
redent = readRDS(paste0(DATADIR, 'raw/markit/cds/redent.rds'))

# Load corporate actions data to track name changes
corpact = readRDS(paste0(DATADIR, 'raw/markit/cds/redentcorpact.rds'))

# Load information on obligations associated with particular redcodes
redobl = readRDS(paste0(DATADIR, 'raw/markit/cds/redobl.rds'))
redobllookup = readRDS(paste0(DATADIR, 'raw/markit/cds/redobllookup.rds'))
redoblrefentity = readRDS(paste0(DATADIR, 'raw/markit/cds/redoblrefentity.rds'))

# Missing cusips collected by hand
miss_cusip = fread(paste0(DATADIR, 'processed/markit/cds/missing_cusips_filled.csv'))

# Manually override CUSIP6s from Markit
redcode_cusip_manual = fread(paste0(DATADIR, 'processed/link_tables/redcode_cusip6_manual.csv'))
redcode_cusip_manual$red_cusip_manual = str_pad(redcode_cusip_manual$red_cusip_manual, 
                                                width = 6,
                                                side = "left",
                                                pad = "0")
# ================================================================= #
# Clean FISD bond issuance data ####
# ================================================================= #
# Isolate debt issue observations within year range
base = amt_out[year(date) >= byear & year(date) <= eyear, 
               .(date, issue_id, issuer_id, parent_id, cusip, issuer_cusip,
                 fisd_name, hname, cusip_name, legal_name, parent_legal_name,
                 gvkey)][order(issue_id, date)][,mdate := year(date)*100+month(date)]

# ============================================================================ #
# Create map between CDS data and valid redcode-cusip links from Markit ####
# ============================================================================ #
# Clean company names in CDS data
setnames(cds, c('ticker', 'shortname'), c('red_ticker', 'red_name'))
cds[, red_name := clean_company_names0(red_name)]

# Adjust column names and subset entity data
redent = redent[ ,.(redcode, red_cusip = entity_cusip, red_ticker = ticker,
                    red_name = clean_company_names0(shortname), validfrom, validto)]

# Merge in the company information for each CDS contract
cds_base <- merge(cds[!is.na(redcode)],
                  redent[!is.na(redcode), .(redcode, red_cusip, validfrom, validto)],
                  by = c('redcode'),
                  all.x = TRUE)

# Remove linked CUSIPs if the date of the contract falls outside valid link range
cds_base[date < validfrom | date > validto, c("red_cusip", "valid_from", "valid_to") := NA]

# Remove duplicates due to tenor of contract
cds_base <- unique(cds_base[,.(redcode, mdate, date, red_ticker, red_name, red_cusip)])

# Some checks 
sprintf('CUSIP6 information is missing for %.2f %% of the redcode/month observations.', 
        cds_base[,mean(is.na(red_cusip))]*100)

# At this point, cds_base includes for each date-redcode pair a valid CUSIP6 and metadata

# ====================================================================== #
# Using cds_base create map with link ranges instead of full panel ####
# ====================================================================== #
# Keep only the first and last date for each entity-metadata pair
cds_names = cds_base[,.(first_date = min(date),
                        last_date = max(date)),
                     by = .(redcode, red_cusip, red_ticker, red_name)]

if (override_cusip6){
  # Merge in override CUSIP6 values
  cds_names = merge(cds_names,
                    redcode_cusip_manual,
                    by = "redcode",
                    all.x = T)
  
  # Force override
  cds_names[!is.na(red_cusip_manual), red_cusip := red_cusip_manual]
  cds_names[,red_cusip_manual := NULL]
}

# ========================================================================= #
# Matching strategy 1: using obligations (CUSIP9s) linked to redcodes ####
# ========================================================================= #
# Clean the obligation information linking individual CUSIP9s to the redcode it represents
redo = redoblrefentity[!is.na(redcode),
                       .(redcode, obl_cusip, role, pairvalidfrom, pairvalidto,
                         ispreferred, preferreddate)]

# Merge obligation data into CDS contract data; this yields for every CDS redcode-
# date (possibly) multiple matches to debt obligations
cobl = merge(cds_base, 
               redo, 
               by = "redcode",
               allow.cartesian = T)

# Keep only the debt obligation for a CDS contract if the date of the CDS falls within
# the pair date for the obligation
cobl = cobl[(date >= pairvalidfrom | is.na(pairvalidfrom)) & 
                  (date <= pairvalidto | is.na(pairvalidto))]

# Remove if linked obligation CUSIP is NA
cobl = cobl[!is.na(obl_cusip)]

# Remove if linked oblication CUSIP is a "dummy"
cobl = cobl[obl_cusip!='DUMYREFOB']

# At this point, we have a dataset of unique (date, redcode, obl_cusip) triples
if(any(duplicated(cobl, by = c("redcode", "date", "obl_cusip")))){
  stop("Duplicate (date, redcode, obl_cusip) triples")
}

# This filters out any duplicate observations due to repeated information but 
# for multiple dates (not an important filter as those records effectively the same)
cobl = unique(cobl[,.(mdate, obl_cusip, redcode, red_name, red_ticker, 
                      role, ispreferred, pairvalidfrom, pairvalidto)])

# At this point, we have a dataset of unique (mdate, redcode, obl_cusip) triples
if(any(duplicated(cobl, by = c("redcode", "mdate", "obl_cusip")))){
  stop("Duplicate (mdate, redcode, obl_cusip) triples")
}

# Order the data by redcode, then date, then from earliest to latest end date link
# keeping NAs at the end
cobl <- cobl[order(redcode, mdate, pairvalidto, na.last=TRUE)]

# Goal is to merge this into the FISD data and then at the issuer_id - mdate level, 
# find a unique redcode based on a sequence of de-duplication steps

# Merge the monthly dataset of redcodes for each CUSIP9 into the FISD data (introduces
# duplicates as a single (obligation, mdate) duple could have multiple associated
# redcodes)
map_obl = merge(base, 
             cobl,
             by.x = c("cusip", "mdate"),
             by.y = c("obl_cusip", "mdate"),
             allow.cartesian = T)

# Keep links to redcodes only if the date of the FISD observation lies within the link range
map_obl = map_obl[(date >= pairvalidfrom | is.na(pairvalidfrom)) & 
                  (date <= pairvalidto | is.na(pairvalidto))]

# There will be duplicates at the (issuer_id, mdate) level. The goal is to associate
# to each (issuer_id, mdate) a single redcode. We proceed with several de-duplication
# steps and then verify that no more than one redcode is attached to each duple

# If there are duplicates present AND a link is preferred, keep that link
map_obl[,upref := uniqueN(ispreferred), by = .(mdate, issuer_id)]
map_obl = map_obl[upref == 1 | (upref == 2 & ispreferred == 't')][,upref := NULL]

# If there are duplicates present AND both Guarantor and Issuer are available
map_obl[,urole := uniqueN(role), by = .(mdate, issuer_id)]
map_obl = map_obl[urole == 1 | (urole == 2 & role == "Issuer")][,urole := NULL]

# Check to ensure a clean mapping
if(any(duplicated(map_obl, by = c("issuer_id", "mdate")))){
  sprintf("Remember: Merge strategy on obl_cusip still yielding duplicates")
}

# At this point, each issuer_id and mdate pair is unique and a column of redcodes
# exists. Now, we want to create continuous date ranges for each issuer_id. The steps
# are: 
# 1) Take the minimum/maximum valid pairdates for each (issuer_id, redcode) pair,
# 2) For each pair, join in the metadata on the first/last date the redcode is valid/
# appears in the CDS data and create a conservative range. 

# For each issuer and redcode pair, define the date range over which the pair is
# valid
map_obl = map_obl[,.(pairvalidfrom = min(pairvalidfrom),
                         pairvalidto = max(pairvalidto)),
                      by = .(issuer_id, redcode)]

# Merge in the date ranges and metadata from cds_names and then take conservative 
# range
map_obl = merge(map_obl,
                cds_names,
                by = "redcode")
map_obl[, c('lcds_start', 'lcds_end') := .(first_date, last_date)]
map_obl[lcds_start < pairvalidfrom, lcds_start := pairvalidfrom]
map_obl[lcds_end > pairvalidto, lcds_end := pairvalidto]
map_obl = map_obl[lcds_start<=lcds_end]
map_obl = merge(map_obl, issuer[,.(issuer_id, cusip_name)], by = 'issuer_id')

# Order links according to issuer_id and when the link starts
map_obl = map_obl[order(issuer_id, lcds_start), .(issuer_id, redcode, red_cusip, 
                                                  red_ticker, red_name, cusip_name,
                                                  lcds_start, lcds_end)]

# Merge into base
map_obl_base = map_obl[,.(issuer_id, redcode, red_name, lcds_start, lcds_end)][base, on = .(issuer_id), allow.cartesian = TRUE]
map_obl_base = map_obl_base[date >= lcds_start & date <= lcds_end]

# ================================================================= #
# Matching strategy 2: using issuer CUSIPs (CUSIP6s) to link ####
# ================================================================= #
# One issue with red_cusips: as per email exchange with Markit, CUSIPs are never recycled. 
# If a redcode changed, it would get a new cusip - maybe a fake one. Ex. APPLE.
# New cusip won't map to other data bases. 
# redent[redcode %in% c('03AFB6', '03AFCJ')]

# Strategy: create new entries in cds_names with cusip6 inferred from the redo table.
# Our assumption is that obl_cusip6 is also a valid cusip6 for the redcode.

# Possible error 1: redcode mapping to wrong cusip -> obl_cusip was valid only for a limited period.
#   unlikely, because if there is a structural change in the company Markit assigns a new redcode.

# Also solves the issue that obl_map might break because of lack of eligible obl.
# -> The map will persist as long the redcode - red_cusip did not change.

redo_cusip_map = unique(redo[!is.na(obl_cusip) & 
                             !(obl_cusip %in% c('LPNLIST00', 'DUMYREFOB') &
                             role == 'Issuer'), 
                      .(redcode, obl_cusip6 = substr(obl_cusip, 1, 6))])

redo_cusip_map = merge(redo_cusip_map, redent, by = 'redcode')

# there are many cases that obl_cusip is different than red_cusip
redo_cusip_map[,mean(obl_cusip6 != red_cusip)]

redo_cusip_map = merge(cds_names,
                       redo_cusip_map[obl_cusip6 != red_cusip | is.na(red_cusip), .(redcode, obl_cusip6)],
                       by = 'redcode')

# extend cds_names
ext_cds_names = rbind(cds_names[!is.na(red_cusip)],
                      redo_cusip_map[, .(redcode, red_cusip = obl_cusip6, red_ticker, red_name, first_date, last_date)])

# Look at one example: cusip 037833 now lives until the end of the sample.
ext_cds_names[like(red_cusip, '0378')]

# Merge FISD data based on CUSIP6 with the cds_names (linking to redcodes) 
ccusip = merge(base,
               ext_cds_names[!is.na(red_cusip)],
               by.x = "issuer_cusip",
               by.y = "red_cusip")

# Keep only observations that fall within the valid redcode link range
ccusip = ccusip[date >= first_date & date <= last_date]

# Create map from issuance data with corresponding redcodes. For each issuer_id
# and redcode pair, find the date range present in the data (and keep additional
# metadata)
map_cusip = ccusip[,.(lcds_start = min(first_date),
                      lcds_end = max(last_date)),
                   by = .(issuer_id, redcode, red_ticker, cusip_name, red_name)]

# Order the links
map_cusip = map_cusip[order(issuer_id, lcds_start, lcds_end)]

# Merge into base
map_cusip_base = map_cusip[,.(issuer_id, redcode, red_name, lcds_start, lcds_end)][base, on = .(issuer_id), allow.cartesian = TRUE]
map_cusip_base = map_cusip_base[date >= lcds_start & date <= lcds_end]

# ================================================================= #
# Combine strategy 1 and 2: clean duplicates ####
# ================================================================= #
map_base <- unique(rbind(map_obl_base[, .(date, issuer_id, redcode, cusip_name, red_name, merge_type = 'obl')],
                         map_cusip_base[, .(date, issuer_id, redcode, cusip_name, red_name, merge_type = 'cusip')]),
                   by = c('date', 'issuer_id'))

# Among the duplicate links take the link with the most similar firm name
map_base[,sdist := stringdist(tolower(red_name), tolower(cusip_name))]
map_base = map_base[order(issuer_id, date, -sdist, na.last = T)]
map_base = unique(map_base, by = c("issuer_id", "date"), fromLast = T)
map_base[, c('cusip_name', 'sdist') := NULL]

# Merge to base 
base_merged = merge(base,
                    map_base, 
                    by = c('date', 'issuer_id'),
                    all.x = TRUE)
base_merged[,mean(!is.na(redcode))]

# ================================================================= #
# Matching strategy 3: using names to link ####
# ================================================================= #
# Create a list of FISD names
fisd_names = unique(base_merged[is.na(redcode),.(issuer_id, cusip_name, fisd_name)])

# Merge FISD data based on cusip_name with cds_names (linking to redcodes)
mcusip_names = merge(fisd_names,
                     cds_names,
                     by.x = "cusip_name",
                     by.y = "red_name",
                     all.x = TRUE)

# Merge FISD data based on cusip_name with cds_names (linking to redcodes)
mfisd_names = merge(mcusip_names[is.na(redcode), .(issuer_id, cusip_name, fisd_name)],
                    cds_names,
                    by.x = "fisd_name",
                    by.y = "red_name")

# Collect all merges based on names
cnames = rbind(mcusip_names, mfisd_names)

# Merge back into base and keep records within valid ranges
cnames = merge(unique(base_merged[is.na(redcode),.(issuer_id, date)]),
               cnames[,.(issuer_id, redcode, first_date, last_date)],
               by = c("issuer_id"),
               allow.cartesian = T)

# Filter out if date not within first and last date
cnames = cnames[date >= first_date & date <= last_date]

# As long as each issue is linked to a single redcode on a particular date then
# match is ok
cnames = unique(cnames, by = c("issuer_id", "date", "redcode"))

# Throw error if a particular issue-date is linked to more than one redcode
if(any(duplicated(cnames, by = c("issuer_id", "date")))){
  stop("Duplicate observations from merge on names")
}

base_merged = merge(base_merged,
                    cnames[,.(date, issuer_id, name_redcode = redcode)],
                    by = c('issuer_id', 'date'),
                    all.x = TRUE)

# ================================================================= #
# Assign redcode at the issue level ####
# ================================================================= #
# Order of assignment: by obligation or CUSIP6 (according to min name dist), than name
base_merged[is.na(redcode), merge_type := 'name']
base_merged[is.na(redcode), redcode := name_redcode]
base_merged[,mean(!is.na(redcode))]

# ================================================================= #
# Create final map at the issuer_id level ####
# ================================================================= #
# Find the widest date range present in the issue data for all (issuer_id, redcode)
final_map = base_merged[!is.na(redcode),
                        .(red_start = min(date), red_end = max(date)),
                        by = .(issuer_id, redcode, merge_type)]

# Merge with CDS info also aggregated to the maximum date range
final_map = merge(final_map,
                  cds_names[,.(cds_start = min(first_date), cds_end = max(last_date)), by = .(redcode)],
                  by = 'redcode',
                  all.x = T)

# Add issuer characteristics
issuer_char <- amt_out[,.(issuer_start = min(date), issuer_end = max(date)), 
                       by = .(issuer_id, cusip_name)][order(issuer_id)]

# Merge with issuer characteristics
final_map = merge(final_map,
                  issuer_char,
                  by = "issuer_id")

# Conservative link range by taking the latest start date and earliest end date
# across all data sources (FISD, CDS, base_merged)
final_map[,red_start := pmax(red_start, cds_start, issuer_start)]
final_map[,red_end := pmin(red_end, cds_end, issuer_end)]

# Clean map
final_map = clean_map(final_map[,.(issuer_id, redcode, red_start, red_end, merge_type)],
                      id_vars = c('issuer_id', 'redcode', 'red_start', 'red_end'),
                      link_source = 'merge_type')

# Merge with entity data
final_map = merge(final_map,
                  redent[,.(redcode, red_cusip, red_ticker, red_name)],
                  by = 'redcode')

# Merge first into base_merged
base_merged_map = merge(base,
                        final_map, 
                        by = "issuer_id")[date >= red_start & date <= red_end]


# Form base_merged_final
base_merged_final = merge(base,
                          base_merged_map[,.(issue_id, date, redcode, red_cusip, red_ticker, red_name, red_merge_type = merge_type)],
                          by = c("issue_id", "date"),
                          all.x = T)

# Check again for duplicates
if(any(duplicated(base_merged_final, by = c("issue_id", "date")))){
  stop("Duplicates in base_merged_final")
}

# Add information in amt_out
cds_base <- merge(amt_out, base_merged_final[,.(issue_id, date, redcode, red_cusip, red_ticker, red_name, red_merge_type)], 
                  by = c('issue_id', 'date'), 
                  all.x = TRUE)

if(any(duplicated(cds_base, by = c('issue_id', 'date')))){
  stop("Duplicates in final cds_base")
}

# ================================================================= #
# Write Data ####
# ================================================================= #
# Save issue id map
saveRDS(cds_base, paste0(DATADIR, 'processed/fisd/corp_amt_c_cdsus_merged.rds'))
saveRDS(final_map, paste0(DATADIR, 'processed/link_tables/map_issuerid_centered_redcode.rds'))

print('Successfully created cds_us_merged.')
