# ================================================================= #
# fisd_bond_rating.R ####
# ================================================================= #
# Description:
# ------------
#   This code merges amount outstanding data with ratings.
#   Use historical bond ratings: meta Ratings r_sp > r_mr > r_fr
#   Create numeric rating (higer rating with higher ranking) and NAICS rating 
#
# Input(s):
# ------------
#   Dropbox FISD data:
#   1. data/processed/fisd/corp_amt_outstanding
#   2. data/raw/fisd/fisd_rating_hist
#   3. data/raw/fisd/fisd_ratings
#
# Output(s) :
# ------------
#   Dropbox data/processed/
#
# Date:
# ------------
#   2022-05-13
#   update: 2026-01-22
#
# Author(s):
# ------------
#   Lira Mota Mertens, liramota@mit.edu
#   Ruiquan Chang, chang.2590@osu.edu
#
# Additional note(s):
# ------------
# Rank of rating
# [[1]] "D"
# [[2]] "CA" "CC" "C" 
# [[3]] "CAA3" "CCC-"
# [[4]] "CAA2" "CCC" 
# [[5]] "CAA"  "CAA1" "CCC+"
# [[6]] "B3" "B-"
# [[7]] "B2" "B" 
# [[8]] "B1" "B+"
# [[9]] "BA3" "BB-"
# [[10]] "BA2" "BB" 
# [[11]] "BA"  "BA1" "BB+"
# [[12]] "BAA3" "BBB-"
# [[13]] "BAA2" "BBB" 
# [[14]] "BAA1" "BBB+" "BAA" 
# [[15]] "A3" "A-"
# [[16]] "A2" "A" 
# [[17]] "A1" "A+"
# [[18]] "AA3" "AA-"
# [[19]] "AA2" "AA" 
# [[20]] "AA1" "AA+"
# [[21]] "AAA"
# [[22]] NR
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
library(lubridate)

# Source helper scripts
source('utils/setPaths.R')


# ================================================================= #
#  Read Data ####
# ================================================================= #
acorp <- readRDS(paste0(PROCDIR, "fisd/corp_amt_outstanding.rds"))
acorp <- acorp[order(issue_id, date)]

# History
lrat <- readRDS(paste0(RAWDIR, "fisd/fisd_ratings_hist.rds"))
lrat[rating_date == "2202-09-02", rating_date := as.Date("2020-09-02")]

# Current
crat <- readRDS(paste0(RAWDIR, "fisd/fisd_ratings.rds"))
crat[, c('rating_status_date', 'investment_grade') := NULL]


# ================================================================= #
#  Append history with most recent ####
# ================================================================= #
lrat[, .N, by = .(issue_id, rating_date, rating_type)][order(-N)]

lrat[issue_id == 1]
crat[issue_id == 1042557]
lrat[issue_id == 1042557]

lrat <- rbind(lrat, crat, fill = TRUE)
lrat <- unique(lrat)


# ================================================================= #
#  Clean ####
# ================================================================= #
# When a rating agenct withdraws a ratings, we should not consider this rating any longer. 
lrat[reason == 'WITH', table(rating, useNA = 'always')]
lrat[reason == 'WITH', rating := 'NR']

lrat[duplicated(lrat, by = c('issue_id', 'rating_date', 'rating_type', 'rating'))]
lrat <- unique(lrat, by = c('issue_id', 'rating_date', 'rating_type', 'rating') )
lrat <- lrat[rating != 'NR']

# what to do with duplicates?
lrat[, .N, by = .(issue_id, rating_date, rating_type)][order(-N)]
lrat <- lrat[order(issue_id, rating_date, rating_type, rating, rating_status)]
lrat <- unique(lrat, by = c('issue_id', 'rating_date', 'rating_type') )

# dcast in rating type
rat <- dcast.data.table(lrat, issue_id + rating_date ~ rating_type, value.var = 'rating')
setnames(rat, 'rating_date', 'date')


# ================================================================= #
# Merge with amt_out ####
# ================================================================= #
rtypes_list <- c('DPR', 'FR', 'MR', 'SPR')
rcorp <- copy(acorp)

for(i in rtypes_list){
  srat <- rat[,c('issue_id', 'date', i), with = FALSE]
  setnames(srat, i, 'rtype')
  srat <- srat[!is.na(rtype)]
  srat <- srat[order(issue_id, date)]
  
  # Merge
  rcorp <- srat[rcorp, on = .(issue_id, date), roll=TRUE]

  # Set name
  setnames(rcorp, 'rtype', i)
}


# ================================================================= #
# Checks ####
# ================================================================= #
rcorp[, mean(!is.na(SPR))]  # 0.6471103
rcorp[, mean(!is.na(MR))]  # 0.691892
rcorp[, mean(!is.na(FR))]  # 0.3419825
rcorp[, mean(!is.na(DPR))]  # 0.03024016

lrat[, lastdate := max(rating_date), by = issue_id]
lrat[reason == 'WITH' & rating_date < lastdate]
lrat[issue_id==15672 & rating_type=='MR']
rcorp[issue_id == 15672 & date>='2003-10-10']


# ================================================================= #
# Create Ratings Classification ####
# ================================================================= #
# Create Meta Ratings: r_sp > r_mr > r_fr
rcorp[, mr := SPR]
rcorp[mr == 'NR' | is.na(mr), mr := MR]
rcorp[mr == 'NR' | is.na(mr), mr := FR]
rcorp[, mr := toupper(mr)]
rcorp[, table(mr)]

# Create Raiting rank
## Do ot include D or P-1 since it means the bond in default
r_rank <- list(c('AAA'),
               c('AA1', 'AA+'),
               c('AA2', 'AA'),
               c('AA3', 'AA-'),
               c('A1', 'A+'),
               c('A2', 'A'),
               c('A3', 'A-'),
               c('BAA1', 'BBB+', 'BAA'),
               c('BAA2', 'BBB'),
               c('BAA3', 'BBB-'),
               c('BA', 'BA1', 'BB+'),
               c('BA2', 'BB'),
               c('BA3', 'BB-'),
               c('B1', 'B+'),
               c('B2', 'B'),
               c('B3', 'B-'),
               c('CAA', 'CAA1', 'CCC+'),
               c('CAA2', 'CCC'),
               c('CAA3', 'CCC-'),
               c('CA', 'CC', 'C'),
               c('D'))

# Higher rating higher ranking
length(r_rank)
r_rank <- rev(r_rank)

rcorp[,rating_rank := numeric()]
for (i in c(1:length(r_rank))) {
  rcorp[mr%in%r_rank[[i]], rating_rank := i]
}
rcorp[is.na(mr) | mr == 'NR', rating_rank := i+1]
table(rcorp$rating_rank)

# Create Naics Rating Code
naics_labels <- c('NAIC1 (AAA/AA)','NAIC1 (A)', 
                  'NAIC2 (BBB)','NAIC3 (BB)', 
                  'NAIC4 (B)', 'NAIC5 (CCC)', 
                  'NAIC6 (bCC)', 'NR')
naics_names <- c('NAIC1_AA','NAIC1_A', 
                 'NAIC2','NAIC3', 
                 'NAIC4', 'NAIC5', 
                 'NAIC6', 'NR')

rcorp[mr%in%c('AAA', 'AA+','AA','AA-', 'AA1', 'AA2', 'AA3'), naics := naics_names[1]]
rcorp[mr%in%c('A+','A', 'A-', 'A1', 'A2', 'A3'),naics := naics_names[2]]
rcorp[mr%in%c("BBB+", "BBB", "BBB-", 'BAA1', 'BAA2',  'BAA3', 'BAA'), naics := naics_names[3]]
rcorp[mr%in%c(c("BB+", "BB", "BB-",  "BA1", "BA2", "BA3", 'BA')), naics := naics_names[4]]
rcorp[mr%in%c("B+", "B", "B-", "B1", "B2", "B3"), naics := naics_names[5]]
rcorp[mr%in%c("CCC+", "CCC", "CCC-", "CAA1", "CAA2", "CAA3", 'CAA'), naics := naics_names[6]]
rcorp[mr%in%c("CC", "C", "D", "CA", "C"), naics := naics_names[7]]
rcorp[mr=='NR'|is.na(mr), naics := naics_names[8]]

rcorp[, naics := as.character(factor(rcorp$naics, levels = naics_names))]
setnames(rcorp, c('SPR', 'MR', 'FR', 'DPR'), c('r_sp', 'r_mr', 'r_fr', 'r_dp'))


# ================================================================= #
# Write Data ####
# ================================================================= #
if(any(duplicated(rcorp[, .(issue_id, date)]))){
  stop('Data primary key violated.')
}
range(rcorp$date)  # "1902-03-31" "2122-04-15"

saveRDS(rcorp, file = paste0(PROCDIR, 'fisd/corp_ratings.rds'))
print('Successfully created final ratings table.')



