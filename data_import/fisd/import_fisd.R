# ================================================================= #
# import_fisd.R ####
# ================================================================= #
# Description:
# ------------
#   This downloads and saves FISD Issuance and Issuer raw data base from WRDS server.
#   It uses RPostgres direct download.
#
# Input(s):
# ------------
#   WRDS connection
#
# Output(s) :
# ------------
#   Dropbox FISD raw data:
#     1. data/raw/fisd/fisd_issue.rds
#     2. data/raw/fisd/fisd_mergedissuer.rds
#     3. data/raw/fisd/fisd_ratings.rds
#     4. data/raw/fisd/fisd_ratings_hist.rds
#     5. data/raw/fisd/fisd_amt_out_hist.rds
#     6. data/raw/fisd/fisd_amt_out.rds
#     7. data/raw/fisd/fisd_issuer_cusip.rds
#     8. data/raw/fisd/fisd_agent.rds
#     9. data/raw/fisd/fisd_redemption.rds
#
# Date:
# ------------
#   2022-05-11
#   update: 2026-01-22
#
# Author(s):
# ------------
#   Lira Mota Mertens, liramota@mit.edu
#   Ruiquan Chang, chang.2590@osu.edu
#
# Additional note(s):
# ----------
# FISD Variable Description: ####
# ------------------------------#
# 1 ISSUE_ID 	Num	8	A Mergent-generated number unique to each issue.
# 2	ISSUER_ID 	Num	8	A Mergent-generated number unique to each issuer.
# 3	PROSPECTUS_ISSUER_NAME 	Char	64	The name of the issuer as in the prospectus.
# 4 ISSUER_CUSIP 	Char	6	Issuer CUSIP.
# 5	ISSUE_CUSIP 	Char	3	Issue CUSIP
# 6	ISSUE_NAME 	Char	64	Issue type description as taken from the prospectus.
# 7	MATURITY 	Num	8	Date that the issue\'\'s principal is due for repayment.
# 8	SECURITY_LEVEL 	Char	4	Indicates if the security is a secured, senior or subordinated issue of the issuer.
# 9	SECURITY_PLEDGE 	Char	4	A flag indicating that certain assets have been pledged as security for the issue.
# 10	ENHANCEMENT 	Char	1	Flag indicating that the issue has credit enhancements.
# 11	COUPON_TYPE 	Char	4	The coupon type for the issue.
# 12	CONVERTIBLE 	Char	1	Flag indicating the issue can be converted to the common stock (or other security).
# 13	MTN 	Char	1	A flag denoting that the issue is a medium term note.
# 14	ASSET_BACKED 	Char	1	Flag indicating that the issue is an asset-backed issue.
# 15	YANKEE 	Char	1	A flag indicating that the issue has been issued by a foreign issuer, but has been registered with the SEC and is payable in dollars.
# 16	CANADIAN 	Char	1	Flag indicating that the issuer is a Canadian entity issuing the bond in U.S. dollars.
# 17	OID 	Char	1	Original issue discount flag.
# 18	FOREIGN_CURRENCY 	Char	1	Flag indicating that the issue is denominated in a foreign currency.
# 19	SLOB 	Char	1	A flag denoting that the issue is a secured lease obligation issue.
# 20	ISSUE_OFFERED_GLOBAL 	Char	1	A flag denoting that the issue is offered globally.
# 21	SETTLEMENT_TYPE 	Char	4	A code indicating whether the issue will settled in same-day or next-day funds.
# 22	GROSS_SPREAD 	Num	8	The difference between the price that the issuer receives for its securities and the price that investors pay for them.
# 23	SELLING_CONCESSION 	Num	8	The portion of the gross spread paid to other securities dealers in the offering syndicate for reselling the issue for the underwriter.
# 24	REALLOWANCE 	Num	8	The portion of the selling concession that an underwriter foregoes if the issue is sold to another securities firm which is not a member of the underwriting syndicate.
# 25	COMP_NEG_EXCH_DEAL 	Char	4	Code indicating the type of issue sale.
# 26	RULE_415_REG 	Char	1	A flag indicating whether the issue is an SEC Rule 415 shelf registration.
# 27	SEC_REG_TYPE1 	Char	4	A code field containing the SEC registration type.
# 28	SEC_REG_TYPE2 	Char	4	An additional SEC registration type, when applicable.
# 29	RULE_144A 	Char	1	A flag denoting that the issue is a private placement exempt from registration under SEC Rule 144a.
# 30	TREASURY_SPREAD 	Num	8	The difference between the yield of the benchmark treasury issue and the issue\'\'s offering yield expressed in basis points.
# 31	TREASURY_MATURITY 	Char	20	Maturity of benchmark Treasury issue against which the issue\'\'s offering yield was measured.
# 32	OFFERING_AMT 	Num	8	The par value of debt initially issued.
# 33	OFFERING_DATE 	Num	8	The date the issue was originally offered.
# 34	OFFERING_PRICE 	Num	8	The price as a percentage of par at which the issue was originally sold to investors.
# 35	OFFERING_YIELD 	Num	8	Yield to maturity at the time of issuance.
# 36	DELIVERY_DATE 	Num	8	The date the issue was or will be initially delivered by the issuer of the security.
# 37	UNIT_DEAL 	Char	1	A flag indicating whether the issue is part of a unit deal.
# 38	FORM_OF_OWN 	Char	4	Code indicating form of ownership.
# 39	DENOMINATION 	Char	9	The multiples or minimum of principal amount in which the bond can be purchased.
# 40	PRINCIPAL_AMT 	Num	8	The face or par value of a single bond.
# 41	COVENANTS 	Char	1	Flag indicating that the issue\'\'s covenants are recorded in the COVENANTS table.
# 42	DEFEASED 	Char	1	Flag indicating that the issue has been defeased.
# 43	DEFEASANCE_TYPE 	Char	4	A code indicating the type of defeasance allowed.
# 44	DEFEASED_DATE 	Num	8	The date on which the issue was defeased.
# 45	DEFAULTED 	Char	1	Flag indicating that the issuer is in default of the terms of this issue.
# 46	TENDER_EXCH_OFFER 	Char	1	A flag denoting that at least one tender or exchange offer has been made (or is currently outstanding) for the issue.
# 47	REDEEMABLE 	Char	1	A flag indicating that the bond is redeemable under certain circumstances.
# 48	REFUND_PROTECTION 	Char	1	A flag denoting that the issuer is restricted from refunding this issue.
# 49	REFUNDING_DATE 	Num	8	The first date on which the issuer may refund the issue.
# 50	PUTABLE 	Char	1	Put option flag.
# 51	OVERALLOTMENT_OPT 	Char	1	Overallotment option flag.
# 52	ANNOUNCED_CALL 	Char	1	Indicates that the issuer has announced a call for this issue.
# 53	ACTIVE_ISSUE 	Char	1	Flag indicating whether all or a portion of this issue remains outstanding.
# 54	DEP_ELIGIBILITY 	Char	4	Code indicating the depositories on which the offered bonds are eligible for trading.
# 55	PRIVATE_PLACEMENT 	Char	1	Flag indicating that the issue is only being offered privately.
# 56	BOND_TYPE 	Char	4	A code denoting the type of issue.
# 57	SUBSEQUENT_DATA 	Char	1	Flag indicating whether this issue has proceeded beyond the \'\'initial input\'\' phase.
# 58	PRESS_RELEASE 	Char	1	Flag indicating whether this issue contains a press release in the footnotes relating to a Fitch rating action.
# 59	ISIN 	Char	12	The International Securities Identification Number associated with this issue.
# 60	PERPETUAL 	Char	1	Flag denoting an issue has no set maturity date.
# 61	SEDOL 	Char	7	A unique seven character number assigned to securities by the Stock Exchange Daily Official List.
# 62	EXCHANGEABLE 	Char	1	Flag indicating the issue can be converted to the common stock (or other security) of a subsidiary or affiliate of the issuer.
# 63	FUNGIBLE 	Char	1	Flag denoting securities that are, by virtue of their terms, equivalent, interchangeable or substitutable.
# 64	REGISTRATION_RIGHTS 	Char	1	Indicates the issue contains a registration rights agreement.
# 65	PREFERRED_SECURITY 	Char	1	Flag indicating this issue is a preferred security.
# 66	COMPLETE_CUSIP 	Char	9	
#
# Corporate Bonds: BOND_TYPE 
# -------------------------- -
# CCOV: US Corporate Convertible
# CCPI: US Corporate Inflation Indexed
# CDEB: US Corporate Debenture: A debenture is a type of debt instrument that is not secured by physical assets or collateral. Debentures are 
#                               backed only by the general creditworthiness and reputation of the issuer. Both corporations and governments frequently 
#                               issue this type of bond to secure capital. Like other types of bonds, debentures are documented in an indenture.
# CLOC: US Corporate LOC backed
# CMTN: US Corporate MTN - A medium term note (MTN) is a note that usually matures in five to 10 years. A corporate MTN can be continuously offered by 
#                          a company to investors through a dealer with investors being able to choose from differing maturities, ranging from nine months
#                          to 30 years, though most MTNs range in maturity from one to 10 years. For corporate MTNs, this type of debt program is used by a 
#                          company so it can have constant cash flows coming in from its debt issuance; it allows a company to tailor its debt issuance to 
#                          meet its financing needs. Medium-term notes allow a company to register with the Securities and Exchange Commission (SEC) only once,
#                          instead of every time for differing maturities.
# CMTZ: US Corporate MTN Zero
# CP: US Corporate Paper 
# CPAS: US Corporate Pass Thru Trust
# CPIK: US Corporate PIK Bond - A payment-in-kind (PIK) bond is a type of bond that pays interest in additional bonds rather than in cash. 
#                               The bond issuer incurs additional debt to create the new bonds for the interest payments. Payment-in-kind 
#                               bonds are considered a type of deferred coupon bond since there are no cash interest payments during the bond's term.
# CS: US Corporate Strip - A strip bond is a bond where both the principal and regular coupon payments--which have been removed--are sold separately. Also known as a "zero-coupon bond."
# CUIT: US Corporate UIT
# CZ: US Corporate ZeroCoupon 
# USBN: US Corporate Bank Note
# UCID: US corporate Insured Debenture
#
# Rating Type 
# ----------- -
# DPR RATING_TYPE Duff and Phelps Rating
# FR RATING_TYPE Fitch Rating
# FT RATING_TYPE Fitch Credit Trend
# MR RATING_TYPE Moody's Rating
# SPR RATING_TYPE Standard and Poor's Rating
#
# ================================================================= #


# ================================================================= #
# Environment ####
# ================================================================= #
# Clear workspace
rm(list = ls())

# Import libraries
library(data.table)
library(RPostgres)
library(zoo)
library(lubridate)

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


# ================================================================= #
# Read data ####
# ================================================================= #
# Bond Issue -------------------------------------------------------------
issue <- dbGetQuery(wrds, "SELECT * FROM fisd.fisd_issue")
issue <- as.data.table(issue)
issue[, range(offering_date, na.rm = TRUE)]

# Bond Issuer Merged ------------------------------------------------------
# Merged has exactly the same number of lines as issuer, but with more info.
# Such as SIC code.
missuer <- dbGetQuery(wrds, "SELECT * FROM fisd.fisd_mergedissuer")
missuer <- as.data.table(missuer)

# Bond Ratings ------------------------------------------------------------
rating <- dbGetQuery(wrds, "SELECT * FROM fisd.fisd_ratings")
rating <- as.data.table(rating)
rating[, range(rating_date, na.rm = TRUE)]  # "1894-10-02" "2091-06-14"

# Bond Historical Ratings  ------------------------------------------------
ratingh <- dbGetQuery(wrds, "SELECT * FROM fisd.fisd_rating_hist")
ratingh <- as.data.table(ratingh)
ratingh[, range(rating_date, na.rm = TRUE)]  # "1900-03-30" "2040-04-30"

# Bond Historical Amount Outstanding ---------------------------------------
aout <- dbGetQuery(wrds, "SELECT * FROM fisd.fisd_amt_out_hist")
aout <- as.data.table(aout)
aout[, range(effective_date, na.rm = TRUE)]  # "1895-10-01" "2025-07-08"

# Bond Amount Outstanding (current) ---------------------------------------
aoutc <- dbGetQuery(wrds, "SELECT * FROM fisd.fisd_amount_outstanding")
aoutc <- as.data.table(aoutc)
aoutc[, range(effective_date, na.rm = TRUE)]  # "1914-10-25" "2025-07-17"
aoutc[effective_date == "2202-04-22", effective_date := as.Date("2022-04-22")]

# Issuer Cusip ---------------------------------------
issuer_cusip <- dbGetQuery(wrds, "SELECT * FROM fisd.fisd_issuer_cusip")
issuer_cusip <- as.data.table(issuer_cusip)

# Agent data ---------------------------------------
agent <- dbGetQuery(wrds, "SELECT * FROM fisd.fisd_agent")
agent <- as.data.table(agent)

# Redemption data ---------------------------------------------------------
redemp <- dbGetQuery(wrds, "SELECT * FROM fisd.fisd_mergedredemption")
redemp <- as.data.table(redemp)


# ================================================================= #
# Write data ####
# ================================================================= #
# Bond Issue -------------------------------------------------------------
if(any(duplicated(issue[, .(issue_id)]))){
  stop('Data primary key violated.')
}
saveRDS(issue, file = paste0(RAWDIR, 'fisd/fisd_issue.rds'))
rm(issue)

# Bond Issuer Merged ------------------------------------------------------
if(any(duplicated(missuer[, .(issuer_id)]))){
  stop('Data primary key violated.')
}
saveRDS(missuer, file = paste0(RAWDIR, 'fisd/fisd_mergedissuer.rds'))
rm(missuer)

# Bond Ratings ------------------------------------------------------------
saveRDS(rating, file = paste0(RAWDIR, 'fisd/fisd_ratings.rds'))
rm(rating)

# Bond Historical Ratings ------------------------------------------------
saveRDS(ratingh, file = paste0(RAWDIR, 'fisd/fisd_ratings_hist.rds'))
rm(ratingh)

# Bond Historical Amount Outstanding ---------------------------------------
if(any(duplicated(aout[, .(issue_id, transaction_id)]))){
  stop('Data primary key violated.')
}
saveRDS(aout, file = paste0(RAWDIR, 'fisd/fisd_amt_out_hist.rds'))
rm(aout)

# Bond Amount Outstanding (current) ---------------------------------------
if(any(duplicated(aoutc[, .(issue_id)]))){
  stop('Data primary key violated.')
}
range(aoutc[!is.na(effective_date)]$effective_date)  # "1914-10-25" "2025-07-17"
saveRDS(aoutc, file = paste0(RAWDIR, 'fisd/fisd_amt_out.rds'))
rm(aoutc)

# Issuer Cusip ---------------------------------------
saveRDS(issuer_cusip, file = paste0(RAWDIR, 'fisd/fisd_issuer_cusip.rds'))
rm(issuer_cusip)

# Agent data ---------------------------------------
saveRDS(agent, file = paste0(RAWDIR, 'fisd/fisd_agent.rds'))
rm(agent)

# Redemption data ---------------------------------------------------------
if(any(duplicated(redemp[, .(issue_id)]))){
  stop('Data primary key violated.')
}
saveRDS(redemp, file = paste0(RAWDIR, 'fisd/fisd_redemption.rds'))
rm(redemp)

dbDisconnect(wrds)
print('FISD tables have been downloaded successfully.')


