library(here)
library(data.table)
library(stringr)
source(here::here("utils/clean_company_names0.R"))

# test <- data.table('Issuer'=c('Apple. Inc [big chapter]', 
#                               'Facebook, Inc. (US)', 
#                               'Alpha-bet LLC', 
#                               'UK Ltd.', 
#                               'INCA & Co.'))
# original_company_name <- test$Issuer

clean_company_names1 <- function(original_company_name){

  # Step 1: apply clean company names 1: cap letter and get rid of punctuation.   
  new_company_name <- clean_company_names0(original_company_name)
  
  # Step 2: replace shortened version with long versions of suffixes 
  ## Careful: keep spaces to make sure to only include full words.
  abb <- rbind(c("\\bCORP\\b", "CORPORATION"),
               c("\\bCOR$", "CORPORATION"),
               c("\\bCO$", "COMPANY"),
               c("\\bCOS$", "COMPANIES"),
               c("\\bINC\\b", "INCORPORATED"),
               c("\\bLTD\\b", "LIMITED"),
               c("\\bHLDNG\\b", "HOLDINGS"),
               c("\\bINTL\\b", "INTERNATIONAL"),
               c("\\bPRODS\\b", "PRODUCTS"),
               c("\\bINDS\\b", "INDUSTRIES"),
               c("\\bCOMMUN\\b", "COMMUNICATIONS"),
               c("\\bMGMT\\b", "MANAGEMENT"),
               c("\\bBLDG\\b", "BUILDING"),
               c("\\bTECH\\b", "TECHNOLOGY"),
               c("\\bGRP\\b", "GROUP"),
               c("\\bWRHS\\b", "WAREHOUSE"))
  abb <- as.data.table(abb)
  colnames(abb) <- c('short_name', 'long_name') 
  
  for(i in 1:nrow(abb)){
    new_company_name <- str_replace(new_company_name, abb[i,short_name], abb[i,long_name])
  }
  
  return(new_company_name)
}
