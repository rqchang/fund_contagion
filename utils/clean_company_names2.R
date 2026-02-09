library(here)
library(data.table)
library(stringr)
source(here::here("utils/clean_company_names1.R"))

# test <- data.table('Issuer'=c('Apple. Inc [big chapter]', 
#                               'Facebook, Inc. (US)', 
#                               'Alpha-bet LLC', 
#                               'UK Ltd.', 
#                               'INCA & Co.'))
# original_company_name <- test$Issuer

clean_company_names2 <- function(original_company_name){
  # Step 1: first cleaning
  new_company_name <- clean_company_names1(original_company_name)
  
  # Step 2: get rid of common suffixes at the end
  cs <- c("\\bTHE\\b",  
          "\\bINCORPORATED\\b",
          "\\bCORPORATION\\b",
          "\\bLIMITED\\b",
          "\\bCOMPANY\\b", 
          " AMERICAN$", 
          "\\bGROUP\\b" , 
          "\\bPLC\\b", 
          " INTERNATIONAL\\b", 
          "\\bUSA\\b") 
  # The space and the str_replace function is important so we do not change cases like INCA.
  for(c in cs){
    new_company_name <- str_replace(new_company_name, c, "")
  }
  new_company_name <- trimws(new_company_name) # remove leading/trailing white spaces
  return(new_company_name)
}



