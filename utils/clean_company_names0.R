library(data.table)
library(stringr)

test <- data.table('Issuer'=c('Huntsman Intl Corporation',
                             'Credit Suisse (USA) Corporation',
                             'CBS Corporation.',
                             'CBS Incorporated',
                             'INCA & Co.',
                             "AARON'S INCORPORATED"))
# original_company_name <- test$Issuer

clean_company_names0 <- function(original_company_name){
  # Step 1: to upper case
  new_company_name <- toupper(original_company_name)
  
  # Step 2: get rid of periods, commas, dashes, 's, space between single letters
  new_company_name <- gsub("\\.", "", new_company_name)
  new_company_name <- gsub("\\,", "", new_company_name)
  new_company_name <- gsub("\\-", "", new_company_name)
  new_company_name <- gsub("\\'", "", new_company_name)
  new_company_name <- gsub("(?<=\\b\\w)\\s(?=\\w\\b)", "", new_company_name, perl = TRUE)
  return(new_company_name)
}


clean_company_names0(test)
