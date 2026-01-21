# ================================================================= #
# Functions ####
# ================================================================= #
#There are two main functions
# 1. makevwportfolio: creates value weighted portfolios based on the group_var
# 2. maketestassetpanel: wrapper of makevwportfolio function to make a panel
#       with the test assets based on grouping vars

makevwportfolio <- function(
    DT, group_var, value_var, return_var, date_var, current, lag_threshold) {
  #This function creates value weighted portfolios based on the group_var
  
  #DT <- datatable with information
  #group_var <- vector of variables to group portfolios by
  #value_var <- variable to use as value for value weighting
  #return_var <- variable to use as return for the portfolio
  #date_var <- variable to be use in `by` statement
  # For example: c("year", "month") will make per year and month
  # "weekdate" will make it weekly. Must be a datetime variable
  #current: if TRUE use [t] group_var to compute returns at [t]
  #         if FALSE use [t-1] group_var to compute returns at [t]
  #lag_threshold <- number of days to consider an asset as disappeared
  #    and re-appeared. If more than lag_threshold days, we use NA
  
  # Create a unique identifier based on the group_var
  identifier <- paste(group_var, collapse = "_")
  
  if (current) {
    print("Using current characteristics")
    #Using current characteristics
    if (is.null(group_var)) {
      adj_group_vars <- NULL 
    } else {
      adj_group_vars <- group_var
    }
  } else if (!current){
    #Using lagged characteristics
    print("Using lag characteristics")
    #Lag group_var
    if (is.null(group_var)){  
      adj_group_vars <- NULL
    } else {
      for (var in group_var) {
        #Lag each variable
        DT[, 
           paste0("lag_", var) := shift(get(var), type = "lag"),
           by = c("isin")
        ]
      }
      #Store new variables names
      adj_group_vars <- paste0("lag_", group_var)
    }
  } else {
    stop("current must be TRUE or FALSE")
  }  
  
  # Lag value-weighting variable (usually market value)
  setorderv(DT, c("isin", date_var))
  DT[, lag_mktvalue :=
       shift(get(value_var), type = "lag"),
     by = isin]
  
  # Calculate market size per adj_group (depends on current) and date
  DT[, lag_totalmkt := sum(lag_mktvalue, na.rm = T),
     by = c(adj_group_vars, date_var)]
  
  # Calculate weights
  DT[, paste0("w_", identifier) := 
       lag_mktvalue / lag_totalmkt
  ]
  
  # Check for consecutive dates for each isin
  setorderv(DT, c("isin", date_var))
  DT[, datedistance := 
       as.numeric(get(date_var) - shift(get(date_var), type="lag")),
     by = isin]
  # If datedistance is greater than lag_threshold, we replace by 0
  # For example if an asset disappears in january and re-appears in august
  # We cannot know what happened and we do not include in the ptf
  DT[, paste0("w_", identifier) :=
       ifelse(
         datedistance >= lag_threshold,
         0,
         get(paste0("w_", identifier))
       ),
     by = isin
  ]
  #Also, if the asset has NA in the value_var or return_var, we replace by 0
  DT[is.na(get(value_var)) | is.na(get(return_var)), 
     paste0("w_", identifier) := 0]
  #The steps above require standardizing weights so they sum to 1
  DT[, paste0("norm_w_", identifier) :=
       get(paste0("w_", identifier)) / 
       sum(
         get(paste0("w_", identifier)),
         na.rm = TRUE
       ), 
     by = c(adj_group_vars, date_var)
  ]
  
  # Make the vw risk portfolio
  DT[, paste0("portfolio_", identifier) := 
       sum(
         get(paste0("norm_w_", identifier)) * get(return_var),
         na.rm = TRUE
       ), 
     by = c(adj_group_vars, date_var)]
  
  #Get number of assets in the portfolio
  DT[, paste0("n_", identifier) := .N, 
     by = c(adj_group_vars, date_var)]
  
  # Clean up temporary columns
  if (current) {
    DT[, c(
      "datedistance",
      "lag_mktvalue",
      "lag_totalmkt",
      paste0("w_", identifier)
    ) := NULL]
  } else {
    DT[, c(
      "datedistance",
      "lag_mktvalue",
      "lag_totalmkt",
      paste0("w_", identifier)
    ) := NULL] 
  }
  
  return(DT)
}

maketestassetpanel <- function(
    DT, group_var, value_var, return_var, date_var, current, lag_threshold = 45) {
  # Wrapper of makevwportfolio function
  # Make a panel with the test assets based on grouping vars
  # Panel is value-weighted portfolio for each characteristic
  # Returns also a graph over time for checking 
  
  # group_var <- vector of variables to group portfolios by
  # group_var example c("rating_group", "currency")
  # value_var <- variable to use as value for value weighting
  # return_var <- variable to use as return for the portfolio
  # date_var <- variable to be use in `by` statement
  #    For example: c("year", "month") will make per year and month
  #    Or c("yearweek") will make weekly (user defined variable)
  # current <- if TRUE use [t] group_var to compute returns at [t]
  #            if FALSE use [t-1] group_var to compute returns at [t]
  #lag_threshold <- number of days to consider an asset as disappeared
  #    and re-appeared. If more than lag_threshold days, we use NA
  
  
  #initialize function
  if (current) {
    adj_group_vars <- group_var
  } else {
    adj_group_vars <- paste0("lag_", group_var)
  }
  
  by_expr <- c(adj_group_vars, date_var)
  
  #Calculate risk portfolio
  DT <- makevwportfolio(
    DT,
    group_var = group_var,
    value_var,
    return_var,
    date_var,
    current = current,
    lag_threshold = lag_threshold
  )
  if (is.null(group_var)) {
    w_check <- "norm_w_"
  } else {
    w_check <- paste0(c("norm_w", group_var), collapse = "_")
  }
  
  # Check calculations
  check_result <- DT[, sum(get(w_check), na.rm = TRUE),
                     by = eval(by_expr)]
  tolerance <- 1e-9 #some values are 1 but not precisely one due to computing
  check_result$V1[abs(check_result$V1 - 1) < tolerance] <- 1
  if (any(check_result$V1 != 1 & check_result$V1 != 0)) {
    warning("Sum of weights per period should be 1 or 0. Please double-check.")
    warning(paste("Weights are:", unique(check_result[, V1])))
  }
  
  # Make test assets panel
  if (is.null(group_var)) {
    columns <- c(date_var, adj_group_vars, "portfolio_", "n_")
  } else {
    columns <- c(date_var, adj_group_vars,
                 paste(c("portfolio", group_var), collapse = "_"),
                 paste(c("n", group_var), collapse = "_"))
  }
  test_assets <- unique(DT[, ..columns])
  
  # Check panel
  panel_check <- test_assets[, .N, by = eval(by_expr)]
  if (any(panel_check$N > 1)) {
    warning("Multiple observations found for some date-group combinations.")
  }
  
  # Filter out NA in group_vars
  if (!current) {
    #This happens only when using lag variables for grouping
    logical_expr <- Reduce(
      #Reduce w &: check if all the variables in adj_group_vars are not NA
      `&`,
      #Check if any of the group_vars is NA
      lapply(adj_group_vars, function(var) !is.na(test_assets[[var]]))
    )
    # Filter rows based on constructed logical expression
    test_assets <- test_assets[logical_expr]
  }
  
  if (current) {
    first_date <- min(test_assets[[date_var]])
    test_assets <- test_assets[test_assets[[date_var]] > first_date]
  }
  
  #Remove columns created by makevwportfolios
  identifier <- paste(group_var, collapse = "_")
  DT[, c(paste0("portfolio_", identifier), paste0("n_", identifier), w_check) := NULL]
  setorderv(test_assets, c(date_var))
  return(test_assets)
}