# ================================================================= #
# db_utilities.R ####
# ================================================================= #
# Description:
# ------------
#     Helper functions for research_data repository
#
# Input(s):
# ---------
#     None
#
# Output(s):
# ----------
#     None
#
# Author(s):
# ----------
#     Nicholas von Turkovich
#
# Additional note(s):
# ----------
#     
# ================================================================= #

library(RPostgres)
library(data.table)
library(bcputility)

# ================================================================= #
# Helper functions ####
# ================================================================= #

# Creates a link to particular database on server
db_con <- function(dbname, windows_auth = TRUE){
  
  os <- Sys.info()["sysname"]
  
  if (os == "Linux"){
    if (is.null(Sys.getenv("mssql_uid")) | is.null(Sys.getenv("mssql_pw"))){
      stop("UID/PW not set in environment variables")
    } else {
      dbcon <- DBI::dbConnect(odbc::odbc(), DSN = "STS-LIRAMOTA", UID = Sys.getenv("mssql_uid"), PWD = Sys.getenv("mssql_pw"), database = dbname)
    }
  } else {
    if (windows_auth){
      dbcon <- DBI::dbConnect(drv = odbc::odbc(),Driver = "ODBC Driver 17 for SQL Server", Server="sts-liramota.mit.edu,1433",Encrypt="yes",TrustServerCertificate="yes",ServerSPN="MSSQLSvc/sts-liramota.mit.edu:1433",Database=dbname, trusted_connection="yes")
    } else {
      if (is.null(Sys.getenv("mssql_uid")) | is.null(Sys.getenv("mssql_pw"))){
        stop("UID/PW not set in environment variables")
      } else {
        dbcon <- DBI::dbConnect(odbc::odbc(),
                                .connection_string = paste0("driver={ODBC Driver 17 for SQL Server};server=sts-liramota.mit.edu;database=", dbname, ";uid=", Sys.getenv("mssql_uid"), ";pwd=", Sys.getenv("mssql_pw")))
      }
    }
  }
  
  
  return(dbcon)
}

db_conArgs <- function(dbname, windows_auth = TRUE){
  if (windows_auth){
    dbconArgs<- bcputility::makeConnectArgs(server = "sts-liramota.mit.edu,1433", database = dbname)
  } else {
    if (is.null(Sys.getenv("mssql_uid")) | is.null(Sys.getenv("mssql_pw"))){
      stop("UID/PW not set in environment variables")
    } else {
      dbconArgs<- bcputility::makeConnectArgs(server = "sts-liramota.mit.edu,1433", database = dbname, trustedconnection = FALSE, username = Sys.getenv("mssql_uid"), password = Sys.getenv("mssql_pw"))
    }
  }
}

# Create a link to WRDS
wrds_con <- function(){
  if (is.null(Sys.getenv("wrds_username"))){
    stop("Remember to add WRDS credentials to .Renviron")
  }
  wrds <- dbConnect(Postgres(),
                    host='wrds-pgdata.wharton.upenn.edu',
                    port=9737,
                    user= Sys.getenv("wrds_username"),
                    sslmode='require',
                    dbname='wrds')
  return(wrds)
}

# Determine field types for format of SQL table
compute_field_types <- function(db, df){
  
  # Get metadata and start type vector
  ctypes <- mapDataTypes(df)
  
  # Change FLOAT to DECIMAL
  ctypes[ctypes == "FLOAT"] <- "DECIMAL(36,18)"
  
  # Change varchar to nvarchar
  ctypes <- gsub("VARCHAR", "NVARCHAR", ctypes)
  
  return(ctypes)
  
}

# Function to read using ODBC connection
db_read <- function(db, table_name = NULL, sql_query = NULL){
  if (is.null(sql_query) & !is.null(table_name)){
    df <- dbReadTable(db, name = table_name) |> as.data.table()
  } else if (!is.null(sql_query)) {
    df <- DBI::dbGetQuery(db, statement = sql_query)
  } else {
    stop("Did not specify table name")
  }
  print(paste0("Loaded ", table_name, " successfully"))
  return(df)
}

# Function to test equality of dataframe with SQL table
check_db <- function(db, table_name, table, sort_cols = NULL, first_n = NULL, subset_cmd = NULL){
  
  if (!is.null(first_n)){
    
    if (first_n > dim(table)[1]){
      stop("Comparison dataframe doen't have specified number of rows")
    }
    
    rs <- dbSendQuery(db, paste0("SELECT * FROM ", table_name))
    test = dbFetch(rs, n = first_n) |> as.data.table()
    dbClearResult(rs)
    table <- table[1:first_n,]
  } else if (!is.null(subset_cmd)){
    rs <- dbSendQuery(db, subset_cmd)
    test = dbFetch(rs) |> as.data.table()
    dbClearResult(rs)
  } else {
    test = dbReadTable(db, name = table_name) |> as.data.table()
  }
  
  print(paste0("Loaded ", table_name, " successfully"))
  
  if (!is.null(sort_cols) & !any(sort_cols == "")){
    setorderv(test, cols = sort_cols, order = 1, na.last = TRUE)
    setorderv(table, cols = sort_cols, order = 1, na.last = TRUE)
  } else {
    test <- test[do.call(order, test)]
    table <- table[do.call(order, table)]
  }
  
  if (!isTRUE(all.equal(table, test, check.attributes = FALSE, ignore.col.order = TRUE))){
    stop(paste0("SQL dataframe for " , table_name, " does not match"))
  } else {
    print(paste0("SQL dataframe matches for " , table_name))
  }
  
  
}

# Function to add a primary key to a SQL table
db_add_key <- function(db, table_name, pk, pk_types){
  
  # Credit: https://github.com/schardtbc/DBIExt/blob/master/R/sql-generation.R
  
  stopifnot(DBI::dbExistsTable(db, table_name))
  colsInTable <- DBI::dbListFields(db,table_name)
  stopifnot(setequal(pk,intersect(pk,colsInTable)))
  
  table_name.q <- DBI::dbQuoteIdentifier(db, table_name)
  
  pk.q <-
    sapply(pk, function(x) {
      DBI::dbQuoteIdentifier(db, as.character(x))
    })
  
  # Make sure columns are not nullable
  sql_nullable_cols <- 
    sapply(seq(1,length(pk.q)), function(x){
      paste0(
        "ALTER TABLE ",
        table_name.q,
        "\n",
        "ALTER COLUMN ",
        pk.q[x], 
        " ",
        pk_types[x],
        " not null;\n"
      )
    }) |> Reduce(f = paste0) |> DBI::SQL()
  stat <- dbExecute(db, sql_nullable_cols)
  
  sql_alter_table <- DBI::SQL(paste0(
    "ALTER TABLE ",
    table_name.q,
    "\n",
    "ADD PRIMARY KEY (",
    paste0(pk.q, collapse = ", "),
    ");"
  ))
  stat <- dbExecute(db, sql_alter_table)
  
}

# Function to convert select set up types before writing to SQL
convert_types <- function(df){
  
  col_types <- lapply(sapply(df, class), '[[', 1)
  
  # Convert hms/difftime to character
  hms_cols <- names(col_types[col_types %in% c("hms", "difftime")])
  if (length(hms_cols) > 0){
    df <- df[, (hms_cols) := lapply(.SD, as.character), .SDcols = hms_cols]
  }
  
  
  return(df)
  
}

tbl_to_database <- function(input_tbl, db_from, db_to, db, schema, tbl_name){
  
  if(DBI::dbExistsTable(db_to, tbl_name)){
    DBI::dbRemoveTable(db_to, tbl_name)
  }
  
  # SQL query
  sql_query <- glue::glue("SELECT *\n",
                          "INTO {db}.{schema}.{tbl_name}\n",
                          "FROM (\n",
                          dbplyr::sql_render(input_tbl),
                          "\n) AS from_table")
  
  # run query
  DBI::dbExecute(db_from, as.character(sql_query))
}

# Function to write to SQL table using bcp utility
db_write <- function(db, connectArgs, table_name, table, pk = NULL, field_types = NULL){
  
  # Remove any quotes in character types
  char_cols <- names(table)[(lapply(table, class) == "character")]
  if (length(char_cols) > 0){
    table[,(char_cols) := lapply(.SD,gsub,pattern = "\"",
                                 replacement = ""), .SDcols = char_cols]
  }
  
  # Convert specific types
  table <- convert_types(table)
  
  # Generate types for columns if non passed in
  if (is.null(field_types)){
    field_types <- compute_field_types(connectArgs, table)
  }
  
  # Round off any decimal columns
  round_cols <- names(field_types[grepl("DECIMAL", field_types)])
  if(length(round_cols) > 0){
    round_fun <- function(sd){
      ret <- as.numeric(substr(field_types[sd], nchar(field_types[sd])-2, nchar(field_types[sd])-1))
      return(ret)
    }
    table[,(round_cols) := lapply(.SD, round, digits = round_fun(names(.SD))), .SDcols = round_cols]
  }
  
  # Create table
  if (checkTableExists(connectArgs, table_name)){
    dropTable(connectArgs, table_name)
  }
  createTable(connectArgs, table_name, coltypes = field_types)
  print(paste0("Created ", table_name))
  
  # Create table
  bcpImport(table,
            connectargs = connectArgs,
            table = table_name,
            overwrite = FALSE,
            bcpOptions = list("-h", "TABLOCK", "-a", 65535, "-b", 50000, "-C", 65001))
  
  if (!is.null(pk) & !any(pk == "")){
    
    pk_types <- unname(field_types[pk])
    
    if (any(duplicated(table, by = pk))){
      stop("Duplicates in pk")
    }
    
    db_add_key(db, table_name, pk, pk_types)
    
  }
  
}
