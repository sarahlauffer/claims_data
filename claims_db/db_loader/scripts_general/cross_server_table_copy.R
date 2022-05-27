## This script is used to copy data from a table on one server to the other.
## Use case example: stage table, archive and backup table get wiped out do to error

options(max.print = 350, tibble.print_max = 50, warning.length = 8170,
        scipen = 999)

library(tidyverse) # Manipulate data
library(dplyr) # Manipulate data
library(lubridate) # Manipulate dates
library(odbc) # Read to and write from SQL
library(keyring) # Access stored credentials
library(R.utils)

devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/claims_data/master/claims_db/db_loader/mcaid/create_db_connection.R")

memory.limit(size = 56000)
server <- "hhsaw"
prod <- TRUE
interactive_auth <- TRUE

conn <- create_db_connection(server, interactive = interactive_auth, prod = prod)

df <- DBI::dbGetQuery(conn, "SELECT * FROM [claims].[stage_mcaid_elig]")
etlh <- DBI::dbGetQuery(conn, "SELECT [etl_batch_id], [file_name] FROM [claims].[metadata_etl_log] where [file_name] is not null")

server <- "phclaims"
conn <- create_db_connection(server, interactive = interactive_auth, prod = prod)

etlp <- DBI::dbGetQuery(conn, "SELECT [etl_batch_id] as 'new_id', [file_name] FROM [metadata].[etl_log] where [file_name] is not null")
etl <- left_join(etlh, etlp)
dfj <- inner_join(etl, df)
dfj$etl_batch_id <- dfj$new_id
dfj <- subset(dfj, select = -c(file_name, new_id))

inc <- 100000
d_stop <- as.integer(nrow(dfj) / inc)
if (d_stop * inc < nrow(dfj)) { d_stop <- d_stop + 1 }
message(paste0("...Loading Progress - 0%"))

### Begin data loading loop
for( d in 1:d_stop) {
  conn <- create_db_connection(server, interactive = interactive_auth, prod = prod)
  d_start <- ((d - 1) * inc) + 1
  d_end <- d * inc
  if (d_end > nrow(dfj)) { d_end <- nrow(dfj) }
  dbAppendTable(conn, 
                name = DBI::Id(schema = 'stage', table = 'mcaid_elig'), 
                value = dfj[d_start:d_end,])  
  message(paste0("...Loading Progress - ", round((d / d_stop) * 100, 2), "%"))
}
