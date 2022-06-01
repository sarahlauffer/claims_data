library(stringr)
library(R.utils)

base_dir <- "//phcifs/SFTP_DATA/APDEDataExchange/WA-APCD/"
export_dir <- paste0(base_dir,"export/")
load_date <- "20220527"

file_list <- list.files(path = export_dir,
                        recursive = T,
                        full.names = T)

for(file in file_list) {
  #new_file <- str_replace_all(file, "0527", "0531")
  new_file <- str_replace_all(file, ".txt", "")
  new_file <- str_replace(new_file, ".gz.0", ".0")
  new_file <- str_replace(new_file, ".part", ".txt")
  new_file <- str_replace(new_file, ".txt", paste0("_", load_date, ".txt"))
  file.rename(file, new_file)
}
