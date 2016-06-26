library(tools) 

get_full_path_for_db_output <- function(path_to_input_file, output_dir){
  name_of_output_file <- basename(file_path_sans_ext(path_to_input_file))
  name_of_output_file <- paste(name_of_output_file, "db", sep = ".")
  full_path_to_output <- file.path(output_dir, name_of_output_file) 
  #print(paste("full path to output: ", full_path_to_output))
  return(full_path_to_output)
}

setup_connection_to_write_to <- function(path_to_write_to){
  library(RSQLite)
  library(DBI)   
  db_driver <- dbDriver("SQLite")
  open_db <- dbConnect(db_driver, path_to_write_to) 
  return(open_db)
} 


process_one_file_of_Rdata <- function(path_to_input_file, output_dir){
  full_path_to_output <- get_full_path_for_db_output(path_to_input_file, output_dir)  

  dir.create(output_dir)
  if ( file.exists(full_path_to_output) ) { 
     print(paste("Output file already exists. Skipping.", full_path_to_output))
     return(1)
  }


  #print(paste("path to input file: " , path_to_input_file))
  print(paste("path to output_file: ", full_path_to_output) )
  system(paste("touch ", full_path_to_output))
  open_db <- setup_connection_to_write_to(full_path_to_output)

  print(paste("Loading data file from : ", path_to_input_file)) 

  load(path_to_input_file, verbose=TRUE) 
  print(paste("going to write N rows, where N = ", nrow(atsnp.tables)))
  data_to_write <- as.data.frame(atsnp.tables) 
  dbWriteTable(open_db, 'scores_data', data_to_write, overwrite=TRUE) 
}




args = commandArgs(trailingOnly=TRUE)
source_dir <- args[1]
dest_dir <- args[2]

print(paste("source dir: ", source_dir)) 
print(paste("dest dir: ", dest_dir))

#simple checks.
if (! ( dir.exists(source_dir)) ) { print(paste("Source directory not valid.", source_dir)) }
if (! ( dir.exists(dest_dir))){ print(paste("Destination directory not valid.", source_dir)) }

filenames <- list.files(source_dir)
print("filenames..")
print(filenames)

for (i in 1:length(filenames)){
  one_path <- file.path(source_dir, filenames[[i]]) 
  stopifnot(file.exists(one_path))
  process_one_file_of_Rdata(one_path, dest_dir)
}


