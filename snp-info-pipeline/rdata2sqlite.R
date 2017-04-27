library(tools) 

#THIS VERSION is specifically for SNP_INFO
#it DOES NOT work for our purposes right now, though.
#...

#this script should take a path to an input file and a working directory
#read in the input file, and output an sqlite3 table to the specified working directory.

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


#Should be called on one file of SNP_INFO data; will write to the db connection
#one row at a time.
put_snp_data_in_dataframe <- function(snpinfo_hg38){
   #snpinfo_hg38 is already loaded and available here, right?
   snpids <- unlist(snpinfo_hg38$snpids)
   ref_base <- unlist(snpinfo_hg38$ref_base)
   snp_base <- unlist(snpinfo_hg38$snp_base)
   sequence_matrix <- 
        apply(snpinfo_hg38$sequence_matrix, 2, 
              function(x) paste(x, collapse=""))
   #transform the sequence_matrix from a 61xHUGE to a vector
   #of strings, the collapsed columns of each row of the matrix.
   #print(paste("length of snpids; ", length(snpids)))
   #print(paste("length of sequence_matrix; ", length(sequence_matrix)))
   #print(paste("length of ref_base; ", length(ref_base)))
   #print(paste("length of snp_base; ", length(snp_base)))
   df <- data.frame(snpids, ref_base, snp_base, sequence_matrix)
   return(df) 
}



process_one_file_of_Rdata <- function(path_to_input_file, output_dir){
  full_path_to_output <- get_full_path_for_db_output(path_to_input_file, output_dir)  
  #TODO: ensure that the output directory is creaetd by the calling python script.
  #dir.create(output_dir)
  
  #overwrite file if exists; don't just bail
  if ( file.exists(full_path_to_output) ) { 
     print(paste("Output file already exists. Deleting.", full_path_to_output))
     file.remove(full_path_to_output)
     #consider deleting file here...
  }
  #print(paste("path to input file: " , path_to_input_file))
  print(paste("output_file: ", full_path_to_output) )
  system(paste("touch ", full_path_to_output))
  open_db <- setup_connection_to_write_to(full_path_to_output)
  #print(paste("Loading data file from : ", path_to_input_file)) 
  load(path_to_input_file, verbose=TRUE) 

  #print(paste("writing N rows, where N=", nrow(atsnp.bigtables)))
  print(paste("writing N rows, where N=", length(snpinfo_hg38$ref_base)))

  #data_to_write <- as.data.frame(atsnp.bigtables) 
  data_to_write <- put_snp_data_in_dataframe(snpinfo_hg38)
  #data_to_write <- as.data.frame(snpinfo_hg38) 

  dbWriteTable(open_db, 'snpinfo_hg38', data_to_write, overwrite=TRUE) 
  dbDisconnect(open_db)
}

args = commandArgs(trailingOnly=TRUE)
source_file <- args[1] #processing only one file to sqlite3 at a time.
dest_dir <- args[2]

print(paste("source file: ", source_file)) 
print(paste("dest dir: ", dest_dir))

#simple checks.
if (! ( file.exists(source_file)) ) { print(paste("Source file not valid.", source_file)) }
if (! ( dir.exists(dest_dir))){ print(paste("Destination directory not valid.", dest_dir)) }

#these outputs are redundant at this point
#filenames <- list.files(source_dir)
#print("filenames..")
#print(source_file)

process_one_file_of_Rdata(source_file, dest_dir)

#for (i in 1:length(filenames)){
#  one_path <- file.path(source_dir, filenames[[i]]) 
#  stopifnot(file.exists(one_path))
#  process_one_file_of_Rdata(one_path, dest_dir)
#}


