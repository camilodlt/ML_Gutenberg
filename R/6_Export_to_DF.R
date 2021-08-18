#################################################################
##              Step 5: Create synthetic mistakes              ##
#################################################################

######
PATHS <- c("data_working/5_ML_formatted_synthetic", "data_working/5_unbias_constant_synthetic","data_working/5_unbias_multiply_synthetic")
SOURCES <- c("Normal", "Constant", "Unbiased")
#####
source("R/common.R")
source("R/functions_synthetic_mistakes.R")
options(future.rng.onMisuse="ignore")

# Caller
step <- "Step 5: Read synthetic mistakes"
# Purpose
explanations <- "
# Mistakes are now created
# This script reads them and binds them into a txt file
"
print(banner(step))
print(boxup(explanations, centre = F))

# DIRECTORY FN ------
dir_check<- function(path){
  info(logger, "Checking directory")
  if (dir.exists(path) & override==FALSE ){ # Exists & not to override
    warning("DIRECTORY ALDEADY EXIST")
    quit(save="ask")
  } else if (!dir.exists(path) | (dir.exists(path) & isTRUE(override))){ # does not exist OR exist & override
    unlink(path,recursive = TRUE)
    dir.create(path,showWarnings = FALSE,recursive = TRUE)
    info(logger, "Directory created")
  }
}

# LIBRARIES ------
suppressMessages(library(dplyr,quietly = TRUE))
suppressMessages(library(disk.frame,quietly = TRUE))
suppressMessages(library(purrr,quietly = TRUE))
suppressMessages(library(furrr,quietly = TRUE))
suppressMessages(library(pryr,quietly = TRUE))
# HYPER ------
override<- TRUE
source_choice<- 1 # 1-3
PATH<- PATHS[source_choice] # 1 - 3
SOURCE <- SOURCES[source_choice]
##---------------------------------------------------------------
##                  Apply synthethic mistakes                   -
##---------------------------------------------------------------

info(logger, "Reading disk.frames")

# FILES ------
df_no_touched <- disk.frame(PATH)

# INFORMATION ------
info(logger, "Print information about disk.frame")
print(df_no_touched)

# LOAD TO MEMORY AND CONVERT TO DF ------
info(logger, "LOAD TO MEMORY")
n_chunk<- seq(nchunks(df_no_touched))
plan(multisession,workers = 6)
Text_data <-furrr::future_map_dfr(n_chunk, function(x){
  as.data.frame(get_chunk(df_no_touched, x))})

cat("NUMBER OF ROWS:", nrow(Text_data),"\n")
cat("SIZE OF DF IN MEMORY:", object_size(Text_data)/(1024*1024),"\n")
cat("% OF MISTAKES IN LINES:", round(1-mean(Text_data$eq),3),"\n")

# SAVING -----
info(logger, "SAVING")
base_path<- paste0(getwd(),"/data_output/",SOURCE)
dir_check(base_path) # Create, check or override
message("Saving to", base_path)
write.table(Text_data[,c("Truth","Mistake")], # Just Truth and Mistake
            file=paste0(base_path,"/READY_ML.txt"),
            row.names = FALSE,
            col.names = TRUE)
# Write sample
write.table(Text_data[1:1000,c("Truth","Mistake")], # Just Truth and Mistake
            file=paste0(base_path,"/Sample_ML.txt"),
            row.names = FALSE,
            col.names = TRUE)

cat("FINISHED")
