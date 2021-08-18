#################################################################
##              Step 5: Create synthetic mistakes              ##
#################################################################

######
PATHS <- c("data_working/3.5_ML_formatted/","data_working/4_unbias_constant/","data_working/4_unbias_multiply/")
DEST_PATHS <- c("data_working/5_ML_formatted_synthetic", "data_working/5_unbias_constant_synthetic","data_working/5_unbias_multiply_synthetic")
stopifnot({length(PATHS)==length(DEST_PATHS)})
#####
# TESTS YIELDED THIS
#4 cores : Time difference of 4.562504 hours. 2500 batch
#6 cores: Time difference of 3.654242 hours. batch 2500
#6 cores. Time difference of 3.982715 hours. Batch 5000
#6 cores. Time difference of 4.109128 hours. Batch 2500 without GC
#8 cores. Time difference of 3.701708 hours. Batch 2500, without GC

source("R/common.R")
source("R/functions_synthetic_mistakes.R")

options(future.rng.onMisuse="ignore")
options(future.globals.maxSize= +Inf)

# Caller
step <- "Step 5: Create synthetic mistakes"
# Purpose
explanations <- "
# Create synthethic mistakes for at least half of the sentences
# Each sentence has a ground truth and a phrase which is potentially mispelled
# Since the process is long, synthethic mistakes will be applied by several processes (cores)
# Each process writes a txt file that will be concatenated later
"
print(banner(step))
print(boxup(explanations, centre = F))

# LIBRARIES ------
library(dplyr)
library(disk.frame)
library(purrr)
library(furrr)
library(spell4french)
# HYPER ------
override<- TRUE

##---------------------------------------------------------------
##                  Apply synthethic mistakes                   -
##---------------------------------------------------------------

info(logger, "Reading Dataframe ...")

# FILES ------
df_no_touched <- disk.frame(PATHS[1])
df_unbias_constant <- disk.frame(PATHS[2])
df_unbias_multiply <- disk.frame(PATHS[3])

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

# ITERATE OVER DATA OPTIONS ------
for(data_opt_in in seq_along(PATHS)){
  #* Dest ---
  data_opt<- PATHS[data_opt_in]
  outdir <- DEST_PATHS[data_opt_in]
  overwrite_check(outdir,overwrite = override)
  #* Message ---
  info_m <- paste0("Going over data option: ", data_opt)
  info(logger, info_m)
  #* Get disk frame ---
  temp_disk_frame<- disk.frame(data_opt)
  #* Get chunks  ---
  files <- list.files(data_opt, full.names = TRUE)
  files_shortname <- list.files(data_opt, full.names = FALSE)
  cid = get_chunk_ids(temp_disk_frame, full.names = TRUE)
  #* PB ---
  pb<-progress::progress_bar$new(total = length(cid), force = T, clear = FALSE)
  #* IERATE OVER CHUNKS ---
    for(ii in seq_along(cid)){
      cl<-parallelly::makeClusterPSOCK(workers = 10)
      #cl<- parallelly::autoStopCluster(cl)
      future::plan(future::cluster, workers = cl,.cleanup = TRUE, gc = TRUE)
      ds = disk.frame::get_chunk(temp_disk_frame, cid[ii], full.names = TRUE)
      res<- furrr::future_map_chr(ds$Truth, decider)
      # RESET WORKERS
      parallel::stopCluster(cl = cl)
      future::plan(sequential)
      # Write data
      stopifnot({length(res)==length(ds$Truth)})
      df_modified<-data.frame(Truth= ds$Truth, Mistake= res)
      df_modified<-df_modified%>% mutate(eq= ifelse(Truth==Mistake,TRUE,FALSE))
      fst::write_fst(df_modified, file.path(outdir, files_shortname[ii]))
      # MOVE ON
      pb$tick()
      rm(res,df_modified,cl)
      gc()
    }
}

info(logger, "Mistake Application finished ... ")



# DO FUTURE ------

# OLD WAY
# #sample
# books_df<- df[sample(1:nrow(df), 100),]
# write.table(rownames(books_df), file='10_5_M_sampled.txt', sep='\t', row.names = FALSE)

#   foreach('x'=ds$Truth,
#                       .inorder = FALSE,
#                       .packages= c('purrr','spell4french'),
#                       .combine = 'c',
#                       .options.future = list(chunk.size = 2500)
# ) %dopar% # do in parrallel
#   {
#     # CONNECTION ---
#     # con<-file(paste0(dir_txt,sprintf("/output_%d.txt", Sys.getpid())), open = "a")
#     # APPLY MISTAKE ---
#     synthetic<-decider(x)
#     # MAKE STRUCTURE ---
#     download<- data.frame('Mistakes'=synthetic, 'Truth'=x)
#     # WRITE ---
#     write.table(download,file=con, sep='\t', row.names = FALSE, append = FALSE,col.names = F)
#     ;NULL
#   }
