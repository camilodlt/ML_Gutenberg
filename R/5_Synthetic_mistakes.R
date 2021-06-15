#################################################################
##              Step 5: Create synthetic mistakes              ##
#################################################################

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
library(purrr)
library(doFuture)
library(future.callr)
library(spell4french)

# HYPER ------
dir_txt<- "data_working/mistakes_txt"
override<- TRUE

##---------------------------------------------------------------
##                  Apply synthethic mistakes                   -
##---------------------------------------------------------------

info(logger, "Reading Dataframe ...")
# FILES ------
# keep_size => constant
last_file<-last_version("data_working/", pattern = ".feather",overall_pattern = "4_Unbiased_constant")
# keep_size => multiply
last_file<-last_version("data_working/", pattern = ".feather",overall_pattern = "4_Unbiased_multiply")
# READ FEATHER ------
df<- feather::read_feather(last_file)

# DIRECTORY ------
info(logger, "Checking directory")
if (dir.exists(dir_txt) & override==FALSE ){ # Exists & not to override
  warning("DIRECTORY ALDEADY EXIST")
  quit(save="ask")
} else if (!dir.exists(dir_txt) | (dir.exists(dir_txt) & isTRUE(override))){ # does not exist OR exist & override
  unlink(dir_txt,recursive = TRUE)
  dir.create(dir_txt,showWarnings = FALSE,recursive = TRUE)
  info(logger, "Directory created")
}


#sample
books_df<- df[sample(1:nrow(df), 100),]
write.table(rownames(books_df), file='10_5_M_sampled.txt', sep='\t', row.names = FALSE)

tic <- Sys.time()
registerDoFuture()
plan(callr, workers = 8)
nothing<-foreach('x'=books_df$Mistakes,'y'=books_df$Truth,'z'=rownames(books_df),
                 .inorder = FALSE,
                 .noexport = c("books_df"),
                 .packages= c('purrr','spell4french'),
                 .combine = 'c',
                 .options.future = list(chunk.size = 2500)
) %dopar% #
  {
    # CONNECTION
    con<-file(paste0(dir_txt,sprintf("/output_%d.txt", Sys.getpid())), open = "a")
    # APPLY MISTAKE
    synthetic<-decider(x)
    download<- data.frame('Mistakes'=synthetic, 'Truth'=y)
    name<- paste0("/home/camilo/Documents/Own Projects/Gutenberg/Books/synthetic/test.",z,".txt")
    write.table(download,file=name, sep='\t', row.names = FALSE, append = FALSE)
    ;NULL
  }


info(logger, " Apply testing partitions to the persisted model...")
