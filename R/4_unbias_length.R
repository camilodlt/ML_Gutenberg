  ##################################################################
  ##                  Step 4: Unbias the lengths                  ##
  ##################################################################

  source("R/common.R")

  # Caller
  step <- "Step 4: Unbias the lengths"
  # Purpose
  explanations <- "Models reproduce data biases.
  We don't want our model to only learn to make long sentences.
  Short sentences should also be corrected.
  - From the long sentences, we generate sentences of all sizes
  - Whether to tokenize or replace the sentences is a parameter
  # that means that a sentence can either serve to generate sentences of various sizes
  # (used multiple times, but different length)
  # or be used only one time for an specific size
  # (used one time)
  - Save to disk
  "
  print(banner(step))
  print(boxup(explanations, centre = F))

  # LIBRARIES ------
  suppressMessages(library(furrr))
  suppressMessages(library(purrr))
  suppressMessages(library(future))
  suppressMessages(library(dplyr))
  suppressMessages(library(tokenizers))
  suppressMessages(library(disk.frame))
  suppressMessages(library(progress))

  #setup_disk.frame(workers = 2)
  # HYPER ------
  hyper1 <- '
  # This script tries to reduce the bias towards large sentences.
  # if keep_size = "constant": 1 subset of each sentence will be used as the Truth sentence
  # its size will vary between one word and all the words (sampled)
  # if keep_size= "multiply": a sentence with n words will have n subsets (one with one word,
  # another with 2 words, ..., until the maximum number of words in the dataset)'
  #* Define ---
  keep_size <- "multiply" # constant or multiply or no_touched
  #* Print to console ---
  print(boxup(hyper1, centre = F))
  print(sprintf("Keep_size set to: %s", keep_size))

  # FILES ------
  info(logger, "Loading data...")
  # last_file<-last_version("data_working/", pattern = ".feather",overall_pattern = "ML_formatted")
  # # READ FEATHER ------
  # df<- feather::read_feather(last_file)
  # # TO RM
  # df<- df[1:2000000,]
  df.frame <- disk.frame("data_working/3.5_ML_formatted")
  print(sprintf("Number of chunks: %s", nchunks(df.frame)))

  ## ---------------------------------------------------------------
  ##          Sample ngrams of diff size from sentences           -
  ## ---------------------------------------------------------------

  info(logger, "WORD COUNT PER SENTENCE")
  # WORD COUNT PER SENTENCE ------
  print("# sentence per word count")
  freq <- cmap(df.frame, ~ count_words(.x[, "Truth"]$Truth), lazy = FALSE) %>% unlist()
  table(freq) %>% print()
  n_sen <- nrow(df.frame)
  print(sprintf("Number of sentences: %s", n_sen))
  max_word <- max(freq)
  rm(freq)

# FN
 unbias_by_chunk<- function(sentences, ids,...) {
   #on.exit({rm(list = ls(1),pos=1);rm(list=ls());unlink(paste0(normalizePath(tempdir()), "/", dir(tempdir())), recursive = TRUE);gc()})

    #sentences <- chunk[, "Truth"]$Truth #  chunk or batch
    #ids <- chunk[, "id"]$id #  chunk or batch

    # iterate over chunk
    new_sentences_df <- map2_dfr(sentences, ids, function(x, y) {
      max_word <- tokenizers::count_words(x)
      df <- data.frame("ngram" = tokenizers::tokenize_ngrams(x, n_min = 1, n = max_word, simplify = TRUE))
      df <- df %>% mutate(count = tokenizers::count_words(ngram))
      df <- df %>%
        group_by(count) %>%
        tidyr::nest(data = c(ngram))
      df <- df %>%
        mutate("one" = map_chr(data, ~ sample(.[[1]], 1))) %>%
        ungroup()
      df$id <- y
      df <- df %>% select(id, "Truth" = one)
      return(df)
    })
    return(new_sentences_df)
 }

# SAMPLE DIFFERENT SIZES ------
info(logger, "Sampling sentences...")
if (keep_size == "constant") {
  # KEEP SIZE == "Constant" ------
  info(logger, "Constant strategy selected")
  df.frame_caller <- cmap(
    df.frame,
    function(chunk) {
      sentences <- chunk[, "Truth"]$Truth #  chunk or batch
      # iterate over chunk
      new_sentences <- map_chr(sentences, function(x) {
        x <- tokenizers::tokenize_ngrams(x, n_min = 1, n = 16, simplify = T)
        return(sample(x, 1))
      })
      new_sentences <- as_tibble(list(
        "id" = chunk[, "id"]$id,
        "Truth" = new_sentences
      ))
      return(new_sentences)
    }
  )
  df.frame_caller %>% compute(outdir = "data_working/4_unbias_constant", overwrite = TRUE)
  state <- 1 # No problem
} else if (keep_size == "multiply") {
  # KEEP SIZE == "Multiply" ------
  info(logger, "Multiply strategy selected")
  outdir <- "data_working/4_unbias_multiply/"
  overwrite_check(outdir,overwrite = TRUE)
  files <- list.files("data_working/3.5_ML_formatted/", full.names = TRUE)
  files_shortname <- list.files("data_working/3.5_ML_formatted/", full.names = FALSE)
  cid = get_chunk_ids(df.frame, full.names = TRUE)
  pb<-progress::progress_bar$new(total = length(cid), force = T)
  for(ii in seq_along(cid)){
    cl<-parallelly::makeClusterPSOCK(workers = 11)
    cl<- parallelly::autoStopCluster(cl)
    future::plan(future::cluster, workers = cl)
    ds = disk.frame::get_chunk(df.frame, cid[ii], full.names = TRUE)
    res<-furrr::future_pmap_dfr(ds,~unbias_by_chunk(sentences = ..2, ids = ..1)) # careful
    fst::write_fst(res, file.path(outdir, files_shortname[ii]))
    pb$tick()
    rm(list = "cl")
    gc()
  }
  state <- 1 # No problem
} else if (keep_size == "no_touched") {
  # KEEP SIZE == "no_touched" ------
  info(logger, "'Leave as is' strategy selected")
  state <- 1 # No problem
} else {
  # wrong KEEP SIZE  ------
  message("keep_size method not defined")
  state <- 0 # problem
}

## ---------------------------------------------------------------
##                        Write to disk                         -
## ---------------------------------------------------------------
# if (state > 0) {
#   info(logger, "Writing to disk")
#   # Write feather ---
#   #* feather
#   filename <- get_versioned_file_name("data_working/", paste("4_Unbiased", keep_size, sep = "_"), file_suffix = ".feather")
#   write_feather(df, path = filename)
# }
