######################################################################
##  Step 2: Data Enrichment. Max length + verify if in French dict  ##
######################################################################

source("R/common.R")

# Caller
step <- "Step 2. Data Enrichment."
# Purpose
explanations <- "
Continue to clean, ML models specially in mind.\n
1. Max length for lines
2. Ensure French. This time we ensure that lines have words that appear in the french dict.
as opposed to when cld2 guessed the language
This step is an extra insurance to be sure that words are correct.
A threshold is defined.
\n
Output:
  - Same list of vectors.
  - Elements of character vectors have a max nchar length
  - + are, at least, composed of 80% of french words in spell4french dict.
"
print(banner(step))
print(boxup(explanations, centre = F))

# IMPORTS AND READS ------
# Last step : Clean books ---
info(logger, "Loading integrated clean books")
integrate_to_load <-
  last_version("data_working/", ".Rds", overall_pattern = "integrated")
books<-readRDS(integrate_to_load)
# Libraries ---
info(logger, "Loading libraries")
library(purrr)
library(cld2)
library(tokenizers)
library(progress)
library(spell4french)

##----------------------------------------------------------------
##                            Clean                              -
##----------------------------------------------------------------

# CORRECT LENGTH ------
# if length > 50 chars, tokenize
# Keep 1/6 of the ngrams returned
info(logger, "Correcting length ...")
pb <- progress_bar$new(total = length(books))
books <- map(books, function(x) {
  # Print progress
  pb$tick()
  # Separate in fixed word length
  temp <-
    unlist(map(x,  ~ unlist(tokenize_if_length(.x)))) # Keeps 1/6 of the samples if the line is tokenized
  return(temp)
}) # takes a while
# Remove empties ---
books <- books %>% compact()

# REMOVE NON PHRASES THAT ARE NOT IN FRENCH ------
# Language detector ir re-ran items of shorter legnth than previously (in integrate)
info(logger, "Language Detector")
books <- map(books, remove_non_fr)

# REMOVE NON PHRASES THAT ARE NOT IN FRENCH DICT ------
# Accept a max of 20% of words non in dict per ith element of character vector
info(logger, "Check Words in dict")
pb <- progress_bar$new(total = length(books),force = TRUE)
books <- map(books,
             function(x) {
               # Print progress
               pb$tick()
               # check dict
               vect <- compact(# remove NULLs
                 unlist(# get a char vector per book
                   map(x, function(x) {
                     temp <-
                       unique(tokenize_words(x, simplify = T)) # get all the words in ith row of a character vector
                     logic = map_lgl(temp, check_dictionary) # see if the words are present in a french dict
                     do_pass = sum(logic) / length(logic) # % of words present in dict
                     if (do_pass > 0.8) {
                       return(x)
                     } else {
                       return(NULL)
                     } # if > 0.8 return all ith row, else return NULL, it will get removed
                   })))
               return(vect)
             })


##----------------------------------------------
##  Writing cleaned version to data_working/   -
##----------------------------------------------
info(logger, "Writing enriched data to data_working/ directory...")
integrated_data_file_name <-
  get_versioned_file_name("data_working", "enriched_checked_in_dict", ".Rds")
saveRDS(books, integrated_data_file_name)
