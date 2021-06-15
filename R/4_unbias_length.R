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
suppressMessages(library(purrr))
suppressMessages(library(dplyr))
suppressMessages(library(tokenizers))
suppressMessages(library(progress))
suppressMessages(library(feather))

# HYPER ------
hyper1<-'
# This script tries to reduce the bias towards large sentences.
# if keep_size = "constant": 1 subset of each sentence will be used as the Truth sentence
# its size will vary between one word and all the words (sampled)
# if keep_size= "multiply": a sentence with n words will have n subsets (one with one word,
# another with 2 words, ..., until the maximum number of words in the dataset)'
#* Define ---
keep_size= "multiply" # constant or multiply
#* Print to console ---
print(boxup(hyper1, centre = F))
print(sprintf("Keep_size set to: %s", keep_size))

# FILES ------
info(logger, "Loading data...")
last_file<-last_version("data_working/", pattern = ".feather",overall_pattern = "ML_formatted")
# READ FEATHER ------
df<- feather::read_feather(last_file)

##---------------------------------------------------------------
##          Sample ngrams of diff size from sentences           -
##---------------------------------------------------------------

info(logger, "WORD COUNT PER SENTENCE")

# WORD COUNT PER SENTENCE ------
print("# sentence per word count")
table(count_words(df$Truth))%>% print()
n_sen<- nrow(df)
max_word <- max(count_words(df$Truth))
print(sprintf("Number of sentences: %s", n_sen))

# SAMPLE DIFFERENT SIZES ------
# KEEP SIZE == "Constant" ------
info(logger, "Sampling sentences...")
if (keep_size=="constant"){
pb<-progress_bar$new(total=nrow(df),force = TRUE)
df$Truth<- map_chr(df$Truth, function(x){
  pb$tick()
  x= tokenize_ngrams(x, n_min =1 ,n=16, simplify = T)
  return(sample(x,1))
})
  state=1 # No problem
} else if (keep_size=="multiply"){
  pb<-progress_bar$new(total=nrow(df),force = TRUE)
  df<-map2_dfr(df$Truth,df$id, function(x,y){
    pb$tick()
    max_word= count_words(x)
    df <- data.frame("ngram"=tokenize_ngrams(x, n_min =1 ,n=max_word, simplify = TRUE))
    df <- df%>% mutate(count= count_words(ngram))
    df<- df %>% group_by(count)%>% tidyr::nest(data=c(ngram))
    df<- df%>% mutate("one"= map_chr(data, ~sample(.[[1]],1)))%>% ungroup()
    df$id<- y
    df <- df%>% select(id,"Truth"=one)
    return(df)
  })
  state=1 # No problem
} else{
  message("keep_size method not defined")
  state<- 0 # problem
}

##---------------------------------------------------------------
##                        Write to disk                         -
##---------------------------------------------------------------
if(state>0){
info(logger, "Writing to disk")
# Write feather ---
  #* feather
filename<- get_versioned_file_name("data_working/", paste("4_Unbiased", keep_size, sep="_"), file_suffix = ".feather")
write_feather(df,path = filename)

}
