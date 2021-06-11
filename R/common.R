# Common Libraries
#library(tidyverse)
#library(snakecase)
#library(feather)
library(stringr)
suppressWarnings(library(log4r,warn.conflicts = F,quietly = T))
library(bannerCommenter)
#library(here)
source(str_c(getwd(), "/R/utilities.R"))

# Constants
project_name <- "ML_GutenbergFR"

# Global options
options(scipen = 1000)

# Common logger
logger <- log4r::logger()

# FUNCTIONS ------
last_version <- function(path, pattern, overall_pattern){
  pat= paste0(".{19}(?=",pattern,")")
  files<- list.files(path, full.names = T)
  files[!str_detect(files, overall_pattern)]<- ""
  dates<-str_extract_all(files, pat)
  dates<- dates %>%purrr::compact()
  #as.POSIXlt(files, format = "%Y-%m-%d_%H:%M:%S")
  ordering<- as.character(dates)%>% order()
  pattern_to_look<-dates[which.max(ordering)]
  last_file<- files[str_detect(files,pattern_to_look[[1]])]
  return(last_file)
}

# For 1_integrate ---
remove_non_fr<- function(x){
  detected_lan<- detect_language(x)
  temp<- x[detected_lan=='fr' & !is.na(detected_lan)]
  return(temp)
}
# Functions for 2_enrich ---
tokenize_if_length<-function(x){
  if(nchar(x)>50){
    temp<- tokenize_ngrams(x,n_min=10, n=10, simplify = TRUE)
    n_to_keep <- max(round(length(temp)/6),1) # at least 1
    temp<- sample(temp, size = n_to_keep) # keep 1/6th of the ngrams
    return(temp)} else {
      return(x)
    }
}

