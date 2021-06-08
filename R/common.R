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
# For 1_integrate ---
remove_non_fr<- function(x){
  detected_lan<- detect_language(x)
  temp<- x[detected_lan=='fr' & !is.na(detected_lan)]
  return(temp)
}
