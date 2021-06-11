#################################################################
##              Step 4: Create synthetic mistakes              ##
#################################################################

source("R/common.R")
# Caller
step <- "Step 4: Create synthetic mistakes"
# Purpose
explanations <- "Phrases have now a maximum length (chars).
- We check this length (char and words)
- Normal looking but with outliers. Remove upper outliers.
- Create Truth and Mistakes columns. (X, Y)
"
print(banner(step))
print(boxup(explanations, centre = F))

# LIBRARIES ------
library(purrr)
library(doFuture)
library(future.callr)
library(spell4french)

##-------------------------------------------------------
##  Apply testing partitions to the persisted model     -
##-------------------------------------------------------

info(logger, " Apply testing partitions to the persisted model...")

#testing_df <- read_feather(get_versioned_file_name("data_working", "enriched", ".feather")) %>%
#  filter(!training_ind) %>%
#  dplyr::select(-training_ind)
#
#mod <- readRDS(get_versioned_file_name("models", "readmissions", ".mod"))
#testing_df <- testing_df %>% cbind(prediction = predict(mod, testing_df, type = "response")) %>% tibble()

##----------------------------------------------------------------------------------------------
##  Write scored results to data_output/ directory with version-controlled naming convention   -
##----------------------------------------------------------------------------------------------

#testing_df %>% write_csv(get_versioned_file_name("data_output", "testing_w_predictions", ".csv"))
