#################################################################
##                 3.5 : DISK FRAME converter                  ##
#################################################################

# LIBRARIES
suppressMessages(library(disk.frame))
suppressMessages(library(purrr))
suppressMessages(library(dplyr))
suppressMessages(library(feather))
disk.frame::setup_disk.frame()
# Common
source("R/common.R")

# FILES ------
last_file<-last_version("data_working/", pattern = ".feather",overall_pattern = "ML_formatted")
# READ FEATHER ------
df<- feather::read_feather(last_file)
# TO DISK FRAME ------
disk_df<-as.disk.frame(df, outdir = "data_working/3.5_ML_formatted", overwrite = T, nchunks = 2000)
# RM all
rm(list = ls())
.rs.restartR()
