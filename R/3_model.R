#################################################################
##             Step 3: Machine Learning Prepartion             ##
#################################################################

source("R/common.R")
# Caller
step <- "Step 3: Machine Learning Prepartion"
# Purpose
explanations <- "Phrases have now a maximum length (chars).
- We check this length (char and words)
- Normal looking but with outliers. Remove upper outliers.
- Create Truth and Mistakes columns. (X, Y)
"
print(banner(step))
print(boxup(explanations, centre = F))

# LIBRARIES ------
suppressMessages(library(purrr))
suppressMessages(library(ggplot2))
suppressMessages(library(dplyr))
suppressMessages(library(data.table))
suppressMessages(library(feather))

# LOADING ------
info(logger, "Loading enriched data.")
  #* Import ---
to_import<-last_version("data_working/",pattern = ".Rds","enrich")
books<-readRDS(to_import)

# PARAMS ------
remove_upper<-TRUE
remove_lower<-FALSE
sprintf("Remove Upper: %s",as.character(remove_upper))%>% print()
sprintf("Remove Lower: %s",as.character(remove_lower))%>% print()

##----------------------------------------------------------------
##                        Length Checker                         -
##----------------------------------------------------------------

info(logger, "Visualizing Length (char and words) ")
# CHECK ACTUAL LENGTH ------
  #* Check char length -----
chars<- unlist(map(books,nchar))
chars<- data.frame("n_chars"=chars)
ggplot(chars,aes(x=n_chars)) + geom_density(bw=0.5) # pretty normal looking

  #* Check word count -----
# really centered at 10 words by design.
words<-unlist(map(books, ~map_int(strsplit(., " "),length)))
words<- data.frame("n_words"=words)
ggplot(words,aes(x=n_words)) + geom_density(bw=0.5) # pretty normal looking

# DECISION: TRIM 5% extremities (lower and higher) ------
info(logger, "Removing Outliers based on decision parameters")
  # Deciles are not recaculated after decision effects
deciles<-quantile(chars$n_chars, probs = seq(0,1,0.05))
  #* Lower ---
if(remove_lower){
  th_l<- deciles[2] # the 5 %
  n= (chars$n_chars<th_l)%>% sum
  cat("We keep every char vector that has more than ", th_l, " characters \n")
  # Remove lower --
  books<- map(books, ~discard(.,~ nchar(.)< th_l))
  sprintf("N lines removed:%i: ", n)%>% print()
}
  #* Upper ---
if(remove_upper){
  th_h<- deciles[20] # the 95th
  n= (chars$n_chars>th_h)%>% sum
  cat("We keep every char vector that has less than ", th_h, " characters (95th decile)\n")
  # Remove upper --
  books<- map(books, ~discard(.,~ nchar(.)> th_h))
  sprintf("N lines removed: %i ", n)%>% print()

}

# CHECK LENGTH AFTER OUTLIER TREATMENT ------
info(logger, "Checking Length after outlier treatment")
  #* Plotting
chars<- unlist(map(books,nchar))
chars<- data.frame("n_chars"=chars)
ggplot(chars,aes(x=n_chars)) + geom_density(bw=0.5)+ # pretty normal looking. No tails
  ggtitle("After Outlier removal")

##----------------------------------------------------------------
##                          X and Target                         -
##----------------------------------------------------------------

info(logger, "Creating Dataframe, data.table and feather")

# TO DATAFRAME ------
books_df<-imap_dfr(books, function(x,y){
  temp_df<- as.data.frame(books[[y]])
  names(temp_df)<- c("Truth")
  temp_df[["id"]] <- y
  temp_df[["Mistakes"]]<- temp_df[["Truth"]]
  return(temp_df)
})
books_df <- books_df%>% relocate(id, Mistakes, Truth)

# TO DT ------
books_dt <- as.data.table(books_df)

# PRINT INFO ------
sprintf("NROWS: %i", nrow(books_df))
sprintf("NROWS in millions: %i M", floor(nrow(books_df)/1000000))
sprintf("NCOLS: %i", ncol(books_df))
sprintf("Column names: %s", paste(names(books_df), collapse = ", "))
cat("Head \n")
books_df%>% head()%>% as_tibble()%>% print

##----------------------------------------------------------------
##                    Saving to data_working                     -
##----------------------------------------------------------------

info(logger, "Writing to disk : DF, DT, Feather")
# WRITE TO DISK ------
  #* Dataframe
filename<- get_versioned_file_name("data_working/", "ML_formmated_DF", file_suffix = ".Rds")
saveRDS(books_df,file = filename)
  #* Data.table
filename<- get_versioned_file_name("data_working/", "ML_formmated_DT", file_suffix = ".Rds")
saveRDS(books_dt,file = filename)
  #* feather
filename<- get_versioned_file_name("data_working/", "ML_formmated_Feather", file_suffix = ".feather")
write_feather(books_df,path = filename)

