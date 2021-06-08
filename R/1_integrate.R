##-------------------------------------------------------------------------------------------------------------------------------------------------
##  Step 1: Data Integration: Import donwloaded Books. Remove NA books -
##                                                                                       -
##-------------------------------------------------------------------------------------------------------------------------------------------------
# Source common
source("R/common.R")

# Caller
step <- "Step 1. Data integration and cleaning"
# Purpose
explanations <- "
 We have a lot of books in batches in rds format. \n
 1. Import books. Check for empty books. Check encoding. Discard problems. Strip. remove book id.\n
 2. Flatten them. Tokenize sentences.\n
 3. Detect language. Keep only french. Remove non latin chars. \n
"
print(banner(step))
print(banner(explanations,emph = F))
# Load Libraries ---
info(logger, "Loading libraries")
library(cld2)
library(dplyr,quietly = TRUE,warn.conflicts = F) %>% suppressWarnings()
library(gutenbergr)
library(stringi)
library(stringr)
library(purrr)
library(tokenizers)

##---------------------------------------------------------------
##                          Integrate                           -
##---------------------------------------------------------------
# IMPORT FILES ------
  # Paths ---
info(logger, "Loading rds files")
dir_set<- "data_input/books_rds"
files_paths<-list.files(path=dir_set, full.names= TRUE)
files_paths<- files_paths[!files_paths=="data_input/books_rds/log_0_get_input_books.txt"]
cat('Files in path: ', length(files_paths), '\n')
  # Reading ---
books<- map(files_paths, function(x){
  temp<- readRDS(x)
  closeAllConnections()
  return(temp)
})

  # Formatting output to list ---
books<- flatten(books)
  # Plotting Lines per book ---
length_per_book=unlist(map(books, ~nrow(.x)))
plot(length_per_book) # plot number lines per book
# CHECK NA BOOKS ------
temp <- unlist(map(books, ~any(is.na(.x$text))))
sprintf('NA BOOKS ("books with no text") : %d',sum(temp)) %>% print()
books<-books[!temp]
info(logger, "NA books removed")
# CHECK ENCODINGS ------
info(logger, "Checking encodings")
enc_check<-map_lgl(books,~stri_enc_isutf8(stri_flatten(.x$text)))
sprintf('Number of books that are not UTF-8 : %d',sum(enc_check)) %>% print()
books<- books[-which(enc_check==FALSE)]
info(logger, "Non UTF-8 removed, if any.")
# NAME BOOKS ------
books_id<-map_chr(books, ~unique(.x$gutenberg_id))
cat('Unique Books IDS = number of books')
(length(unique(books_id))==length(books))%>% cat
books<- set_names(books, books_id)
#* Drop id column | turn dataframes to char vectors ---
info(logger, "Retain character Vectors")
books<- map(books, ~.x%>% dplyr::pull(text))
# STRIP BOOKS ------
info(logger, "Strip books")
books<-modify(books, ~gutenberg_strip(.x)) # problem on 4688, github issue.
#* Remove empty strings ---
info(logger, "Remove empty character vectors (lines)")
books<-modify(books,~.x[!stri_isempty(.x)]) # remove empty strings inside character vectors
#* Remove empty vectors ---
info(logger, "Remove empty character vectors (books)")
temp<- map_lgl(books, is_empty)
books<- books[!temp]
# FLATTEN AND SPLIT ------
info(logger, "Flatten books")
books<- modify(books, ~stri_flatten(.x,collapse = ' ') )
# TOKENIZE SENTENCES ------
info(logger, "Tokenize sentences")
books<- tokenize_sentences(books, lowercase = FALSE, strip_punct = FALSE, simplify = TRUE)
# DETECT LANGUAGE & remove non French ------
info(logger, "Remove non french sentences")
books<-map(books, remove_non_fr)
# REMOVE SPECIAL CHARS ------
info(logger, "Retain only latin + accepted chars")
books<- modify(books, ~str_remove_all(.x, "[^\\s\\p{Latin}!?€$,']+"))
books<- modify(books, str_squish)
books<-modify(books,~.x[!stri_isempty(.x)]) # remove empty strings inside character vectors




##---------------------------------------------------------------
##          Write integrated data to the data_working           -
##---------------------------------------------------------------

info(logger, "Writing integrated data to data_working/ directory...")
integrated_data_file_name <- get_versioned_file_name("data_working", "integrated_clean_books",".Rds")
saveRDS(books, integrated_data_file_name)
