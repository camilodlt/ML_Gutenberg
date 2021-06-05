##-------------------------------------------------------------------------------------------------------------------------------------------------
##  Step 0 : Data Integration: Download French Books from the Gutenberg project                                                                   -
##                                                                                                                                                -
##           Using the gutenbergr package.                                                                                                        -
##           Books will be fetched in batches and saved as rds files in input folder.                                                             -
##           These books will be the input data for the models to come.                                                                           -
##-------------------------------------------------------------------------------------------------------------------------------------------------

# NOTES ------
# book with id 4688 was not used. Strip function removed all the content
# https://github.com/ropensci/gutenbergr/issues/27
# If we want this book it has to be manually stripped

# PROCESS ------
# French books are fetched from Gutenbergr
# Encoding is assessed. Explicit encoding is found or guessed
# With the encoding (found or guessed), books are coerced to UTF-8
# UNLESS book is already in explicit UTF-8 format.
# Book information is also printed.
#* LIBRARIES ------
library(gutenbergr)
library(stringr)
library(stringi)
library(dplyr)
library(purrr)

# FUNCTIONS ------

#* DIR TO STORE ---
# ' Looks for data_input/books_rds
# ' Can override or not the folder
# ' A txt file is created inside ONLY if the folder is overwritten or did not exist
# ' Creates a connection to that file named 'log_con'
dir_gutenberg<- function(override=FALSE){
  if (dir.exists("data_input/books_rds") & override==FALSE ){
    warning("DIRECTORY ALDEADY EXIST")
  } else {
    unlink("data_input/books_rds/",recursive = TRUE)
    dir.create("data_input/books_rds/",showWarnings = FALSE,recursive = TRUE)
    file.create("data_input/books_rds/log_0_get_input_books.txt")
    log_con<-file("data_input/books_rds/log_0_get_input_books.txt",open="a")
    return(log_con)}
}

#* MISC AND ENCODING INFORMATION ABOUT BOOKS ---
# ' Info tries to find the encoding of a book
# ' It reads the first 300 lines looking for a regex pattern: '(?<=Character set encoding: ).*'
# ' This pattern was found looking at the books
# ' A lot of books have it
# ' If the pattern is found, encoding is returned.
# ' Book information is passed to log_con
info<- function(book_var, pos){
  writeLines(paste('BOOK :',pos,'------'),log_con)
  enc_found<-map(1:300,~stringr::str_extract(book_var$text[.x],pattern = '(?<=Character set encoding: ).*'))
  names(enc_found)<- 1:300
  enc_found<- enc_found%>% discard(.p = is.na)
  writeLines(names(enc_found),log_con)

  # Log encoding of the book
  writeLines('Declared encoding of the book ---',log_con)
  writeLines(as.character(enc_found),log_con)

  # Log if all lines have bytes that form utf-8 understandable string
  writeLines('is UTF ? ---',log_con)
  writeLines(as.character(all(stringi::stri_enc_isutf8(book_var$text))),log_con)
  # Print encodings
  writeLines('Encodings as a table ---',log_con)
  writeLines(as.character(table(Encoding(book_var$text))),log_con)
  if(is_empty(enc_found)){
    writeLines('Not declared encoding found',log_con)
    message('Not declared encoding found')}
  return(enc_found)
}
guess_enc<- function(book_var){
  temp<-stringi::stri_enc_detect2(
    stringi::stri_flatten(
      gutenberg_strip(book_var$text)), 'fr')[[1]][[1]][1]
  writeLines('Encoding guessed:',log_con)
  writeLines('temp',log_con)
  return(temp)
}

# ' latin-1 is very common
correct_enc<- function(book_var, enc_found){
  if(!enc_found=='UTF-8'){ # COERCE PROCEDURE
    if(enc_found=='LATIN-1'){ # if declared latin-1 => to iconv is LATIN1 (better not to guess)
      temp_text<- try(iconv(book_var$text, from='LATIN1', to= 'UTF-8'))
    } else { # other enc_found => use it as is
      temp_text<- try(iconv(book_var$text, from=enc_found, to= 'UTF-8'))} # try with given encoding
    if(class(temp_text)=='try-error'){ # if error 1st round
      writeLines("can't coerce with given encoding",log_con)
      guess<-guess_enc(book_var) # try guessing
      temp_text<- try(iconv(book_var$text, from=guess, to= 'UTF-8')) # Try iconv again
      if(class(temp_text)=='try-error'){ # if error again.. NA
        temp_text<- NA
        writeLines("Can't coerce with guessed encoding either",log_con)
      } else { # if no error in 2nd round: Coerced worked. print that it worked
        writeLines("coerced to UTF-8 with guessed encoding",log_con)
      }}
    else {writeLines("coerced to UTF-8",log_con)} # it worked the first time

  } else  { # already UTF-8
    temp_text<- book_var$text
    writeLines("Encoding ok, Nothing done",log_con)
  }
  return(temp_text)
}

#####

#* CREATE DIRECTORIES AND LOG FILES -----
log_con<-dir_gutenberg(override = TRUE)
#* GUTEMBERG -----
gutenberg_fr <- gutenberg_languages %>% filter(language == 'fr') # fr list of books
mirror<- gutenberg_get_mirror() # best mirror
#* CREATE BATCHES ------
docs_to_download<-nrow(gutenberg_fr) # How many books to download
batch<-seq(1,docs_to_download,100) # make a sequence, 100 books batches
if(max(batch) > docs_to_download) {batch[which(batch==max(batch))]<- docs_to_download+1} else {batch<- append(batch, docs_to_download+1)} # correct the sequence
  # Info printing
sprintf("Number of books: %i", docs_to_download)
print("Batches:")
print(batch)

#* ITERATE OVER BATCHES ---
pre_path<- "data_input/books_rds/"
for(position in 1:length(batch)){ # For batches
  # MAKE A SEQUENCE TO DOWNLOAD ---
  if(position+1 <= length(batch)){ # It's at most, the last batch
  books_to_download<- batch[position]:(batch[position+1]-1)
  books<- map(books_to_download,~gutenberg_download(gutenberg_id = gutenberg_fr[.x,], mirror = mirror,
                                                    verbose = TRUE, strip = FALSE)) # get the sequence of books in a list
  books<-books%>% discard(~nrow(.)<1) # Some books doesn't have data
  #* Loop the sequence of books ---
  for(i in 1:length(books_to_download)){ # Go one by one and check encoding, re_download if UTF-8 if not coerce. Finally save as RDS.
    real_book_number <- books_to_download[i]
    enc_found<-info(book_var = books[[i]], pos = real_book_number) # Find encoding. If any
    # Encoding correction
    if(is_empty(enc_found)){
      enc_found<- guess_enc(books[[11]]) # Guess if not found
    }
    # Correct
    books[[i]]$text<- correct_enc(book_var = books[[i]],enc_found = stri_trans_toupper(enc_found[[1]]))
    Sys.sleep(100) # per book. Be kind with Gutenberg
  }
  name<- paste0('books_',min(books_to_download),'_', max(books_to_download), '.rds') # Sequence of books fetched
  name<- paste0(pre_path, name)
  saveRDS(object= books, file = name) # save
  print(paste0(name,': SAVED---------')) # Status

  } # Batch indices (e.g. 1...100 for the first batch)
}

# CLOSE CON ---
close(log_con)
print("Connection Closed")
