#################################################################
##          Functions for applying synthetic mistakes          ##
#################################################################

# LIBRARIES ------
library(spell4french)
# INIT NATURAL MISTAKES
  #* This function applies fat_finger mistakes to a word ---
  # Fat fingerness is a parameter
  # faat_finger = 0.1 => for each char, prob 10% that it'll be concatenated with its neighbors ("a" with "z" for ex)
my_natural<- spell4french:::natural_mistakes(word= "", char_modification_prob = c(0,1), # pass all words to the fat finger_decider
                             max_neighbor_size=2,sample_neighbor_prob = c(0.95,0.05),
                             fat_finger=0.1,return_fun = TRUE)

  #* Decider for a sentence ---
  # 65 % chances that the sentence will pass through the decider word function
  # returns the sentence modified or not
decider <- function(x, sep= " ",...){
  rand<-sample(0:1, size = 1, prob = c(0.35, 0.65))
  if(rand==0){
    return(x)
  }
  else {
    words<-unlist(strsplit(x, sep))
    synthetic<-map_chr(words,~ decider_word(.),...)
    synthetic<- paste0(synthetic, collapse = sep)
    return(synthetic)
  }
}
  #* Decider for a each word ---
  # 30% % chances that the word will pass through the apply_depth_multiple
  # Exclusively, Natural mistakes are preferred (50 % of the time, if the word is to be modified)
  # Apply multiple depth accepts multiple transformations. (functions)
  # returns the word (non modified) or the word modified
decider_word<- function(x,modification_prob= 0.3,
                        functions=  c(my_natural,"de_split","deletes","transposes","replaces","insertions")){
  rand<-sample(0:1, size = 1, prob = c(1-modification_prob, modification_prob))
  if(rand==0){
    return(x)
  } else {
    to_sample<- sample(1:6,1, prob=c(0.5, 0.3, 0.15,0.05,0.0,0.0)) # more natural mistakes
    if(to_sample==1){
      sampled<- functions[to_sample]
    } else{
      sampled<- sample(functions, to_sample)
    }
    print(sampled)
    mistake <- spell4french:::apply_depth_multiple(sampled, x,ret1=TRUE)
    return(mistake)
  }
}

# EX:
#
#
#
#
