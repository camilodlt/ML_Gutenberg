#################################################################
##          Functions for applying synthetic mistakes          ##
#################################################################

source("R/common.R")
############
my_natural<- natural_mistakes(word= "hello", char_modification_prob = c(0,1), # pass all words to the fat finger_decider
                             max_neighbor_size=2,sample_neighbor_prob = c(0.95,0.05),
                             fat_finger=0.1,return_fun = TRUE)


decider <- function(x, sep= " ",...){
  rand<-sample(0:1, size = 1, prob = c(0.4, 0.6))
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


#############
decider <- function(x, sep= " ",...){
  rand<-sample(0:1, size = 1, prob = c(0.5, 0.5))
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

decider_word<- function(x,prob= 0.3){
  rand<-sample(0:1, size = 1, prob = c(1-prob, prob))
  if(rand==0){
    return(x)
  } else {
    functions<- c("de_split","deletes","transposes","replaces","insertions")
    to_sample<- sample(1:5)
    sampled<- sample(functions, to_sample)
    mistakes <- apply_depth_multiple(sampled, x)
    mistakes[['orig_word']]<- NULL
    mistakes<- map(mistakes, ~sample(x = ., size = length(mistakes[[1]]), replace = T))
    mistakes= unlist (mistakes)
    mistake= sample(mistakes, size = 1)
    if(is.null(mistake)){mistake<-'ERROR OCCURED'}
    return(mistake)
  }
}

