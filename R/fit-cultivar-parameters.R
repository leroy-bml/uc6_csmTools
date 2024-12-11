#' ######
#' 
#' @param crop
#' @param model
#' 
#' @importFrom DSSAT read_cul read+eco
#' 
#' @return 

get_cdata <- function(crop, model = c("APS","CER","GRO","ARO","CRP","IXM")) {
  
  # DSSAT path and version
  dssat_csm <- gsub("\\", "/", getOption("DSSAT.CSM"), fixed = TRUE)
  dssat_path <- sub('DSCSM.*', "", dssat_csm)
  dssat_vers <- sub(".*DSCSM", "", sub("\\..*", "", dssat_csm))
  
  #
  crop_code <- paste0(substr(toupper(crop), 1, 2), model, dssat_vers)
  
  # Open genotype files
  filec <- read_cul(file_name = paste0(dssat_path, "Genotype\\", crop_code, ".CUL"))
  #ilee <- read_eco(file_name = paste0(dssat_path, "Genotype\\", crop_code, ".ECO"))
  
  # Output
  out <- filec
  #out <- list()
  #out$CUL <- filec
  #out$ECO <- filee
  attr(out, "crop") <- crop
  #attr(out, "model") <- model
  return(out)
  
}