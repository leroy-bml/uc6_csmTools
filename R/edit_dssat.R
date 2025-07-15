#' Add new cultivar to the CUL table
#' 
#' @param cul a data frame/tibble with cultivar data, as obtained with DSSAT::read_cul
#' @param ccode a length 1 character with the 6-character cultivar code that links to DSSAT file X
#' @param cname a length 1 character with the cultivar name
#' @param ecode a length 1 character with the 6-character ecotype code that links to DSSAT ECO file
#' @param ppars a length 5 numeric vector containing phenology parameters (VSEN, PPSEN, P1, P5, PHINT)
#' @param gpars a length 4 numeric vector containing growth parameters (GRNO, MXFIL, STMMX, SLAP1)
#' 
#' @return a tibble; input table with the added cultivar as last row
#' 
#' @importFrom magrittr %>%
#' @importFrom stringr str_detect
#' @importFrom tibble add_row
#' 
#' @export
#'

add_cultivar <- function(cul, ccode, cname, ecode, ppars, gpars) {
  
  # TODO: handle the read_cul part, with "model" argument
  
  if(sum(str_detect(cul$`VAR#`, ccode)) > 0 | sum(str_detect(cul$VRNAME, cname)) > 0) {
    stop("This cultivar is already in the data. Use the function 'adj_cpars' to modify its parameter values")
  }
  
  # Abort if ecotype does not exist
  # if(!ecode %in% eco[[1]]) {
  #     stop("The specified ecotype was not found in the database. Please specify an existing ecotype or add a new one.")
  # }
  
  cul <- cul %>% 
    add_row(`VAR#` = ccode, VRNAME = cname, `ECO#` = ecode, EXPNO = ".",
            # Phenology parameters
            VSEN = ppars[1], PPSEN = ppars[2], P1 = ppars[3], P5 = ppars[4], PHINT = ppars[5],
            # Growth parameters
            GRNO = gpars[1], MXFIL = gpars[2], STMMX = gpars[3], SLAP1 = gpars[4])
  
  return(cul)
}