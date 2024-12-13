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

#' ######
#' 
#' @param ##
#' 
#' @importFrom ###
#' 
#' @return 

set_class <- function(df, classes) {
  df <- df[names(df) %in% names(classes)]
  correct_class <- sapply(names(df), function(x) class(x)[1])
  df <- mapply(function(value, correct_class) {
    switch(correct_class,
           Date = as.Date(value),
           POSIXct = as.POSIXct(value),
           POSIXt = as.POSIXct(value),
           numeric = as.numeric(value),
           integer = as.integer(value),
           character = as.character(value),
           factor = as.factor(value),
           value)
  }, df, classes, SIMPLIFY = FALSE)
  
  return(as.data.frame(df))
}

#' ######
#' 
#' @param trdata
#' @param R
#' @param O
#' @param C
#' @param TNAME
#' @param CU
#' @param FL
#' @param SA
#' @param IC
#' @param MP
#' @param MI
#' @param MF
#' @param MR
#' @param MC
#' @param MT
#' @param ME
#' @param MH
#' @param SM
#' 
#' @importFrom dplyr "%>%" mutate
#' @importFrom tibble add_row
#' 
#' @return 
#'

args <- list(TNAME = "N saturation", MF = 5)

add_treatment <- function(filex, args = list()) {
  
  trt <- filex[[which(startsWith(names(filex), "TREATMENT"))]]
  
  new_row <- trt[nrow(trt),]
  new_row[[1]] <- max(trt[1]) + 1  # set new row primary key
  
  for (col in names(args)) {
    if (col %in% colnames(trt)) {
      new_row[[col]] <- args[[col]]
    } else {
      warning(paste("Column", arg, "not found in the dataframe"))
    }
  }
  
  out <- bind_rows(trt, new_row)
  filex[[2]] <- as_DSSAT_tbl(out)
  
  return(filex)
}

#' ######
#' 
#' @param fdata
#' @param type
#' @param rate
#' @param FDATE
#' @param FNAME
#' @param FMCD
#' @param FACD
#' @param FDEP
#' @param FOCD
#' 
#' @importFrom dplyr "%>%" mutate
#' @importFrom tibble add_row
#' 
#' @return 

filex <- read_filex("C:/DSSAT48/Wheat/KSAS8101.WHX")
section <- "irrigation"
args <- list(EFIR = 1, IDATE = c("1981-05-26","1981-06-24"), IRVAL = c(50,50))
#args <- list(FEDATE = c("1981-05-26","1981-06-24"), FMCD = "FE041", FACD = "AP001", FAMN = 120, FAMP = 0, FAMK = 0, FAMC = 0, FDEP = 1)

add_management(filex = filex, section = "irrigation", args = args)

# TODO: handle composite section (initial_conditions, irrigation); class list for collapsed vars
add_management <- function(filex,
                           section = c("initial_conditions","planting","tillage","irrigation",
                                       "fertilizer","organic_amendment","chemicals","harvest"),
                           args = list()) {
  
  # Match management type to corresponding file X section
  input_nm <- toupper(substr(section, 1, 5))
  sections_pref <- sapply(names(filex), function(x) substr(x, 1, 5))
  matches <- sapply(sections_pref, function(x) identical(input_nm, x))
  
  if (!section_name %in% sections_pref) {
    stop(paste(section, "is not a valid file X section"))
  }
  
  # Set focal section and its elements
  sec_nm <- names(matches[matches])
  sec <- filex[[sec_nm]]  # select focal section
  cols <- names(sec)  # section columns
  classes <- sapply(sec, function(x) class(x)[1])  # classes for section columns

  # Format args as a new row for the specified section
  new_row <- setNames(as.list(rep(NA, length(cols))), cols)
  for (col in names(args)) {
    if (col %in% cols) {
      new_row[[col]] <- args[[col]]
    } else {
      warning(paste("Column", arg, "not found in the dataframe"))
    }
  }
  
  new_row <- set_class(df = new_row, classes = classes)
  new_row[[1]] <- max(sec[1]) + 1  # set new row primary key
  
  # If collapsible section:
  nested_cols <- names(classes[classes %in% "list"])
  
  if (length(nested_cols > 0)){
    classes_nested <- sapply(sec[nested_cols], function(x) sapply(x, function(x) class(x)[1]))
    new_row_lss <- set_class(df = new_row, classes = classes_nested)
    new_row <- cbind(
      new_row[, !(names(new_row) %in% intersect(names(new_row), nested_cols))],
      new_row_lss
    )
  }
  
  # Bind new row to section data
  if (section %in% c("initial_conditions","irrigation")){  # nested sections
    sec <- unnest(sec, cols = all_of(nested_cols))
    out <- bind_rows(sec, new_row) 
    out <- collapse_cols(new_row, names(new_row_lss))
  } else {
    out <- bind_rows(sec, new_row) 
  }
  
  filex[[sec_nm]] <- as_DSSAT_tbl(out)

  return(filex)
}


#' ######
#' 
#' @param fdata
#' @param type
#' @param rate
#' @param FDATE
#' @param FNAME
#' @param FMCD
#' @param FACD
#' @param FDEP
#' @param FOCD
#' 
#' @importFrom dplyr "%>%" mutate
#' @importFrom tibble add_row
#' 
#' @return 
#' 

disable_stress <- function(filex, stress = c("irrigation","nitrogen")){
  
  if("irrigation" %in% stress){
    filex[["SIMULATION CONTROLS"]]$IRRIG <- "A"
  } 
  
  if("nitrogen" %in% stress){
    # Set N saturation fertilizer regime
    max_n <- list(FDATE = c("1981-10-29","1982-03-24","1981-06-10"),
                  FMCD = rep("FE041", 3), FACD = rep("AP001",3 ), FDEP = 1,
                  FAMN = 120, FAMP = 0, FAMK = 0, FAMC = 0, FAMO = 0)
    filex_n <- add_management(filex, section = "fertilizer", args = max_n)
    
    max_n_trt <- list(TNAME = "N saturation", MF = 5)
    filex <- add_treatment(filex_n, args = max_n_trt)
  }
  
  #TODO: add control and iteration until threshold
  
  return(filex)
} 


disable_stress(filex)
