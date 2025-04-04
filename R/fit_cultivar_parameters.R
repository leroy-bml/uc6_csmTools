library(DSSAT)
library(dplyr)
library(tidyr)
library(ggplot2)
library(parallel)
library(stringr)
library(glue)


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
filex$`SIMULATION CONTROLS`$SMODEL <- "WHAPS"
write_filex(filex, "C:/DSSAT48/Wheat/KSAS8101.WHX")
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


#' ######
#' 
#' 

write_gluebatch <- function(...){
  
  # if(is.null(dir_out)){
  #   dir_out <- "C:/DSSAT48/GLWork"
  #   message("Output directory not specified. Setting to default C:/DSSAT48/GLWork")
  # }
  
  # Make batch table
  batch_tbl <- data.frame(FILEX = filex,
                          TRTNO = trtno,
                          RP = rp,
                          SQ = sq,
                          OP = op,
                          CO = co)

  header <- c('%-92s', rep('%7s',5)) %>%  # fixed columns widths
    sprintf(c('@FILEX','TRTNO','RP','SQ','OP','CO')) %>%  # column headers
    str_c(collapse = '') %>%  # collapse into a single column header line
    c(paste0('$BATCH(CULTIVAR):', crop_code, ingeno, " ", cultivar), "", .)  # add batch file header label
  
  column_output <- batch_tbl %>%
    mutate(FILEX = sprintf('%-92s', FILEX)) %>%   # expand filex name to full width
    mutate_at(vars(-FILEX), ~sprintf('%7i', .)) %>%  # write out all other columns
    glue_data('{FILEX}{TRTNO}{RP}{SQ}{OP}{CO}')  # combine columns into character vector
  
  batch_output <- c(header, column_output)  # combine header lines and column data
  batch_name <- paste0(cultivar, ".", sprintf("%2s", crop_code), "C")  # set cultivar batch file name
  batch_path <- paste0(dir_out, "/", batch_name)

  write(batch_output, file = batch_path)  # write batch_output to file
  
  return(batch_name)
}


#' ######
#' 
#' 
#' 

write_gluectrl <- function(model, ...){

  controls <- data.frame(
    Variable =
      c("CultivarBatchFile","ModelID","EcotypeCalibration","GLUED","OutputD","DSSATD","GLUEFlag","NumberOfModelRun","Cores","GenotypeD"),
    Value =
      c(batchfile, model, ecocal, dir_glue, dir_out, dir_dssat, flag, reps, cores, dir_genotype)
  )

  filepath <- paste0(dir_glue, "/SimulationControl.csv")
  write.csv(controls, filepath, row.names = FALSE)
  
  return(invisible(controls))
}

#' ######
#' 
#' 
#' 

check_glue_files <- function(...){
  
  sys <- Sys.info()
  os <- sys[["sysname"]]
  
  reqs <- switch(
    os,
    "Windows" = c("MODEL.ERR","SIMULATION.CDE","DSSATPRO.v48","DETAIL.CDE"),
    "Linux" = c("MODEL.ERR","SIMULATION.CDE","DSSATPRO.v48","DSSATPRO.L48","DETAIL.CDE"),
    "Darwin" = c("MODEL.ERR","SIMULATION.CDE","DSSATPRO.v48","DETAIL.CDE")  # not tested
  )

  dssat_paths <- sapply(reqs, function(x) file.path(dir_dssat, x))
  glue_paths <- sapply(reqs, function(x) file.path(dir_glue, x))
  
  for (i in seq_along(glue_paths)){
    if(!file.exists(glue_paths[i])){
      file.copy(dssat_paths[i], glue_paths[i])
    }
  }
}
  
  
  


#' Run GLUE
#' 
#' 
#' 

calibrate_genparams <- function(filex, cultivar, trtno,
                                pars = c("phenology","growth"),
                                model = NULL,
                                reps = 3, cores = NULL,
                                calibrate_ecotype = FALSE,
                                dir_glue, dir_out, dir_dssat, dir_genotype,
                                overwrite = FALSE,
                                ...){
  
  models <- get_dssat_terms("models")
  crops <- get_dssat_terms("crops")
  
  # Validate the model argument
  model_list <- unique(models[[1]])
  if (!is.null(model) && !(model %in% model_list)) {
    stop("Invalid model input. Please specify a model from the following list: ", paste(model_list, collapse = ", "))
  }
  
  # Load file X
  # TODO: distinct naming between actual files (filex) and file tables in R
  filex_tables <- read_filex(filex)  # read file x
  cultivars <- filex_tables$CULTIVARS
  crop_code <- cultivars[cultivars$CNAME == cultivar]$CR
  crop <- crops[crops$`@CDE` == crop_code,]$DESCRIPTION
  ingeno <- cultivars[cultivars$CNAME == cultivar]$INGENO
  
  # Write GLUE batch file
  batchfile <- write_gluebatch(rp = 1, sq = 0, op = 0, co = 0) #TODO: figure out what these do...
  
  # Set GLUE flag
  flag <- switch( 
    toString(pars),
    "phenology, growth" = 1,
    "phenology" = 2,
    "phenology" = 3,
    stop("Invalid parameter type")
  )
  
  # Set model
  model <- if (is.null(model)) {
    if (!is.na(filex$`SIMULATION CONTROLS`$SMODEL) || filex$`SIMULATION CONTROLS`$SMODEL != -99) {
      filex$`SIMULATION CONTROLS`$SMODEL
    } else {
      stop("Invalid model input. Please specify a model implemented into DSSAT.")
    }
  } else {
    model
  }
  version <- DSSAT:::get_dssat_version() ; print(version)
  model_ver <- paste0(model, sprintf("%03d", as.numeric(version))) ; print(model_ver)
  
  # Set input: calibrate ecotype
  ecocal <- if (calibrate_ecotype) "Y" else "N"
  
  # Set input: number of cores
  cores <- if (is.null(cores)) round(detectCores()/2, 0) else cores
  
  # Write GLUE control files
  write_gluectrl(model = model_ver, batchfile, ecocal, dir_glue, dir_out, dir_dssat, flag, reps, cores, dir_genotype) 
  
  print(dir_genotype)
  
  oldwd <- getwd()
  on.exit(setwd(oldwd))  # ensure the working directory is reset on exit
  setwd(dir_glue)  # set work directory to GLUE directory
  
  check_glue_files()  # check if all required files are present in GLUE dir (if not, copied from DSSAT dir)
  system("Rscript GLUE.r")  # run GLUE
  
  # Format output
  genfile <- paste0(model_ver, ".CUL")
  genpath <- file.path(dir_genotype, genfile)
  outpath <- file.path(dir_out, genfile)
  
  gen <- read_cul(genpath)  # original cultivar file
  fit <- read_cul(outpath)  # new cultivar file
  
  # Write results
  if(overwrite){
    backup_dir <- file.path(dir_genotype, crop, "BackUp")
    backup_path <- file.path(backup_dir, genfile)
    
    if(!dir.exists(backup_dir)){
      dir.create(backup_dir, recursive = TRUE)
    }
    write_cul(gen, backup_path)
    write_cul(fit, genpath)  # overwrite cultivar files in the original location
    message(sprintf("Calibration results written in %s.\nOriginal cultivar file backed-up in %s.", genpath, backup_path))
  }
  
  out <- dplyr::filter(fit, VRNAME == cultivar)  # output the fitted parameters for visualization
  
  return(out)
}


#' TMP: test
#' 

filex <- "C:/DSSAT48/Wheat/KSAS8101.WHX"
cultivar <- "NEWTON"
trtno <- 6
model <- "WHAPS"
dir_glue <- "C:/DSSAT48/Tools/GLUE"
dir_out <- "C:/DSSAT48/GLWork"
dir_dssat <- "C:/DSSAT48"
dir_genotype <- "C:/DSSAT48/Genotype"
reps = 3
cores = 6

tmp <- calibrate_genpars(filex, cultivar, trtno, pars = "phenology",
                         model, reps, cores, calibrate_ecotype = FALSE, dir_glue, dir_out, dir_dssat, dir_genotype,
                         overwrite = FALSE)

#TODO: new cultivar (not in original CUL file; set default params and MIN/MAX = default temporarily)
# sequence phenology: (1) VSEN, PPSEN; (2) P5 [FIXED; DEFAULT IF NOT MEASURED: PHINT and P1]
# sequence growth: (1) GRNO, (2) MXFIL [FIXED; DEFAULT IF NOT MEASURED: STMMX, SLAP1]

defaults_sa <- data.frame(
  type = c("winter wheat", "spring wheat"),
  expno = c(".", "."),
  `ECO#` = c("DFAULT", "DEFAULT"),
  VSEN = c(4,2),
  PPSEN = c(4,2),
  P1 = c(400,400),
  P5 = c(700,700),
  PHINT = c(110,110),
  GRNO = c(25,25),
  MXFIL = c(2,2),
  STMMX = c(1,1),
  SLAP1 = c(300,300)
)

#TODO: select params to be fitted
#TODO: copy key files in GLUE directory if not present
