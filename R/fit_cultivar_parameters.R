get_xfile_sec <- function(xtables, sec_name){
  
  index <- which(grepl(sec_name, names(xtables)))
  tbl <- if(length(index) > 0) xtables[[index]] else NULL
  
  return(tbl)
}


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
#' 
#' @importFrom dplyr "%>%" mutate
#' @importFrom tibble add_row
#' 
#' @return 
#'

add_treatment <- function(filex, args = list()) {
  
  trt <- filex[[which(startsWith(names(filex), "TREATMENT"))]]
  
  # Keep the last row as default
  new_row <- trt[nrow(trt),]
  # Set new row primary key
  new_row[[1]] <- max(trt[1]) + 1
  
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

# TODO: TEST handle composite section (initial_conditions, irrigation); class list for collapsed vars
#section <- "irrigation"
#args <- list(EFIR = 1, IDATE = c("1981-05-26","1981-06-24"), IRVAL = c(50,50))
#args <- list(FEDATE = c("1981-05-26","1981-06-24"), FMCD = "FE041", FACD = "AP001", FAMN = 120, FAMP = 0, FAMK = 0, FAMC = 0, FDEP = 1)
#add_management(filex = filex, section = "irrigation", args = args)

add_management <- function(filex,
                           section = c("initial_conditions","planting","tillage","irrigation",
                                       "fertilizer","organic_amendment","chemicals","harvest",
                                       "simulation_controls"),
                           args = list()) {
  
  filex = filex_tables  #tmp
  section = "simulation_controls"  #tmp
  args = list(WATER = "Y", IRRIG = "A")  #tmp
  
  
  # Match management type to corresponding file X section
  input_nm <- toupper(substr(section, 1, 5))
  sections_pref <- sapply(names(filex_tables), function(x) substr(x, 1, 5))
  matches <- sapply(sections_pref, function(x) identical(input_nm, x))
  
  if (!input_nm %in% sections_pref) {
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


disable_stress <- function(xfile, stress = c("water", "nitrogen")) {
  
  xfile_out <- xfile  # Initialize output
  
  if ("water" %in% stress) {
    sm <- get_xfile_sec(xfile, "SIMULATION")
    sm <- sm[max(nrow(sm), 1), ]  # Keep last row if multiple levels exist
    sm_list <- lapply(sm, identity)
    sm_list$WATER <- "Y" 
    sm_list$IRRIG <- "A"
    
    xfile_out <- add_management(xfile_out, section = "simulation_controls", args = sm_list)
    
    sm_index <- max(get_xfile_sec(xfile_out, "SIMULATION")[1])
  }
  
  if ("nitrogen" %in% stress) {
    fe_list <- list(
      FDATE = c("1981-10-29", "1982-03-24", "1981-06-10"),
      FMCD = rep("FE041", 3), FACD = rep("AP001", 3), FDEP = 1,
      FAMN = 120, FAMP = 0, FAMK = 0, FAMC = 0, FAMO = 0
    )
    
    xfile_out <- add_management(xfile_out, section = "fertilizer", args = fe_list)
    
    fe_index <- max(get_xfile_sec(xfile_out, "FERTILIZER")[1])
  }
  
  if (identical(sort(stress), c("nitrogen", "water"))) {
    attrs <- list(TNAME = "Auto-irrigation + N saturation", SM = sm_index, MF = fe_index)
  } else if ("water" %in% stress) {
    attrs <- list(TNAME = "Auto-irrigation", SM = sm_index)
  } else if ("nitrogen" %in% stress) {
    attrs <- list(TNAME = "N saturation", MF = fe_index)
  }
  
  out <- add_treatment(xfile_out, args = attrs)
  return(out)
}


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
  
  setup_calibration <- function(...){
    
    # TMP: args
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
    
    ###------------ Default directories -------------------------------------
    ### TODO
    
    ###------------ Retrieve, load, and backup input files ------------------
    
    # Load file X
    filex_tables <- read_filex(filex)
    xfile_nm <- basename(filex)
    
    # Retrieve focal cultivar file based on model input (retrieve from file X if NULL)
    identify_model <- function(filex_tables, model){
      
      # Import DSSAT dict to control input data annotation
      models <- get_dssat_terms("models")
      
      # Validate model input annotation
      models_short <- unique(models[[1]])
      if (!is.null(model) && !(model %in% models_short)) {
        
        if (!is.na(filex_tables$`SIMULATION CONTROLS`$SMODEL) || filex_tables$`SIMULATION CONTROLS`$SMODEL != -99) {
          model <- filex_tables$`SIMULATION CONTROLS`$SMODEL
        } else {
          stop("Error: invalid model. Please specify a model currently implemented in DSSAT: ", paste(models_short, collapse = ", "))
        }
      }
      
      version <- DSSAT:::get_dssat_version()  #TODO: workflow with DSSAT.CSM not set in options
      cfile_nm <- paste0(model, sprintf("%03d", as.numeric(version)))
      
      out <- c(model, cfile_nm)
      
      return(out)
    } #!!FUN
    model <- identify_model(filex_tables, model)[1]
    
    # Load file CUL
    cfile_nm <- identify_model(filex_tables, model)[2]
    cfile_nm <- paste0(cfile_nm, ".CUL")  # Append extension
    cfile_path <- file.path(dir_genotype, cfile_nm)
    cfile_tables <- read_cul(cfile_path)
    
    # Create backup directory
    dir_date <- format(Sys.Date(), "%Y%m%d")
    backup_dir <- file.path(dir_dssat, "0_BackUp", dir_date)
    if(!dir.exists(backup_dir)){
      dir.create(backup_dir, recursive = TRUE)
    }
    
    # Backup X and C files
    cfile_backup <- file.path(backup_dir, cfile_nm)
    xfile_backup <- file.path(backup_dir, xfile_nm)
    write_cul(cfile_tables, file_name = cfile_backup)
    write_filex(filex_tables, xfile_backup)
    
    message(sprintf("Original files backed-up in %s.", backup_dir))
    
    
    ###------------ Retrieve crop code and cultivar identifier --------------
    
    cuTable <- filex_tables$CULTIVARS  # Cultivar table
    
    ingeno <- cuTable[cuTable$CNAME == cultivar]$INGENO  # Cultivar Identifier
    crop_code <- cuTable[cuTable$CNAME == cultivar]$CR  # Crop code
    
    # Retrieve crop full name
    crop_nms <- suppressWarnings(get_dssat_terms("crops"))
    crop <- crop_nms[crop_nms$`@CDE` == crop_code,]$DESCRIPTION  # Crop name
    
    
    ###------------ Set treatment level for calibration ---------------------
    
    
    # TROUBLESHOOT
    ### filex_tables BOTH FE and IR
    ### BACKUP tmp <- filex_tables
    ### CHANGE TO NOTHING #filex_tables <- filex_tables[!grepl("FERTILIZER|IRRIGATION", names(filex_tables))]
    ### CHANGE TO FE ONLY #filex_tables <- filex_tables[grepl("FERTILIZER", names(filex_tables))]
    ### CHANGE TO IR ONLY #filex_tables <- filex_tables[grepl("IRRIGATION", names(filex_tables))]
    
    #trtno <- NULL  #tmp TEST ALL POSSIBILITIES AND TROUBLESHOOT
    
    if (is.null(trtno)) {
      
      feTable <- get_xfile_sec(filex_tables, "FERTILIZER")  # TODO: include OM table too + NITRO/WATER TO Y
      irTable <- get_xfile_sec(filex_tables, "IRRIGATION")
      haTable <- get_xfile_sec(filex_tables, "HARVEST")  # TODO: add to disable_stress new method
      
      # Find the highest nitrogen application
      if (!is.null(feTable)){
        
        feMax <- feTable %>%
          group_by(.[1]) %>%
          summarise(FAMN = sum(FAMN)) %>%
          slice_max(FAMN) %>%
          pull(1)
      } else {
        feMax = 0
      }

      # Find the highest irrigation amount
      if (!is.null(irTable)){
        irMax <- irTable %>%
          unnest(cols = c(IDATE, IROP, IRVAL)) %>%
          group_by(.[1]) %>%
          summarise(IRVAL = sum(IRVAL)) %>%
          slice_max(IRVAL) %>%
          pull(1)
      } else {
        irMax = 0
      }
      
      # Define stress types based on the presence/absence of nitrogen and water management
      stress_types <- c()
      
      if (is.null(feTable)) stress_types <- c(stress_types, "nitrogen")
      if (is.null(irTable)) stress_types <- c(stress_types, "water")
      
      # 1- Drop stress in new treatment if irrigation or fertilization were applied
      if (length(stress_types) > 0) {
        filex_tables <- disable_stress(filex_tables, stress = stress_types)
      }
      
      trtMat <- get_xfile_sec(filex_tables, "TREATMENT")
      trtno <- max(trtMat[1])
      
      # Set treatment number to the highest fertilization level if fertilization was applied
      if (!is.null(feTable)) trtMat[trtno,]$MF <- feMax
      # Set treatment number to the highest irrigation level if irrigation was applied
      if (!is.null(irTable)) trtMat[trtno,]$MI <- irMax
      
      # 2- Both fertilization and irrigation applied: find treatment number with both max values
      if (!is.null(feTable) & !is.null(irTable)) {
        trtno <- trtMat[[which(trtMat$MF == feMax & trtMat$MI == irMax), 1]]
        if (length(trtno) > 1) trtno <- max(trtno)
      }
      
      # Update treatment matrix in xtables accordingly
      filex_tables[grepl("TREATMENT", names(filex_tables))][[1]] <- trtMat
    }
    
    
    ###------------ Write batch files for calibration -----------------------
    
    # Write GLUE batch file
    batchfile <- write_gluebatch(filex_tables, trtno, rp = 1, sq = 0, op = 0, co = 0) #TODO: figure out what these do...
    
    # Set GLUE flag
    flag <- switch( 
      toString(pars),
      "phenology, growth" = 1,
      "phenology" = 2,
      "phenology" = 3,
      stop("Invalid parameter type")
    )
    
    # Set input: calibrate ecotype
    ecocal <- if (calibrate_ecotype) "Y" else "N"
    
    # Set input: number of cores
    cores <- if (is.null(cores)) round(detectCores()/2, 0) else cores
    
    # Write GLUE control files
    write_gluectrl(model = cfile_nm, batchfile, ecocal, dir_glue, dir_out, dir_dssat, flag, reps, cores, dir_genotype) 
    
    
    ###------------ Set bounds for genetic parameters -----------------------
    #TODO
    
    genpars_set_bounds <- function(){
      
      cfile_tables 
      #### BACKUPo
      
      # Customized bounds for calibration
      defaults <- data.frame(
        method = "sa_nwheat",
        type = c("winter wheat", "spring wheat"),
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
      
      
    }
  } 
  
  
  #### TO TEST
  
  run_calibration <- function(method = "glue", ...){
    
    # Ensure the working directory is reset on exit
    oldwd <- getwd()
    on.exit(setwd(oldwd))
    # Set work directory to GLUE directory
    setwd(dir_glue)
    
    # Check if all required files are present in GLUE dir (if not, copied from DSSAT dir)
    check_glue_files()
    # Run GLUE
    system("Rscript GLUE.r")
    
    # Format output
    genpath <- file.path(dir_genotype, cfile_nm)  # original path
    outpath <- file.path(dir_out, cfile_nm)
    cfile_fit <- read_cul(outpath)  # new cultivar file  TODO: check if single rec or full file
    
    # Write results
    write_cul(cfile_fit, genpath)  # overwrite cultivar files in the original location
    message(sprintf("Calibration results written in %s.", genpath))
    
    # Output the fitted parameters for visualization
    out <- dplyr::filter(fit, VRNAME == cultivar)  
    
    return(out)
  }
  
  

}


##### TMP CALL

#tmp <- calibrate_genpars(filex, cultivar, trtno, pars = "phenology",
#                         model, reps, cores, calibrate_ecotype = FALSE, dir_glue, dir_out, dir_dssat, dir_genotype,
#                         overwrite = FALSE)
#
#TODO: new cultivar (not in original CUL file; set default params and MIN/MAX = default temporarily)
# sequence phenology: (1) VSEN, PPSEN; (2) P5 [FIXED; DEFAULT IF NOT MEASURED: PHINT and P1]
# sequence growth: (1) GRNO, (2) MXFIL [FIXED; DEFAULT IF NOT MEASURED: STMMX, SLAP1]
