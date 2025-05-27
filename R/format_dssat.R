#' Reset index variables to start at 1
#' 
#' Includes a special case for ignoring 0s (ID in treatment matrix with no linked record in a management section)
#' 
#' @export
#' 
#' @param df a data frame with rows identified by an integer index variable
#' @param id_col a length 1 character containing the name of the index column in df
#' 
#' @return a data frame; the input data with a reset index variable
#'

reset_id <- function(df, id_col) {
  
  id <- df[[id_col]]
  suppressWarnings(
    min_id <- min(id[id != 0])
  )
  
  id[id > 0] <- id[id > 0] - min_id + 1
  df[[id_col]] <- id
  
  return(df)
}

#' Split a multiyear crop experiment dataset by year
#' 
#' Handles all artefacts created by spitting the original data (e.g., empty data frames, columns with only NAs)
#' 
#' @export
#' 
#' @param ls a list of data frames containing ICASA-structured crop experiment data
#' 
#' @return a list containing n lists of crop experiment data tables, where n is the number of years in the experiment
#'

split_by_year <- function(ls) {

  # Drop metadata common to all years
  ls_ipt <- ls[!names(ls) %in% c("GENERAL", "FIELDS", "SOIL_Header", "SOIL_Layers")]  #!!
  
  # Split data by year
  ls_yr <- lapply(ls_ipt, function(df) split(df, f = df[["Year"]]))
  ls_yr <- revert_list_str(ls_yr)
  names(ls_yr) <- paste0("Y", names(ls_yr))
  
  # Remove empty data frames, e.g., management categories irrelevant for the focal year
  ls_yr <- lapply(ls_yr, function(ls) ls[lengths(ls) > 0])
  
  # Remove columns with only NAs in each data frame
  ls_yr <- lapply(ls_yr, function(ls){
    ls <- lapply(ls, function(df) {
      df[, colSums(is.na(df)) != nrow(df)]
    })
  })
  
  # Special case: file A with only TRTNO & Year, TimeSeries with only TRTNO, Year, DATE
  ls_yr <- lapply(ls_yr, function(ls){
    if (all(colnames(ls[["OBSERVED_Summary"]]) %in% c("TRTNO","Year","DATE"))) {
      ls[["OBSERVED_Summary"]] <- NULL
    }
    if (all(colnames(ls[["OBSERVED_TimeSeries"]]) %in% c("TRTNO","Year","DATE"))) {
      ls[["OBSERVED_TimeSeries"]] <- NULL
    }
    return(ls)
  })
  
  # Reset management IDs
  filex_sec_ids <- c("N", "C", "L", "A", "C", "P", "T", "I", "F", "R", "C", "E", "H")
  filex_trt_ids <- c("CU", "FL", "SA", "IC", "MP", "MI", "MF", "MR", "MC", "MT", "ME", "MH", "SM")
  
  ls_yr <- lapply(ls_yr, function(ls) {
    
    ls_mngt <- ls[names(ls) %in% filex_sections]
    ls_rest <- ls[!names(ls) %in% filex_sections]
    
    # Reset management IDs in treatment matrix #!! CHECK!!! FERTILIZER
    for (i in colnames(ls_mngt[["TREATMENTS"]])) {
      if (i %in% filex_trt_ids) {
        ls_mngt[["TREATMENTS"]] <- reset_id(ls_mngt[["TREATMENTS"]], i)
      }
    }
    
    # Reset management IDs in management data franes
    ls_mngt <- lapply(ls_mngt, function(df) {
      for (i in colnames(df)) {
        if (i %in% filex_sec_ids) {
          df <- reset_id(df, i)
        }
      }
      return(df)
    })
    
    return(c(ls_rest, ls_mngt))
  })
  
  return(ls_yr)
}

#' Prepare data tables for export as various DSSAT input file types
#' 
#' @export
#' 
#' @param df a data frame of the data to be formatted
#' @param template a model data frame for the focal file type, as generated in 'dssat_file_templates.R'
#' 
#' @return a data frame; a formatted DSSAT input table
#' 
#' lubridate as.POSIXct\ # is part of base now
#' @importFrom dplyr "%>%" bind_rows select slice arrange
#' @importFrom tidyr all_of
#' @importFrom purrr map map_lgl
#' @importFrom DSSAT as_DSSAT_tbl
#' 
#' 

format_table <- function(df, template) {
  
  has_lists <- map_lgl(template, is.list)
  is_list <- has_lists[has_lists]
  
  # Collapse columns of composite sections
  if (length(is_list) > 0) {
    df <- collapse_cols(df, intersect(names(is_list), names(df)))
  }
  
  col_names <- intersect(names(df), names(template))
  
  # Apply template format to the input table
  for (col_name in col_names) {
    
    if(class(template[[col_name]][[1]])[1] == "POSIXct") {
      if (is.list(template[[col_name]])) {
        df[[col_name]][[1]] <- as.POSIXct(as.Date(df[[col_name]][[1]], format = "%Y-%m-%d"))
      } else {
        # TODO: ensure that input dates are always in the format YYYY-MM-DD
        df[[col_name]] <- as.POSIXct(as.Date(df[[col_name]], format = "%Y-%m-%d"))
      }
    }
    
    df[[col_name]] <- as(df[[col_name]], class(template[[col_name]])[1])
  }
  
  # Populate the template with the input data
  out <- template %>%
    bind_rows(df) %>%
    select(all_of(colnames(template))) %>%
    slice(-1) %>%
    arrange(row.names(template))
  
  # Format NULL lists for composite tables (e.g., IRRIGATION)
  has_lists <- map_lgl(out, is.list)
  out[has_lists] <- map(out[has_lists], ~ ifelse(is.null(.x[[1]]), rep(NA, length(out[[1]])), .x))
  
  
  return(as_DSSAT_tbl(out))
}


#' Format crop management data as DSSAT input tables
#' 
#' @export
#' 
#' @param ls a list of data frames containing crop management data, to be formatted into DSSAT input tables
#' @param title a length 1 character vector specifying the title of the dataset
#' @param site_code a length 1 character vector provide the DSSAT standard code for the experimental site
#' 
#' @return a data frame with codes mapped to the format specified in the supplied map
#' 
#' @importFrom dplyr "%>%" mutate across where
#'

# Assemble File X
build_filex <- function(ls, title = NULL, site_code = NA_character_) {

  filex <- ls[["MANAGEMENT"]]
  metadata <- attributes(filex)  #!! check for integration
  
  # Apply the template format to each section
  sec_nms <- sort(names(filex))
  filex <- mapply(format_table, filex[sec_nms], FILEX_template[sec_nms], SIMPLIFY = FALSE)
  
  # Set default values for missing parameters in treatments matrix
  # TODO: transfer as variable parametrization!
  filex[["TREATMENTS"]] <- filex[["TREATMENTS"]] %>%
    mutate(R = ifelse(is.na(R), 1, R),  # default to 1
           O = ifelse(is.na(O), 0, O),  # default to 0
           C = ifelse(is.na(C), 0, C),  # default to 0
           SM = ifelse(is.na(SM), 1, SM),  # default to 1  ##REDUNDANT
    ) %>%  
    mutate(across(where(is.numeric), ~ifelse(is.na(.x), 0, .x)))  # exception is simulation controls

  # Check requirements for writing file X
  required_sections <- c("TREATMENTS","CULTIVARS","FIELDS","PLANTING_DETAILS")
  
  for (i in names(FILEX_template)) {
    if (length(filex[[i]]) == 0) {
      if(i %in% required_sections) {
        filex[[i]] <- FILEX_template[[i]]
        warning(paste0("Required section missing from input data: ", i))
      } else if (i == "GENERAL") {
        filex[[i]] <- FILEX_template[[i]]
        warning("No Metadata provided with input data. ###")
      } else if (i == "SIMULATION_CONTROLS") {
        filex[[i]] <- as_DSSAT_tbl(FILEX_template[[i]])
        warning("SIMULATION_CONTROLS section not provided with input data. Controls set to default values.")
      } else {
        filex[[i]] <- NULL
      }
    }
  }
  
  # Set section in the right order
  filex <- filex[match(names(FILEX_template), names(filex))]
  filex <- filex[lengths(filex) > 0]
  
  # Transfer attributes
  attr(filex, "experiment") <- metadata$experiment
  attr(filex, "file_name") <- metadata$file_name
  attr(filex, "comments") <- metadata$comments

  return(filex)
}


#' Format observed summary data as a DSSAT input table
#' 
#' @export
#' 
#' @param ls a list of data frames containing crop experiment data ### (File A)
#' @param title a length 1 character vector specifying the title of the dataset
#' @param site_code a length 1 character vector provide the DSSAT standard code for the experimental site
#' 
#' @return a data frame with codes mapped to the format specified in the supplied map
#' 
#' @importFrom dplyr "%>%" mutate select across where arrange
#' @importFrom DSSAT as_DSSAT_tbl
#'

build_filea <- function(ls, title = NULL, site_code = NA_character_) {

  if (!"SUMMARY" %in% names(ls)) return(NULL)
    
  filea <- ls[["SUMMARY"]] %>%
    # Format date as DSSAT format
    mutate(across(where(is.Date), ~ format(as.Date(.x), "%y%j"))) %>%
    arrange(TRNO) %>%
    as_DSSAT_tbl() %>%
    # Set formatting attributes for writing files (DSSAT libraries)
    {attr(., "v_fmt") <- v_fmt_filea[names(v_fmt_filea) %in% names(.)]; .}
  
  return(filea)
}


#' 
#' @export
#' 
#' @param ls a list of data frames containing crop experiment data ### (File T)
#' @param title a length 1 character vector specifying the title of the dataset
#' @param site_code a length 1 character vector provide the DSSAT standard code for the experimental site
#' 
#' @return a data frame with codes mapped to the format specified in the supplied map
#' 
#' @importFrom dplyr "%>%" mutate select across where arrange
#' @importFrom DSSAT as_DSSAT_tbl
#'


build_filet <- function(ls, title = NULL, site_code = NA_character_) {
  
  #ls <- data_dssat  #tmp
  
  if (!"TIME_SERIES" %in% names(ls)) return(NULL)
  
  filet <- ls[["TIME_SERIES"]] %>%
    # Format date as DSSAT format
    mutate(DATE = format(as.Date(DATE), "%y%j")) %>%
    arrange(TRNO, DATE) %>%
    #select(-N) %>%  # temp workaround to delete duplicate TRTNO in map (N in TREATMENTS). Fix by handling mapping by section.
    as_DSSAT_tbl() %>%
    # Set formatting attributes for writing files (DSSAT libraries)
    {attr(., "v_fmt") <- v_fmt_filet[names(v_fmt_filet) %in% names(.)]; .}
  
  return(filet)
}


#' Format observed time series data as a DSSAT input table
#' 
#' @export
#' 
#' @param ls a list of data frames containing soil profile data (layers + surface as separate data frames)
#' 
#' @return a data frame containing soil profile data formatted as a DSSAT input
#' 
#' @importFrom dplyr "%>%" slice bind_rows bind_cols
#'

build_sol <- function(ls) {
  
  #ls <- data_dssat
  
  # Apply template format for DSSAT file writing
  sol <- ls[["SOIL"]]
  filesol <- format_table(sol, SOIL_template)
  
  metadata <- attributes(sol)
  attr(filesol, "title") <- metadata$title
  attr(filesol, "file_name") <- metadata$file_name
  attr(filesol, "comments") <- metadata$comments

  return(filesol)
}


#' 
#' Currently contains a workaround to correct date formatting issues with the write_wth function that leads to
#' failed simulation
#' 
#' @export
#' 
#' @param ls a list of data frames containing weather data (daily + header as separate data frames) and station
#' metadata as comments
#' 
#' @return a data frame containing soil profile data formatted as a DSSAT input
#' 
#' @importFrom dplyr "%>%" mutate
#' @importFrom DSSAT as_DSSAT_tbl
#'

build_wth <- function(ls) {
  
  # Apply template format for DSSAT file writing
  wth <- ls[["WEATHER"]]
  filewth <- format_table(wth, WEATHER_template) %>%
    mutate(DATE = format(as.Date(DATE), "%y%j"))
  
  # Corrected print formats (temp workaround for date formatting issue with write_wth)
  wth_fmt <- c(DATE = "%5s",  # instead of %7s
               SRAD = "%6.1f", TMAX = "%6.1f",TMIN = "%6.1f",
               RAIN = "%6.1f", DEWP = "%6.1f", WIND = "%6.0f",
               PAR = "%6.1f", EVAP = "%6.1f", RHUM = "%6.1f")
  #wth <- as.data.frame(mapply(function(x, y) sprintf(y, x), wth, wth_fmt))
  
  # Set metadata as attributes
  metadata <- attributes(wth)
  metadata$station_metadata <- format_table(metadata$station_metadata, WEATHER_header_template)
  
  attr(filewth, "v_fmt") <- wth_fmt  # corrected print formats
  attr(filewth, "GENERAL") <- metadata$station_metadata
  attr(filewth, "location") <- metadata$location
  attr(filewth, "comments") <- metadata$comments
  attr(filewth, "file_name") <- metadata$file_name
  
  return(filewth)
}


#' Write multiple types of standard DSSAT files before
#' 
#' Only handles input tables that have been previously mapped to the DSSAT standard format
#' 
#' @export
#' 
#' @param ls a list DSSAT-formated crop experiment data 
#' @param path a character vector storing the path to the directory where the files will be written
#' 
#' @return NULL
#' 
#' @importFrom DSSAT write_filex write_filea write_filet write_sol write_wth
#'

# temp workaround for date formatting issue with DSSAT::write_wth (open an issue on GitHub)
write_wth2 <- function(wth, file_name) {
  write_wth(wth = wth, file_name = file_name, force_std_fmt = FALSE)
}

#' TODO: Needs documentation!
#' 
#' @export 
#' 
#' @importFrom purrr walk
#' 

write_dssat <- function(ls, sol_append = TRUE, path = getwd()) {
  
  # Write functions (from DSSAT package)
  write_funs <- list(
    filex = write_filex,
    filea = write_filea,
    filet = write_filet,
    filesol = write_sol,
    filewth = write_wth2
  )
  
  walk(names(write_funs), ~{
    if (!is.null(ls[[.]])) {
      print(names(ls[[.]]))
      
      # Add append = FALSE only for write_sol
      if (. == "filesol") {
        write_funs[[.]](ls[[.]], file_name = paste0(path, "/", attr(ls[[.]], "file_name")), append = sol_append)
      } else {
        write_funs[[.]](ls[[.]], file_name = paste0(path, "/", attr(ls[[.]], "file_name")))
      }
      
    } else if (. %in% c("filex", "filewth", "filesol")) {
      warning(paste0("Required ", ., " file is missing."))
    }
  })
}

#' ###
#' 
#' 
#' @param df ###
#' 
#' @return a data frame ###
#' 
#' @importFrom ###
#'

compile_model_dataset <-
  function(dataset, framework = "dssat",
           write = FALSE, sol_append = TRUE, path = getwd(), args = list()) {
    
    
    # Format files
    filex <- build_filex(dataset) # management
    if(!is.null(dataset[["SUMMARY"]])) filea <- build_filea(dataset) else filea <- NULL  # observed summary
    if(!is.null(dataset[["TIME_SERIES"]])) filet <- build_filet(dataset) else filet <- NULL  # observed time series
    filesol <- build_sol(dataset)  # soil profile
    filewth <- build_wth(dataset)  # weather
    
    # Set simulation controls (overwrite default with provided values)
    for (name in names(args)) {
      if (name %in% names(filex$SIMULATION_CONTROLS)) {
        filex$SIMULATION_CONTROLS[[name]] <- args[[name]]
      }
    }
    
    # TODO: Fetch cultivar file
    #if(!is.null(file_cul)) file_cul <- build_cul(file_cul)  # TODO: fetch cultivar file
    
    # Output dataset
    dataset <- list(filex = filex, filea = filea, filet = filet, filesol = filesol, filewth = filewth)
    #return(dataset)
    
    # Write DSSAT files
    if (write == TRUE){
      write_dssat(dataset, sol_append = sol_append, path)
      message(paste("DSSAT data files written in", path))
    }
    return(dataset)
  }