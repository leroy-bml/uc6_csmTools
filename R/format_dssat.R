#' Reset Management Level IDs to Start from 1
#'
#' Resets the values in a specified ID column of a management data frame
#' so that the minimum nonzero ID becomes 1, and all other positive IDs are shifted accordingly.
#'
#' @param df A data frame containing an ID column to reset.
#' @param id_col Character. The name of the column containing the IDs to reset.
#'
#' @details
#' The function finds the minimum nonzero value in the specified ID column and subtracts \code{min_id - 1} from all positive IDs, so that the smallest positive ID becomes 1. Zero values are left unchanged.
#'
#' This is useful for re-indexing management levels (e.g., fertilizer, irrigation) after splitting or subsetting experimental data.
#'
#' @return The input data frame with the specified ID column reset to start from 1.
#'
#' @examples
#' df <- data.frame(fertilizer_level = c(0, 2, 3, 4))
#' reset_id(df, "fertilizer_level")
#' # fertilizer_level: 0, 1, 2, 3
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


#' Split a multiyear crop experiment dataset by Year and Reset Management IDs
#'
#' Splits each data frame in a list by the "Year" column, removes empty and all-NA columns, and resets management IDs for each year.
#'
#' @param ls A named list of data frames, typically representing sections of an experimental dataset.
#'
#' @details
#' The function:
#' \itemize{
#'   \item Removes metadata sections common to all years.
#'   \item Splits each remaining data frame by the "Year" column.
#'   \item Removes empty data frames and columns with only NA values.
#'   \item Removes summary or time series files with only minimal columns.
#'   \item Resets management IDs in the treatment matrix and management data frames for each year, using \code{reset_id}.
#' }
#' The result is a list of lists, one per year, each containing the relevant data frames for that year with cleaned and reset management IDs.
#'
#' The function uses the helper functions \code{revert_list_str} and \code{reset_id}.
#'
#' @return A named list of lists, each corresponding to a year (e.g., "Y2020"), containing the split and cleaned data frames for that year.
#'
#' @examples
#' \dontrun{
#' yearly_data <- split_by_year(ls)
#' names(yearly_data)
#' }
#'
#' @export

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


#' Format a Data Frame According to a Template for DSSAT Files
#'
#' Formats a data frame to match the structure, column types, and composite columns of a DSSAT template.
#'
#' @param df A data frame to be formatted.
#' @param template A template data frame or list, specifying the desired structure and column types.
#'
#' @details
#' The function:
#' \itemize{
#'   \item Collapses columns for composite sections if the template contains list-columns.
#'   \item Ensures columns in \code{df} match the types of those in \code{template}, including date and POSIXct handling.
#'   \item Populates the template with the input data, preserving column order and structure.
#'   \item Handles NULL lists for composite tables by filling with \code{NA} as needed.
#' }
#' The result is a data frame formatted for DSSAT file writing, with the correct structure and types.
#'
#' The function uses \code{collapse_cols}, \code{bind_rows}, and \code{as_DSSAT_tbl} for data manipulation.
#'
#' @return A data frame formatted according to the template, ready for DSSAT file writing.
#'
#' @examples
#' \dontrun{
#' formatted <- format_table(df, template)
#' }
#' 
#' @importFrom magrittr %>%
#' @importFrom dplyr bind_rows select all_of slice arrange
#' @importFrom purrr map_lgl map
#' @importFrom DSSAT as_DSSAT_tbl
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


#' Format management table list as a DSSAT input file format (file X)
#'
#' Assembles and formats all management sections into a DSSAT File X structure,
#' applying templates, setting defaults, and preserving metadata.
#'
#' @param ls A named list containing at least a \code{"MANAGEMENT"} data frame or list of management sections.
#' @param title Character or NULL. Optional title for the file (not currently used).
#' @param site_code Character or NA. Optional site code (not currently used).
#'
#' @details
#' The function extracts the \code{"MANAGEMENT"} section, applies the appropriate template to each management section, and sets default values for missing parameters in the treatments matrix. It checks for required sections and fills them with template defaults if missing, issuing warnings as needed. The resulting list is ordered according to the template and has relevant attributes transferred from the original data.
#'
#' This is useful for preparing a complete DSSAT File X (management file) for writing to disk.
#'
#' @return A named list of formatted management sections, ready for DSSAT File X writing, with metadata attributes attached.
#'
#' @examples
#' \dontrun{
#' filex <- build_filex(ls)
#' }
#'
#' @importFrom magrittr %>%
#' @importFrom dplyr mutate across where
#' @importFrom DSSAT as_DSSAT_tbl
#' 
#' @export
#' 

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


#' Format observed summary table list as a DSSAT input file format
#'
#' Formats an observed summary data frame from a list into the DSSAT summary
#' file structure, applying date formatting and setting file attributes.
#'
#' @param ls A named list containing at least a \code{"SUMMARY"} data frame.
#' @param title Character or NULL. Optional title for the file (not currently used).
#' @param site_code Character or NA. Optional site code (not currently used).
#'
#' @details
#' The function extracts the \code{"SUMMARY"} data frame from the list, formats all date columns as DSSAT day-of-year strings, arranges by treatment, and applies the \code{as_DSSAT_tbl} function. It sets the \code{v_fmt} attribute for variable formatting, using only those formats present in the data.
#'
#' This is useful for preparing observed summary data for writing to a DSSAT-compatible file.
#'
#' @return A formatted data frame ready for DSSAT summary file writing, with formatting attributes attached.
#'
#' @examples
#' \dontrun{
#' filea <- build_filea(ls)
#' }
#'
#' @importFrom magrittr %>%
#' @importFrom dplyr mutate arrange across where
#' @importFrom DSSAT as_DSSAT_tbl
#' 
#' @export
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


#' Format observed time-series table list as a DSSAT input file format
#'
#' Formats a time series observed data frame from a list into the DSSAT
#' time series file structure, applying date formatting and setting file attributes.
#'
#' @param ls A named list containing at least a \code{"TIME_SERIES"} data frame.
#' @param title Character or NULL. Optional title for the file (not currently used).
#' @param site_code Character or NA. Optional site code (not currently used).
#'
#' @details
#' The function extracts the \code{"TIME_SERIES"} data frame from the list, formats the \code{DATE} column as DSSAT day-of-year strings, arranges by treatment and date, and applies the \code{as_DSSAT_tbl} function. It sets the \code{v_fmt} attribute for variable formatting, using only those formats present in the data.
#'
#' This is useful for preparing observed time series data for writing to a DSSAT-compatible file.
#'
#' @return A formatted data frame ready for DSSAT time series file writing, with formatting attributes attached.
#'
#' @examples
#' \dontrun{
#' filet <- build_filet(ls)
#' }
#'
#' @importFrom magrittr %>%
#' @importFrom dplyr mutate arrange
#' @importFrom DSSAT as_DSSAT_tbl
#' 
#' @export
#' 

build_filet <- function(ls, title = NULL, site_code = NA_character_) {
  
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


#' Format soil profile data as a DSSAT input table
#'
#' Formats a soil data frame from a list into the DSSAT soil file structure,
#' applying a template and preserving metadata.
#'
#' @param ls A named list containing at least a \code{"SOIL"} data frame and its attributes.
#'
#' @details
#' The function extracts the \code{"SOIL"} data frame from the list, formats it using \code{format_table} and a \code{SOIL_template}, and attaches relevant metadata attributes (\code{title}, \code{file_name}, \code{comments}) from the original data frame.
#'
#' This is useful for preparing soil data for writing to a DSSAT-compatible .SOL file.
#'
#' @return A formatted data frame ready for DSSAT soil file writing, with metadata attributes attached.
#'
#' @examples
#' \dontrun{
#' filesol <- build_sol(ls)
#' }
#'
#' @export
#' 

build_sol <- function(ls) {
  
  # Apply template format for DSSAT file writing
  sol <- ls[["SOIL"]]
  filesol <- format_table(sol, SOIL_template)
  
  metadata <- attributes(sol)
  attr(filesol, "title") <- metadata$title
  attr(filesol, "file_name") <- metadata$file_name
  attr(filesol, "comments") <- metadata$comments

  return(filesol)
}


#' Build a DSSAT Weather File from a List
#'
#' Formats a weather data frame from a list into the DSSAT weather file structure, applying a template, formatting dates, and preserving metadata.
#' NB: Currently contains a workaround to correct date formatting issues with
#' the write_wth function that leads to failed simulation
#' 
#' @param ls A named list containing at least a \code{"WEATHER"} data frame and its attributes.
#'
#' @details
#' The function extracts the \code{"WEATHER"} data frame from the list, formats it using \code{format_table} and a \code{WEATHER_template}, and formats the \code{DATE} column as DSSAT day-of-year strings. It sets print formats for weather variables and attaches relevant metadata attributes (\code{station_metadata}, \code{location}, \code{comments}, \code{file_name}) to the output.
#'
#' This is useful for preparing weather data for writing to a DSSAT-compatible .WTH file.
#'
#' @return A formatted data frame ready for DSSAT weather file writing, with metadata attributes attached.
#'
#' @examples
#' \dontrun{
#' filewth <- build_wth(ls)
#' }
#'
#' @importFrom magrittr %>%
#' @importFrom dplyr mutate
#'
#' @export
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


#' Write DSSAT Input Files from a List of Data Frames
#'
#' Writes DSSAT input files (X, A, T, SOL, WTH) from a list of formatted data frames, using appropriate write functions and file names.
#'
#' @param ls A named list of formatted DSSAT data frames, each with a \code{file_name} attribute (e.g., as produced by \code{build_sol}, \code{build_wth}, etc.).
#' @param sol_append Logical. Whether to append to the soil file (\code{write_sol}). Default is \code{TRUE}.
#' @param path Character. Directory path to write files to. Default is the current working directory.
#'
#' @details
#' The function iterates over the expected DSSAT file types (X, A, T, SOL, WTH), and for each present in the list, writes the file using the corresponding write function. The soil file (\code{filesol}) is written with the \code{append} argument as specified. If a required file is missing, a warning is issued.
#'
#' This is useful for exporting a complete set of DSSAT input files from a processed dataset.
#'
#' @return Invisibly returns \code{NULL}. Used for its side effect of writing files.
#'
#' @examples
#' \dontrun{
#' write_dssat(ls, sol_append = FALSE, path = \"./dssat_inputs\")
#' }
#'
#' @importFrom purrr walk
#' 

write_dssat <- function(ls, sol_append = TRUE, path = getwd()) {
  
  # temp workaround for date formatting issue with
  # DSSAT::write_wth (open an issue on GitHub)
  write_wth2 <- function(wth, file_name) {
    write_wth(wth = wth, file_name = file_name, force_std_fmt = FALSE)
  }
  
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


#' Compile and Optionally Write a Complete DSSAT Model Dataset
#'
#' Formats and compiles all required DSSAT input files from a dataset, and optionally writes them to disk.
#'
#' @param dataset A named list of data frames representing the model dataset, including management, observed, soil, and weather data.
#' @param framework Character. The modeling framework (default: \code{"dssat"}).
#' @param write Logical. Whether to write the compiled files to disk. Default is \code{FALSE}.
#' @param sol_append Logical. Whether to append to the soil file (\code{filesol}). Default is \code{TRUE}.
#' @param path Character. Directory path to write files to. Default is the current working directory.
#' @param args List. Named list of simulation control values to overwrite in the management file.
#'
#' @details
#' The function formats the management, observed summary, observed time series, soil, and weather data frames using the appropriate builder functions. It updates simulation control values in the management file if specified in \code{args}. The output is a named list of formatted DSSAT input files. If \code{write = TRUE}, the files are written to disk using \code{write_dssat}.
#'
#' @return A named list containing the formatted DSSAT input files (\code{filex}, \code{filea}, \code{filet}, \code{filesol}, \code{filewth}).
#'
#' @examples
#' \dontrun{
#' dssat_files <- compile_model_dataset(dataset, write = TRUE, path = "./dssat_inputs")
#' }
#'
#' @export
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