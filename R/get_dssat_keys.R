#' Download and Load the ICASA Data Dictionary
#'
#' Downloads the official ICASA Data Dictionary Excel file from the DSSAT GitHub repository and loads its sheets as data frames.
#'
#' @details
#' The function downloads the ICASA Data Dictionary from the official DSSAT GitHub repository and loads each worksheet (except the first) into a list of data frames. The list is named according to the sheet names in the workbook.
#'
#' The function uses \code{wb_load} and \code{wb_to_df} for reading the Excel file and converting sheets to data frames.
#'
#' @return A named list of data frames, each corresponding to a sheet in the ICASA Data Dictionary (excluding the first sheet).
#'
#' @examples
#' \dontrun{
#' icasa_dict <- get_icasa()
#' names(icasa_dict)
#' }
#'
#' @importFrom openxlsx2 wb_load wb_to_df
#'
#' @export
#' 

# options(DSSAT.CSM = "C:\\DSSAT48\\DSCSM048.EXE")
# TODO: check if url still valid

get_icasa <- function(){
  url <- "https://github.com/DSSAT/ICASA-Dictionary/raw/refs/heads/main/ICASA%20Data%20Dictionary.xlsx"  # official ICASA repo
  wb <- wb_load(file = url, data_only = TRUE)
  
  ls <- lapply(2:length(wb$worksheets), function(x) { 
    wb_to_df(wb, sheet = x)
  })
  names(ls) <- wb$sheet_names[2:length(wb$worksheets)]
  
  return(ls)
}


#' Lookup Table for DSSAT Reference Keys and File Locations
#'
#' Provides a mapping of DSSAT reference items (e.g., crops, models, soil analyses) to their corresponding file names and section headers.
#'
#' @details
#' This function returns a data frame mapping reference items (such as "crops", "models", and "soil_analyses") to the DSSAT file in which they are found and the header string that marks the start of the relevant section.
#'
#' It is used internally by functions such as \code{get_dssat_terms} to locate and extract reference tables from DSSAT installation files.
#'
#' @return A data frame with columns \code{item}, \code{file}, and \code{header}.
#'
#' @examples
#' lookup_keys()
#' # Returns a data frame mapping items to files and headers
#'
#' @export
#' 

lookup_keys <- function(){
  
  data.frame(
    item = c("crops","models","soil_analyses"),
    file = c("DETAIL.CDE","SIMULATION.CDE","SOIL.CDE"),
    header = c("Crop and Weed Species","Crop Models","")
  )
}


#' Retrieve DSSAT Terms for Crops, Models, or Soil Analyses
#'
#' Extracts reference tables (e.g., crops, models, soil analyses) from DSSAT installation files.
#'
#' @param key Character. The type of terms to retrieve. Options are \code{"crops"}, \code{"models"}, or \code{"soil_analyses"}.
#'
#' @details
#' The function locates the DSSAT installation directory using the \code{DSSAT.CSM} option, then looks up the appropriate file and header for the requested key using \code{lookup_keys()}. It reads the relevant section from the file, parses the lines into columns, and returns a data frame of the terms.
#'
#' This is useful for retrieving reference tables (e.g., crop codes, model codes) used in DSSAT input files.
#'
#' @return A data frame containing the requested DSSAT terms.
#'
#' @examples
#' \dontrun{
#' crops <- get_dssat_terms("crops")
#' models <- get_dssat_terms("models")
#' }
#'
#' @export
#' 

get_dssat_terms <- function(key = c("crops","models","soil_analyses")){
  
  dssat_csm <- options()$DSSAT.CSM  # locate DSSAT.CSM executable
  dssat_dir <- dirname(dssat_csm)  # locate DSSAT directory
  
  keys <- lookup_keys()
  key_location <- keys[which(keys$item == key),]
  
  lines <- readLines(file.path(dssat_dir, key_location$file))
  
  sec_start <- grep(key_location$header, lines, fixed = TRUE)
  sec_end <- which(lines[(sec_start + 1):length(lines)] == "")[1] + sec_start  # find the next empty line
  sec_end <- ifelse(is.na(sec_end), length(lines) + 1, sec_end)  # set to end if no empty line is foun
  sec_lines <- lines[(sec_start + 1):(sec_end - 1)]
  
  sec_comps <- strsplit(sec_lines, " {2,}")
  
  sec_cols <- do.call(rbind, lapply(sec_comps, function(x) {
    # ensure each row has exactly 3 components by collapsing extra spaces
    if (length(x) > 3) {
      x <- c(x[1], x[2], paste(x[3:length(x)], collapse = " "))
    }
    x
  }))
  sec_df <- as.data.frame(sec_cols[-1,])
  colnames(sec_df) <- sec_cols[1,]
  
  return(sec_df)
}
