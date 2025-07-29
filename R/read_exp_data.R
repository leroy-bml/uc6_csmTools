#' Read and standardize the name of agricultural experiment tables (BonaRes LTE data model)
#'
#' Reads all files with a specified extension from a directory into a list of data frames. 
#' The function also simplifies the names of the resulting list elements by removing common prefixes and file extensions.
#' (based on BonaRes LTE file name conventions)
#'
#' @param dir Character. Path to the directory containing the data files.
#' @param extension Character. File extension to look for (default is "csv").
#' @param origin Character. Origin of the data (default is "bonares"). Placeholder; currently not used in the function.
#'
#' @return A named list of data frames, each corresponding to a file in the directory.
#'
#' @details
#' The function reads all files with the specified extension in the given directory using \code{read.csv} with UTF-8 encoding.
#' The names of the list elements are simplified by removing the common prefix, file extension, and (optionally) a table number pattern.
#'  
#' @export
#'  
#' @examples
#' \dontrun{
#' data_list <- read_exp_data("path/to/data", extension = "csv")
#' }
#'

read_exp_data <- function(dir, extension = "csv", origin = "bonares") {
  
  # Set extension pattern
  dir <- file.path(dir)
  pattern <- paste0("\\.", extension, "$")
  
  # Find files in location
  db_files <- list.files(path = dir, pattern = pattern)
  # Make file paths
  db_paths <- sapply(db_files, function(x){ file.path(dir, x) })
  # Read files
  # TODO: handle different formats (other functions)
  # TODO: check best encoding, iso-8859-1 vs. latin1 vs. UTF-8
  db_list <- lapply(db_paths, function(x) { file <- read.csv(x, fileEncoding = "UTF-8") })
  
  # Simplify names
  prefix <- find_common_prefix(names(db_list))  # find common prefix
  names(db_list) <- sub(paste0("^", prefix), "", names(db_list))  # drop common prefix
  names(db_list) <- sub("\\..*$", "", names(db_list))  # drop file extension
  names(db_list) <- sub("^\\d+_V\\d+_\\d+_", "", names(db_list))  # (optional) table number
  
  return(db_list)
}

# Check file encoding
#
# library(stringi)
# raw_content <- readBin(db_paths[[30]], "raw", file.info(db_paths[[30]])$size)
# enc_guess <- stri_enc_detect(raw_content)
# print(enc_guess)
