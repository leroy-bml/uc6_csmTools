#' Format the WEATHER input for DSSAT models
#'
#' Formats the WEATHER section for writing DSSAT files with the DSSAT package.
#' It applies the core logic whether the input `data` is a single data frame (one year)
#' or multiple data frames in a list (multiple years) by using `apply_recursive` to call the core formatting logic.
#'
#' @param data A data.frame or list of data.frames containing DSSAT-formatted weather data
#'
#' @return The formatted weather data, preserving the original structure (list or data.frame).
#'
#' @noRd
#' 

build_wth <- function(data) {
  apply_recursive(data, .build_single_wth)
}

#' Core logic for formatting DSSAT WEATHER data files
#'
#' Helper; applies DSSAT WTH file templates to both daily weather and station metadata
#' (stored in the 'GENERAL' attribute as per the DSSAT package standard).
#' It filters out invalid DSSAT columns and attaches the 'v_fmt' attribute for printing.
#'
#' @param wth_data A data frame for a single DSSAT-formatted weather data, with metadata as 'GENERAL' attribute.
#'
#' @return The write-ready data.frame for the data, with formatted metadata and print formats as attributes
#'
#' @noRd
#'

.build_single_wth <- function(wth_data) {
  
  # --- Weather daily ---
  # Apply template format
  wth_data <- format_dssat_table(wth_data, WEATHER_template)
  # Drop non-DSSAT attributes
  v_fmt_wth_daily <- DSSAT:::wth_v_fmt("DAILY")
  wth_data <- dplyr::select(wth_data, intersect(colnames(wth_data), names(v_fmt_wth_daily)))
  # Set print formats
  attr(wth_data, "v_fmt") <- v_fmt_wth_daily
  
  # --- Station metadata ---
  # Apply template format
  wth_metadata <- format_dssat_table(attr(wth_data, "GENERAL"), WEATHER_header_template)
  # Drop non-DSSAT attributes
  v_fmt_metadata <- DSSAT:::wth_v_fmt("GENERAL", old_format = TRUE)
  wth_metadata <- dplyr::select(wth_metadata, intersect(colnames(wth_metadata), names(v_fmt_metadata)))
  # Set print formats
  attr(wth_metadata, "v_fmt") <- v_fmt_metadata
  # Restore metadata as attribute
  attr(wth_data, "GENERAL") <- wth_metadata
  
  return(wth_data)
}
