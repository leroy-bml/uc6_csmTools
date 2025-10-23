#' Format a simple DSSAT file section
#'
#' The generic function format single data.frame DSSAT sections, as parsed with the DSSAT package
#' (i.e., SUMMARY, TIME_SERIES, SOIL).
#' It applies the relevant data template, filter-out invalid attributes and attaches print formats
#'
#' @param data A single data.frame; DSSAT section to format.
#' @param template The template data.frame for the focal section.
#' @param v_fmt The 'v_fmt' list (named vector) of print formats for the focal section.
#'
#' @return A data.frame; write-ready DSSAT file.
#'
#' @noRd
#' 

build_dssat_file <- function(data, template, v_fmt) {
  
  # Apply template format
  data <- format_dssat_table(data, template)
  # Drop non-DSSAT attributes
  data <- dplyr::select(data, intersect(colnames(data), names(v_fmt)))
  # Set print formats
  attr(data, "v_fmt") <- v_fmt[names(v_fmt) %in% names(data)]
  
  return(data)
}
