#' Format a DSSAT dataset list (internal)
#'
#' Internal orchestrator for formatting a DSSAT dataset. It iterates through the top-level list
#' (e.g., "MANAGEMENT", "SOIL") and delegates formatting to the appropriate `build_*` helper function.
#'
#' @param dataset A named list of data.frames/lists from a DSSAT file reader or mapper. Names should 
#'    correspond to the different types of DSSAT input files: 'MANAGEMENT', 'SUMMARY', 'TIME_SERIES',
#'    'SOIL', and 'WEATHER'
#'
#' @return A named list with all elements formatted.
#'
#' @noRd
#' 

build_dssat_dataset <-  function(dataset) {
  
  data_fmt <- dataset %>%
    purrr::imap(~{
      switch(
        .y,
        "MANAGEMENT" = .build_filex(.x),
        "SUMMARY"    = .build_dssat_file(.x, SUMMARY_template, v_fmt_filea),
        "TIME_SERIES"= .build_dssat_file(.x, TIME_SERIES_template, v_fmt_filet),
        "SOIL"       = .build_dssat_file(.x, SOIL_template, DSSAT:::sol_v_fmt()),
        "WEATHER"    = .build_wth(.x),
        
        # Default case if .y matches no other case.
        stop(paste0("Unrecognized DSSAT section name: '", .y, "'. ",
                    "Expected one of 'MANAGEMENT', 'SUMMARY', 'TIME_SERIES', 'SOIL', or 'WEATHER'."))
      )
    })
  
  return(data_fmt)
}


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

.build_dssat_file <- function(data, template, v_fmt) {
  
  # Apply template format
  data <- format_dssat_table(data, template)
  # Drop non-DSSAT attributes
  data <- dplyr::select(data, intersect(colnames(data), names(v_fmt)))
  # Set print formats
  attr(data, "v_fmt") <- v_fmt[names(v_fmt) %in% names(data)]
  
  return(data)
}


#' Format the MANAGEMENT inputs (FileX) for DSSAT models
#'
#' The function formats the DSSAT FileX, which contains core parameters for DSSAT simulation (i.e., metadata,
#' resources, management regimes and simulation). It iterates through each subsection (e.g., 'TREATMENTS', 'FIELDS'),
#' applies the corresponding template from 'FILEX_template', selects valid DSSAT columns, and attaches print formats.
#'
#' NB: preserves custom input attribute usable in downstreams file writing with the DSSAT package
#' (e.g., file_name, comments)
#'
#' @param data A named list where each element is a data.frame representing a DSSAT FileX subsection.
#'
#' @return A named-list; write-ready DSSAT FileX.
#'
#' @noRd
#' 

.build_filex <- function(data) {
  
  # Store custom attributes
  attrs <- attributes(data)
  custom_attrs <- attrs[!names(attrs) %in% c("names", "row.names", "class")]
  
  # Get section names to run formatting through each section
  sec_nms <- sort(names(data))
  
  # Apply template
  data <- mapply(
    format_dssat_table,
    data[sec_nms],
    FILEX_template[sec_nms],
    SIMPLIFY = FALSE
  )
  # Drop non-DSSAT attributes
  data <- mapply(
    function(x, y) dplyr::select(x, intersect(colnames(x), names(y))),
    data[sec_nms],
    FILEX_template[sec_nms]
  )
  # Attach print formats
  data <- mapply(function(x, y) {
    attr(x, "v_fmt") <- DSSAT:::filex_v_fmt(y)
    x
  }, data[sec_nms], sec_nms, SIMPLIFY = FALSE)
  
  # Order subsections
  data <- data[match(names(FILEX_template), names(data))]
  data <- data[lengths(data) > 0]
  # data <- lapply(data, function(df) df[order(df[[1]]), ])
  
  # Check file X writing requirements
  # TODO: replace by DQ inputs
  required_sections <- c("TREATMENTS", "CULTIVARS", "FIELDS", "PLANTING_DETAILS")
  
  for (i in names(FILEX_template)) {
    if (length(data[[i]]) == 0) {
      if(i %in% required_sections) {
        data[[i]] <- FILEX_template[[i]]
        warning(paste0("Required section missing from input data: ", i))
      } else if (i == "GENERAL") {
        data[[i]] <- FILEX_template[[i]]
        warning("Experiment metadata is missing.")
      } else if (i == "SIMULATION_CONTROLS") {
        data[[i]] <- DSSAT::as_DSSAT_tbl(FILEX_template[[i]])
        #warning("SIMULATION_CONTROLS section not provided with input data. Controls set to default values.")
      } else {
        data[[i]] <- NULL
      }
    }
  }
  
  # Reattach custom attributes
  attributes(data) <- c(attributes(data), custom_attrs)
  
  return(data)
}


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

.build_wth <- function(data) {
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
