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
        "MANAGEMENT" = build_filex(.x),
        "SUMMARY"    = build_dssat_file(.x, SUMMARY_template, v_fmt_filea),
        "TIME_SERIES"= build_dssat_file(.x, TIME_SERIES_template, v_fmt_filet),
        "SOIL"       = build_dssat_file(.x, SOIL_template, DSSAT:::sol_v_fmt()),
        "WEATHER"    = build_wth(.x),
        
        # Default case if .y matches no other case.
        stop(paste0("Unrecognized DSSAT section name: '", .y, "'. ",
                    "Expected one of 'MANAGEMENT', 'SUMMARY', 'TIME_SERIES', 'SOIL', or 'WEATHER'."))
      )
    })
  
  return(data_fmt)
}
