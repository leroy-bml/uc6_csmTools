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

build_filex <- function(data) {
  
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
