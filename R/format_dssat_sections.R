#'
#' @importFrom purrr compact
#'
#' @export
#'

format_dssat_sections <- function(dataset, comments, build = TRUE) {
  
  # --- Management tables ---
  mngt_list <- split_dssat_components(
    dataset,
    sec = "MANAGEMENT",
    merge = FALSE
  )[[1]]
  mngt_fmt <- .format_dssat_mngt(mngt_list, comments)
  
  # --- Observed data ---
  sm_df <- dataset[["SUMMARY"]]
  sm_fmt <- .format_dssat_sm(sm_df, comments)
  if (!is.null(sm_fmt)) {
    attr(sm_fmt, "experiment") <- attr(mngt_fmt, "experiment")
    attr(sm_fmt, "file_name") <- sub(".$", "A", attr(mngt_fmt, "file_name"))
  }
  ts_df <- dataset[["TIME_SERIES"]]
  ts_fmt <- .format_dssat_ts(ts_df, comments)
  if (!is.null(ts_fmt)) {
    attr(ts_fmt, "experiment") <- attr(mngt_fmt, "experiment")
    attr(ts_fmt, "file_name") <- sub(".$", "T", attr(mngt_fmt, "file_name"))
  }
  
  # --- Soil tables ---
  soil_list <- dataset[names(dataset) %in% c("SOIL_META", "SOIL_GENERAL", "SOIL_LAYERS")]
  soil_fmt <- .format_dssat_soil(soil_list, comments)
  
  # --- Weather tables ---
  # Weather components are first grouped by year
  wth_list_all <- dataset[grepl("WEATHER", names(dataset))]
  suffixes <- sub(".*_(\\d+)$", "\\1", names(wth_list_all))
  wth_list_by_year <- split(wth_list_all, f = suffixes)

  wth_fmt <- lapply(wth_list_by_year, .format_dssat_wth, comments =  comments)
  

  # --- Assemble output ---
  dataset_out <- list(
    MANAGEMENT = mngt_fmt,
    SUMMARY = sm_fmt,
    TIME_SERIES = ts_fmt,
    SOIL = soil_fmt,
    WEATHER = wth_fmt
  )
  dataset_out <- purrr::compact(dataset_out)  # Remove NULLs
  
  # --- Add package signature ---
  signature <- paste0("Dataset processed on ", Sys.Date(), " with csmtools.")
  prepend_comment <- function(x, new_comment) {
    comments <- as.character(unlist(attr(x, "comments")))
    attr(x, "comments") <- c(new_comment, comments)
    return(x)
  }
  dataset_out <- lapply(dataset_out, prepend_comment, new_comment = signature)
  # Special case: nested weather dfs
  dataset_out$WEATHER <- lapply(dataset_out$WEATHER, prepend_comment, new_comment = signature)
  
  
  if (build) {
    # Build as input for DSSAT parser
    dataset_out <- build_dssat_dataset(dataset_out)
  }
  
  return(dataset_out)
}


#
#'
#' @noRd
#'

.format_dssat_mngt <- function(mngt, comments) {
  
  exp_metadata <- mngt[["GENERAL"]]
  mngt_out <- mngt
  
  # Extract comments
  mngt_sections <- c(
    "GENERAL",
    "TREATMENTS",
    "CULTIVARS",
    "FIELDS",
    "INITIAL_CONDITIONS",
    "SOIL_ANALYSIS",
    "PLANTING_DETAILS",
    "IRRIGATION",
    "FERTILIZERS",
    "RESIDUES",
    "CHEMICALS",
    "TILLAGE",
    "ENVIRONMENT_MODIFICATIONS",
    "HARVEST"
  )
  mngt_notes <- comments[names(comments) %in% mngt_sections]

  # Generate markdown format with comments
  # if (length(mngt_notes) > 0) {
  #   ## ADD LOGIC
  # }
  
  # Append file building attributes
  attr(mngt_out, "experiment") <- ifelse(
    "EXP_NAME" %in% exp_metadata,
    unique(exp_metadata$EXP_NAME),
    unique(exp_metadata$EXP_ID)
  )
  #attr(mngt_out, "comments") <- mngt_nodes_md
  attr(mngt_out, "file_name") <- unique(exp_metadata$file_name)
  
  return(mngt_out)
}


#'
#'
#'

.format_dssat_sm <- function(sm, comments) {
  
  # Extract comments
  sm_notes <- comments[names(comments) %in% "SUMMARY"]
  
  # Return NULL if only treatment column is present
  # (possible split artefact in multi-year data)
  if (is.null(sm) || (identical(names(sm), "TRNO") && ncol(sm) == 1)) {
    sm_out <- NULL
  } else {
    sm_out <- sm
    # Append attributes for file building
    attr(sm_out, "comments") <- sm_notes
  }
  
  return(sm_out)
}


#'
#'
#'

.format_dssat_ts <- function(ts, comments) {
  
  # Extract comments
  ts_notes <- comments[names(comments) %in% "TIME_SERIES"]
  
  # Return NULL if only treatment and date column is present
  # (possible split artefact in multi-year data)
  if (is.null(ts) || (all(names(ts) %in% c("TRNO", "DATE")) && ncol(ts) <= 2)) {
    ts_out <- NULL
  } else {
    ts_out <- ts
    # Append attributes for file building
    attr(ts_out, "comments") <- ts_notes
  }
  
  return(ts_out)
}


#'
#'
#'

.format_dssat_soil <- function(soil, comments) {
  
  soil_metadata <- soil[["SOIL_META"]]
  
  # Extract comments
  soil_notes <- comments[names(comments) %in% c("SOIL_META", "SOIL_GENERAL", "SOIL_LAYERS")]
  if (length(soil_notes) > 0) {
    soil_notes <- unlist(lapply(soil_notes, function(df) df$Content), use.names = FALSE)
  }
  
  # Merge data in output
  soil_out <- suppressMessages(
    purrr::reduce(soil, dplyr::left_join)
  )
  
  # Append attributes for file building
  attr(soil_out, "comments") <- soil_notes
  attr(soil_out, "title") <- ifelse(
    any(grepl("ISRIC", soil_notes)),
    "ISRIC Soil Grids-derived synthetic profile",
    paste(na.omit(soil_metadata$PEDON), na.omit(soil_metadata$SITE), collapse = "; ")
  )
  attr(soil_out, "file_name") <- unique(soil_metadata$file_name)
  
  return(soil_out)
}


#
#'
#'
#'

.format_dssat_wth <- function(wth, comments) {
  
  wth_data <- wth[grepl("DAILY", names(wth))][[1]]
  wth_metadata <- wth[grepl("METADATA", names(wth))][[1]]
  
  # Extract comments
  wth_notes <- comments[grepl("WEATHER", names(comments))]
  if (length(wth_notes) > 0) {
    wth_notes <- unlist(lapply(wth_notes, function(df) df$Content), use.names = FALSE)
  }
  
  # Merge data in output
  # wth_out <- suppressMessages(reduce(wth, left_join))
  
  # Append file building attributes
  attr(wth_data, "GENERAL") <- wth_metadata
  attr(wth_data, "location") <- unique(toupper(wth_metadata$WST_NAME))
  attr(wth_data, "comments") <- wth_notes
  attr(wth_data, "file_name") <- unique(wth_metadata$file_name)
  
  return(wth_data)
}
