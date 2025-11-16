#'
#' @noRd
#'

standardize_data <- function(dataset, data_model = c("icasa", "dssat")) {
  
  #MATCH ARGS
  
  switch(
    data_model,
    "icasa" = {
      out <- .standardize_icasa_data(dataset)
    },
    "dssat" = {
      out <- .standardize_dssat_data(dataset)
    }
  )
  
  return(out)
}

#'
#' @noRd
#'

# TODO TEST: whole routine should be robust to missing SOIL/WEATHER DATA
.standardize_dssat_data <- function(dataset, build = TRUE) {

  # Apply DSSAT standard codes
  dataset_nms <- apply_naming_rules(dataset)
  # TODO: test with single exp + add file names
  
  # Split by experiment (as per DSSAT definition)
  dataset_split <- split_dataset(dataset_nms, key = "experiment", data_model = "dssat")
  # Split weather data to match the cultivation season for each experiment
  dataset_split <- lapply(dataset_split, extract_season_weather)
  
  # Extract comments
  comments_split <- lapply(dataset_split, function(ls) {
    comments <- purrr::map(dataset, extract_dssat_notes)
    purrr::keep(comments, ~ nrow(.x) > 0)
  } )
  
  # Apply required object structure for file export with DSSAT library functions
  dataset_split_fmt <- purrr::map2(
    dataset_split,
    comments_split,
    format_dssat_sections,
    build = build
  )

  return(dataset_split_fmt)
}


#-----

# TODO
.standardize_icasa_data <- function() {
  print("TODO")
}
