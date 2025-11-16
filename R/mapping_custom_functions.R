#' Calculates the time difference between a value and the previous value in a vector.
#' 
#' @param t A vector of datetime objects.
#' @param units The units for the difference (e.g., 'secs', 'mins').
#' 
#' @return A numeric vector of time differences.
#' 
#' @noRd
#' 

calculate_time_lag <- function(t, units = 'secs') {
  return(as.numeric(t - dplyr::lag(t), units = units))
}


#' Coalesces NA values in a vector with the adjacent value (next or previous).
#' 
#' @param x The input vector.
#' @param direction 'forward' or 'backward'.
#' 
#' @return The vector with NA values filled.
#' 
#' @noRd
#' 

coalesce_with_adjacent <- function(x, direction = 'forward') {
  if (direction == 'forward') {
    return(dplyr::coalesce(x, dplyr::lead(x)))
  } else if (direction == 'backward') {
    return(dplyr::coalesce(x, dplyr::lag(x)))
  }
}


# Function registry
# This named list allows the engine to find functions by their string name from the YAML.

.custom_functions <- list(
  calculate_time_lag = calculate_time_lag,
  coalesce_with_adjacent = coalesce_with_adjacent
)
#source("mapping_custom_functions.R")
