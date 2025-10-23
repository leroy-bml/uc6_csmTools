#' Safely apply an aggregation function with `na.rm = TRUE`
#'
#' Calls `FUN(x, na.rm = TRUE, ...)` if `FUN` supports the `na.rm` argument, otherwise calls `FUN(x, ...)`.
#'
#' @param x A vector.
#' @param FUN An aggregation function (e.g., `sum`, `mean`).
#' @param ... Additional arguments to pass to `FUN`.
#'
#' @return The result of `FUN(x, ...)`.
#' 
#' @noRd
#' 

safe_aggregate <- function(x, FUN, ...) {
  
  if("na.rm" %in% names(formals(FUN))) {
    return(FUN(x, na.rm = TRUE, ...))
  } else {
    return(FUN(x, ...))
  }
}
