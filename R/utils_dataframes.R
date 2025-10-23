#' Find rows present in only one of two data frames (symmetric difference)
#'
#' @param df1 A data frame.
#' @param df2 A data frame with the same column structure.
#'
#' @return A data frame containing rows unique to either `df1` or `df2`.
#' 
#' @noRd
#' 

substr_rows <- function(df1, df2) {
  
  diff1 <- dplyr::anti_join(df1, df2)
  diff2 <- dplyr::anti_join(df2, df1)
  
  diff <- dplyr::bind_rows(diff1, diff2)
  
  return(diff)
}


#' Collapse specified columns into list-columns by group
#'
#' Groups a data frame by all columns *except* those specified in `cols`, then nests `cols` into
#' list-columns.
#'
#' @param df A data frame.
#' @param cols A character vector of column names to collapse.
#'
#' @return A data frame with `cols` nested into list-columns.
#' 
#' @noRd
#' 

collapse_cols <- function(df, cols) {
  
  grp_cols <- setdiff(colnames(df), cols)
  
  df %>%
    dplyr::group_by_at(grp_cols) %>%
    dplyr::summarise(dplyr::across(dplyr::all_of(cols), ~ list(.x)), .groups = "drop")
}


#' Remove columns containing only NAs
#'
#' @param df A data frame.
#'
#' @return A data frame with all-NA columns removed.
#' 
#' @noRd
#' 

remove_all_na_cols <- function(df) {
  df[, colSums(!is.na(df)) > 0, drop = FALSE]
}
