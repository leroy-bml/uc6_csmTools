#' Recursively apply a function to data frames in a list
#'
#' Traverses a list, including nested lists, and applies function `f` to every element that is a data frame.
#'
#' @param x A list, possibly nested, containing data frames.
#' @param f The function to apply to each data frame.
#' @param ... Additional arguments passed on to `f`.
#'
#' @return A list with the same structure as `x`.
#' 
#' @noRd
#' 

apply_recursive <- function(x, f, ...) {
  if (is.data.frame(x)) {
    return(f(x, ...))
  }
  if (is.list(x)) {
    return(lapply(x, apply_recursive, f = f, ...))
  }
  return(x)
}


#' Transpose a list of named lists
#'
#' Converts a list of row-like lists (e.g., `list(list(a=1, b=2), list(a=3, b=4))`)
#' into a list of column-like lists (e.g., `list(a=list(1, 3), b=list(2, 4))`).
#'
#' @param ls A list of named lists, all with the same names.
#'
#' @return A transposed list.
#' 
#' @noRd
#' 

revert_list_str <- function(ls) {
  x <- lapply(ls, `[`, names(ls[[1]]))
  apply(do.call(rbind, x), 2, as.list)
}


#' Get the name of a data frame from a list
#'
#' Finds the name of an element in `ls` that is `identical()` to `df`.
#'
#' @param ls A named list of data frames.
#' @param df A data frame to search for.
#'
#' @return A character vector of the matching name(s).
#' 
#' @noRd
#' 

get_df_name <- function(ls, df) {
  names(ls)[sapply(ls, function(x) identical(x, df))]
}


#' Remove duplicate data frames from a list
#'
#' Keeps only the first occurrence of each unique data frame, based on a string representation (`dput`).
#'
#' @param ls A list of data frames.
#'
#' @return A list with duplicate data frames removed.
#' 
#' @noRd
#' 

drop_duplicate_dfs <- function(ls) {
  
  # Convert each data frame to a string representation
  df_strings <- sapply(lst, function(df) paste(capture.output(dput(df)), collapse = ""))
  # Find unique indices
  unique_indices <- !duplicated(df_strings)
  
  # Return only unique data frames
  lst[unique_indices]
}


#' Find common data frames between two lists
#'
#' Returns data frames from `list1` that are also present (identical in content) in `list2`,
#' based on a string representation (`dput`).
#'
#' @param list1 A list of data frames.
#' @param list2 A list of data frames.
#'
#' @return A list of data frames from `list1` also found in `list2`.
#' 
#' @noRd
#' 

intersect_dfs <- function(list1, list2) {
  
  # Convert each data frame to a string representation
  str1 <- sapply(list1, function(df) paste(capture.output(dput(df)), collapse = ""))
  str2 <- sapply(list2, function(df) paste(capture.output(dput(df)), collapse = ""))
  
  # Find common string representations
  common_strs <- intersect(str1, str2)
  
  # Return the data frames from list1 that are in the intersection
  list1[which(str1 %in% common_strs)]
}
