#' Determine whether a vector is date data
#' 
#' @export
#'
#' @param x a vector
#' 
#' @return a logical value indicating whether x is a date or not
#' 
#' @importFrom lubridate parse_date_time
#' 


is_date <- function(x, dssat_fmt = FALSE, n_check = 5, threshold = 0.8) {
  # DSSAT format handling (unchanged)
  if (dssat_fmt && is.integer(x) && all(nchar(as.character(x)) == 5)) {
    x <- format(as.Date(as.character(x), format = "%y%j"), "%Y-%m-%d")
  }
  
  # Date-like heuristics
  if (!is.character(x) && !is.factor(x) && !is.Date(x) && !is.POSIXct(x) && !is.POSIXlt(x)) return(FALSE) # character or factor
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x)]  # remove NAs
  if (length(x) == 0) return(FALSE)
  if (!any(grepl("[-/T]", x))) return(FALSE)  # at least one date-like separator
  x <- head(x, n_check)
  
  formats <- c(
    "%Y-%m-%dT%H:%M:%S",  # ISO 8601 with T
    "%Y-%m-%dT%H:%M",     # ISO 8601 with T, no seconds
    "%Y-%m-%d %H:%M:%S",  # Datetime with space
    "%Y-%m-%d %H:%M",     # Datetime with space, no seconds
    "%Y-%m-%d",           # Date only
    "%m/%d/%Y",           # US format
    "%d/%m/%Y",           # European format
    "%Y/%m/%d",           # Year first with slashes
    "%Y%m%d",             # Compact YMD
    "%m%d%Y",             # Compact MDY
    "%d%m%Y",             # Compact DMY
    "%Y-%m-%dT%H:%M:%OS", # ISO 8601 with fractional seconds
    "%Y-%m-%dT%H:%M:%OSZ" # ISO 8601 with Zulu time
  )
  
  n_valid <- 0
  for(fmt in formats) {
    res <- try(as.POSIXct(x, format = fmt, tz = "UTC"), silent = TRUE)
    if(!inherits(res, "try-error")) {
      n_valid <- sum(!is.na(res))
      if (n_valid / length(x) >= threshold) return(TRUE)
    }
  }
  FALSE
}


#'
#'
#'

standardize_date <- function(x, output_format = "%Y-%m-%d") {
  # Try all common input formats
  formats <- c(
    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
    "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d", "%Y%m%d", "%m%d%Y", "%d%m%Y"
  )
  for (fmt in formats) {
    res <- suppressWarnings(as.POSIXct(x, format = fmt, tz = "UTC"))
    if (any(!is.na(res))) {
      # Output as Date or as character in desired format
      if (output_format == "Date") {
        return(as.Date(res))
      } else {
        return(format(res, output_format))
      }
    }
  }
  # If nothing worked, return original
  return(x)
}



#' Skip NAs in aggregation functions regardless of whether na.rm is an argument
#' 
#' @export
#'
#' @param x a function name
#' 
#' @return a function name with na.rm argument as appropriate
#' 


safe_aggregate <- function(x, FUN, ...) {
  
  if("na.rm" %in% names(formals(FUN))) {
    return(FUN(x, na.rm = TRUE, ...))
  } else {
    return(FUN(x, ...))
  }
}

#' Subset rows from two data frames
#' 
#' Subsets rows from two data frames by returning te rows that are unique to each one
#' 
#' @export
#'
#' @param df1 the first data frame
#' @param df2 the second data frame
#' 
#' @importFrom dplyr anti_join bind_rows
#' 
#' @return a data frame containing the rows that are unique to each data frame
#' 


substr_rows <- function(df1, df2) {
  
  diff1 <- anti_join(df1, df2)
  diff2 <- anti_join(df2, df1)
  
  diff <- bind_rows(diff1, diff2)
  
  return(diff)
}

#' Revert hierarchy of nested lists
#' 
#' @export
#'
#' @return a list of lists
#' 


revert_list_str <- function(ls) {
  x <- lapply(ls, `[`, names(ls[[1]]))
  apply(do.call(rbind, x), 2, as.list) 
}

#' Extract the name of a data frame from a list of data frame
#' 
#' @export
#'
#' @param ls a list of data frames containing the focal data frame
#' @param df a data frame; the focal table
#' 
#' @return a length 1 character vector corresponding to the name of the mother table in the original dataset
#' 


get_df_name <- function(ls, df) {
  names(ls)[sapply(ls, function(x) identical(x, df))]
}


#' Append data frame attributes to the first non-join columns so that they are not lost at joining
#' 
#' @export
#' 
#' @return a list containing the two data frames with the attributes appended to the first non-join columns
#' 


attr_to_column <- function(df1, df2, attr_name){
  
  col1 <- setdiff(names(df1), names(df2))[1]
  attr(df1[[col1]], attr_name) <- attr(df1, attr_name)
  attr(df1, attr_name) <- NULL
  
  col2 <- setdiff(names(df2), names(df1))[1]
  attr(df2[[col2]], attr_name) <- attr(df2, attr_name)
  attr(df2, attr_name) <- NULL
  
  return(list(df1, df2))
}  # Not used, could be deleted?


#' Nest selected columns into single cells
#' 
#' This function is necessary to format composite management tables, such as INITIAL_CONDITIONS and IRRIGATION
#' 
#' @export
#' 
#' @param df a data frame to collapse
#' @param cols a character vector of column names to collapse
#' 
#' @return a data frame where the specified rows have been collapsed into single rows based on all unique combinations of the remaining columns
#' 
#' @importFrom dplyr "%>%" group_by summarise across
#' @importFrom tidyr all_of
#'

collapse_cols <- function(df, cols) {
  
  grp_cols <- setdiff(colnames(df), cols)
  
  df %>%
    group_by_at(grp_cols) %>%
    summarise(across(all_of(cols), ~ list(.x)), .groups = "drop")
}

#'
#'
#'
#'

drop_duplicate_dfs <- function(lst) {
  # Convert each data frame to a string representation
  df_strings <- sapply(lst, function(df) paste(capture.output(dput(df)), collapse = ""))
  # Find unique indices
  unique_indices <- !duplicated(df_strings)
  # Return only unique data frames
  lst[unique_indices]
}

#'
#'
#'
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
