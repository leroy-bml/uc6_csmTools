#' Check if a Vector Contains Date-like Values
#'
#' Determines whether the input vector contains values that can be interpreted as dates or datetimes, using a variety of common date formats and heuristics. Optionally, supports DSSAT-specific date format conversion.
#'
#' @param x A vector to check for date-like values. Can be character, factor, Date, POSIXct, POSIXlt, or integer (for DSSAT format).
#' @param dssat_fmt Logical. If TRUE, treats integer values of length 5 as DSSAT date format (YYDDD) and converts them to standard date strings. Default is FALSE.
#' @param n_check Integer. Number of non-missing, non-empty values to check for date-likeness. Default is 5.
#' @param threshold Numeric. Proportion (between 0 and 1) of successfully parsed dates (among checked values) required to return TRUE. Default is 0.8.
#'
#' @details
#' The function first optionally converts DSSAT-format integers (YYDDD) to standard date strings if \code{dssat_fmt} is TRUE. It then checks if the input is of a type that could represent dates (character, factor, Date, POSIXct, POSIXlt). It removes missing and empty values, and requires at least one value to contain a date-like separator ("-", "/", or "T").
#'
#' The function attempts to parse up to \code{n_check} values using a set of common date and datetime formats (including ISO 8601, US, European, and compact forms). If at least \code{threshold} proportion of the checked values are successfully parsed as dates, the function returns TRUE; otherwise, it returns FALSE.
#'
#' @return Logical. TRUE if the input vector is likely to contain date-like values, FALSE otherwise.
#'  
#' @examples
#' is_date(c("2023-01-01", "2023-02-15")) # TRUE
#' is_date(c("01/31/2023", "02/15/2023")) # TRUE (US format)
#' is_date(c("31/01/2023", "15/02/2023")) # TRUE (European format)
#' is_date(c("not a date", "2023-01-01")) # TRUE (1/2 is a date)
#' is_date(c("not a date", "still not a date")) # FALSE
#' is_date(23001L, dssat_fmt = TRUE) # TRUE (DSSAT format)
#' is_date(12345L) # FALSE (not DSSAT format)
#'
#' @importFrom lubridate is.Date is.POSIXct is.POSIXlt
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


#' Standardize Date or Datetime Strings to a Desired Format
#'
#' Attempts to parse input values as dates or datetimes using a variety of common formats, and returns them in a standardized output format.
#'
#' @param x A character vector of date or datetime strings to be standardized.
#' @param output_format Character. The desired output format for the dates. Defaults to \code{"\%Y-\%m-\%d"}. If set to \code{"Date"}, the function returns a Date object.
#'
#' @details
#' The function tries to parse each element of \code{x} using a set of common date and datetime formats, including ISO 8601, US, European, and compact forms. The first format that successfully parses any value is used for all elements. If \code{output_format} is \code{\"Date\"}, the result is returned as a Date object; otherwise, the result is formatted as a character vector using the specified format string.
#'
#' If none of the formats match, the original input is returned unchanged.
#'
#' @return A vector of the same length as \code{x}, with dates standardized to the specified format, or the original values if parsing fails.
#'
#' @examples
#' standardize_date(c("2023-01-01", "01/31/2023", "31/01/2023"))
#' standardize_date("2023-01-01T15:30:00", output_format = "%d-%m-%Y")
#' standardize_date("02/15/2023", output_format = "Date")
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
#' Applies a summary or aggregation function to a vector, automatically setting \code{na.rm = TRUE} if the function supports it, to safely handle missing values.
#'
#' @param x A vector of values to be aggregated or summarized.
#' @param FUN The function to apply to \code{x}. Should be a function that takes a vector as its first argument. If \code{FUN} supports the \code{na.rm} argument, it will be set to \code{TRUE} automatically.
#' @param ... Additional arguments to pass to \code{FUN}.
#'
#' @details
#' This utility checks whether the provided function \code{FUN} has an \code{na.rm} argument (commonly used in base R summary functions like \code{sum}, \code{mean}, \code{min}, \code{max}, etc.). If so, it calls the function with \code{na.rm = TRUE} to remove missing values before aggregation. If not, it calls the function without \code{na.rm}.
#'
#' This is useful for writing robust code that works with both base R and custom aggregation functions, regardless of whether they support missing value removal.
#'
#' @return The result of applying \code{FUN} to \code{x}, with \code{na.rm = TRUE} if supported.
#'
#' @examples
#' safe_aggregate(c(1, 2, NA, 4), sum)         # Returns 7
#' safe_aggregate(c(1, 2, NA, 4), mean)        # Returns 2.333...
#' safe_aggregate(c(1, 2, NA, 4), length)      # Returns 4 (length does not support na.rm)
#' safe_aggregate(c(1, 2, 3), function(x) max(x) + 1) # Returns 4
#'
#' @export 

safe_aggregate <- function(x, FUN, ...) {
  
  if("na.rm" %in% names(formals(FUN))) {
    return(FUN(x, na.rm = TRUE, ...))
  } else {
    return(FUN(x, ...))
  }
}


#' Find Rows Present in Only One of Two Data Frames
#'
#' Returns the rows that are present in either \code{df1} or \code{df2}, but not in both. This is equivalent to the symmetric difference of the two data frames by rows.
#'
#' @param df1 A data frame.
#' @param df2 A data frame to compare with \code{df1}.
#'
#' @details
#' The function uses \code{anti_join} from the \strong{dplyr} package to find rows in \code{df1} that are not in \code{df2} and vice versa. It then combines these unique rows from both data frames using \code{bind_rows}. The result is a data frame containing all rows that are unique to either \code{df1} or \code{df2}.
#'
#' Both data frames should have the same column structure for meaningful comparison.
#'
#' @return A data frame containing the rows that are present in only one of the two input data frames.
#'
#' @examples
#' library(dplyr)
#' df1 <- data.frame(a = 1:3, b = c("x", "y", "z"))
#' df2 <- data.frame(a = 2:4, b = c("y", "z", "w"))
#' substr_rows(df1, df2)
#' # Returns rows with a = 1, b = "x" and a = 4, b = "w"
#'
#' @importFrom dplyr anti_join bind_rows
#'

substr_rows <- function(df1, df2) {
  
  diff1 <- anti_join(df1, df2)
  diff2 <- anti_join(df2, df1)
  
  diff <- bind_rows(diff1, diff2)
  
  return(diff)
}


#' Revert a List of Named Lists to a List of Lists by Element
#'
#' Transposes a list of named lists so that each element of the output list contains the values for a given name across all input lists.
#'
#' @param ls A list of named lists, all with the same names.
#'
#' @details
#' This function takes a list of named lists (e.g., the result of splitting a data frame by row and converting each row to a named list) and transposes it. The result is a list where each element is a list of the values for a particular name across all input lists.
#'
#' This is the inverse operation of splitting a data frame into a list of named lists by row, and is useful for reconstructing column-wise lists from row-wise lists.
#'
#' @return A list of lists, where each element corresponds to a name in the original named lists and contains a list of values for that name across all input lists.
#'
#' @examples
#' input <- list(
#'   list(a = 1, b = "x"),
#'   list(a = 2, b = "y"),
#'   list(a = 3, b = "z")
#' )
#' revert_list_str(input)
#' # Returns a list: $a = list(1, 2, 3), $b = list("x", "y", "z")
#'
#' @export

revert_list_str <- function(ls) {
  x <- lapply(ls, `[`, names(ls[[1]]))
  apply(do.call(rbind, x), 2, as.list) 
}


#' Get the Name of a Data Frame Within a List
#'
#' Returns the name(s) of the element(s) in a list that are identical to a given data frame.
#'
#' @param ls A named list of data frames (or other objects).
#' @param df A data frame to search for within the list.
#'
#' @details
#' The function compares each element of the list \code{ls} to the provided data frame \code{df} using \code{identical}. It returns the name(s) of the list element(s) that are exactly identical to \code{df}.
#'
#' This is useful for retrieving the name(s) of a data frame within a list of data frames, especially after subsetting or manipulation.
#'
#' @return A character vector of the name(s) of the list element(s) that are identical to \code{df}. Returns an empty character vector if no match is found.
#'
#' @examples
#' df1 <- data.frame(a = 1:2)
#' df2 <- data.frame(a = 3:4)
#' mylist <- list(first = df1, second = df2)
#' get_df_name(mylist, df2)
#' # Returns \"second\"
#'

get_df_name <- function(ls, df) {
  names(ls)[sapply(ls, function(x) identical(x, df))]
}




#' Collapse Specified Columns into List-Columns by Group
#'
#' Groups a data frame by all columns except those specified, and collapses the specified columns into list-columns within each group.
#'
#' @param df A data frame.
#' @param cols A character vector of column names to collapse into list-columns.
#'
#' @details
#' The function groups the data frame by all columns except those listed in \code{cols}. For each group, it collects the values of the specified columns into lists, resulting in list-columns. This is useful for aggregating or nesting data by group, while retaining all values from the specified columns.
#'
#' The function uses \code{group_by_at} and \code{across} from the \strong{dplyr} package.
#'
#' @return A data frame grouped by the non-collapsed columns, with the specified columns collapsed into list-columns.
#'
#' @examples
#' library(dplyr)
#' df <- data.frame(
#'   id = c(1, 1, 2, 2),
#'   value1 = c('a', 'b', 'c', 'd'),
#'   value2 = 1:4
#' )
#' collapse_cols(df, c('value1', 'value2'))
#' # Returns a data frame with one row per id, and value1/value2 as list-columns
#'
#' @importFrom magrittr %>%
#' @importFrom dplyr group_by_at summarise across all_of
#' 

collapse_cols <- function(df, cols) {
  
  grp_cols <- setdiff(colnames(df), cols)
  
  df %>%
    group_by_at(grp_cols) %>%
    summarise(across(all_of(cols), ~ list(.x)), .groups = "drop")
}


#' Remove Duplicate Data Frames from a List
#'
#' Returns a list of data frames with duplicates (by value) removed, keeping only the first occurrence of each unique data frame.
#'
#' @param lst A list of data frames.
#'
#' @details
#' The function compares data frames in the input list by converting each to a string representation using \code{dput}. It identifies duplicates based on these string representations and returns a list containing only the first occurrence of each unique data frame.
#'
#' This approach compares data frames by content, not by reference or name. Note that this method may be slow for very large data frames.
#'
#' @return A list of data frames with duplicates removed.
#'
#' @examples
#' df1 <- data.frame(a = 1:2)
#' df2 <- data.frame(a = 3:4)
#' df3 <- data.frame(a = 1:2)
#' lst <- list(one = df1, two = df2, three = df3)
#' drop_duplicate_dfs(lst)
#' # Returns a list with df1 and df2 only (df3 is a duplicate of df1)
#'

drop_duplicate_dfs <- function(lst) {
  
  # Convert each data frame to a string representation
  df_strings <- sapply(lst, function(df) paste(capture.output(dput(df)), collapse = ""))
  
  # Find unique indices
  unique_indices <- !duplicated(df_strings)
  
  # Return only unique data frames
  lst[unique_indices]
}


#' Find Common Data Frames Between Two Lists
#'
#' Returns the data frames from the first list that are also present (identical in content) in the second list.
#'
#' @param list1 A list of data frames.
#' @param list2 A list of data frames to compare with \code{list1}.
#'
#' @details
#' The function compares data frames in \code{list1} and \code{list2} by converting each data frame to a string representation using \code{dput}. It then finds the intersection of these string representations and returns the data frames from \code{list1} that are present in both lists.
#'
#' This approach allows for comparison of data frames by value, not by reference or name. Note that this method may be slow for very large data frames.
#'
#' @return A list of data frames from \code{list1} that are also present in \code{list2}.
#'
#' @examples
#' df1 <- data.frame(a = 1:2)
#' df2 <- data.frame(a = 3:4)
#' df3 <- data.frame(a = 1:2)
#' list1 <- list(one = df1, two = df2)
#' list2 <- list(alpha = df3)
#' intersect_dfs(list1, list2)
#' # Returns a list containing df1, since df1 and df3 are identical
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


#' Remove columns containing only NAs from a data frame
#'
#' @param df A data frame.
#' @return A data frame with columns containing only NAs removed.
#' @examples
#' df <- data.frame(a = c(NA, NA), b = c(1, NA), c = c(NA, NA))
#' remove_all_na_columns(df)
#' 

remove_all_na_cols <- function(df) {
  df[, colSums(!is.na(df)) > 0, drop = FALSE]
}


#' Find the longest common prefix in a character vector
#'
#' This function takes a character vector and returns the longest common prefix shared by all elements.
#'
#' @param strings A character vector of strings to compare.
#'
#' @return A character string representing the longest common prefix among the input strings.
#'
#' @examples
#' find_common_prefix(c("apple", "apricot", "ape")) # returns "ap"
#' find_common_prefix(c("dog", "cat", "mouse")) # returns ""
#'

find_common_prefix <- function(strings) {
  common_prefix <- strings[1]
  for (str in strings) {
    while (!startsWith(str, common_prefix) && nchar(common_prefix) > 0) {
      common_prefix <- substr(common_prefix, 1, nchar(common_prefix) - 1)
    }
  }
  return(common_prefix)
}