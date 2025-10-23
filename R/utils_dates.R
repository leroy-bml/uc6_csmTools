#' Check if a vector is date-like
#'
#' Heuristically checks if a vector contains date or datetime values using common formats.
#'
#' @param x A vector to check.
#' @param dssat_fmt Logical. If TRUE, also checks for 5-digit YYDDD integers.
#' @param n_check Integer. Number of non-NA values to check.
#' @param threshold Numeric (0-1). Proportion of checked values that must parse as a date to return TRUE.
#'
#' @return Logical. TRUE if the vector is likely date-like.
#' 
#' @noRd
#' 

is_date <- function(x, dssat_fmt = FALSE, n_check = 5, threshold = 0.8) {
  
  # DSSAT format handling
  if (dssat_fmt && is.integer(x) && all(nchar(as.character(x)) == 5)) {
    x <- format(as.Date(as.character(x), format = "%y%j"), "%Y-%m-%d")
  }
  
  # Date-like heuristics
  if (!is.character(x) && !is.factor(x) &&
      !lubridate::is.Date(x) && !lubridate::is.POSIXct(x) && !lubridate::is.POSIXlt(x)) {
    return(FALSE)
  }
  x <- as.character(x)
  x <- x[!is.na(x) & nzchar(x)] # remove NAs
  if (length(x) == 0) return(FALSE)
  if (!any(grepl("[-/T]", x))) return(FALSE) # at least one date-like separator
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


#' Standardize date or datetime strings
#'
#' Parses a vector using common date formats and returns a standardized character or Date object.
#'
#' @param x A character vector of date strings.
#' @param output_format Character. The desired `format()` string or "Date" to return a Date object.
#'
#' @return A vector of standardized dates, or the original vector if parsing fails.
#' 
#' @noRd
#' 

standardize_date <- function(x, output_format = "%Y-%m-%d") {
  
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

  return(x)  # If nothing worked, return original
}
