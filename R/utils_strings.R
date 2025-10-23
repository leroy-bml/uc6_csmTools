#' Find the longest common prefix in a character vector
#'
#' @param strings A character vector.
#'
#' @return A character string of the longest common prefix.
#' 
#' @noRd
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


#' Strictly abbreviate a string, handling non-ASCII
#'
#' Uses `abbreviate(strict=TRUE)` and transliterates to ASCII if needed.
#' Truncates if abbreviation is longer than `minlength`.
#'
#' @param string Character vector to abbreviate.
#' @param minlength Integer. The exact target length.
#'
#' @return A character vector of abbreviated strings.
#' 
#' @noRd
#' 

strict_abbreviate <- function(string, minlength = 2) {
  
  safe_abbreviate <- function(s) {
    warning_triggered <- FALSE
    abbr <- NULL
    
    withCallingHandlers({
      abbr <- abbreviate(s, minlength = minlength, strict = TRUE)
    }, warning = function(w) {
      if (grepl("non-ASCII", conditionMessage(w))) {
        warning_triggered <<- TRUE
        invokeRestart("muffleWarning")
      }
    })
    
    if (warning_triggered) {
      s <- stringi::stri_trans_general(s, "Latin-ASCII")
      abbr <- abbreviate(s, minlength = minlength, strict = TRUE)
    }
    
    if (nchar(abbr) > minlength) {
      abbr <- substr(abbr, 1, minlength)
    }
    abbr
  }
  
  out <- ifelse(!is.na(string),
                vapply(string, safe_abbreviate, character(1)),
                "XX")
  return(out)
}