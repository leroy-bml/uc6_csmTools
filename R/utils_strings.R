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


#' Strictly abbreviate a string
#'
#' Create a standard abbreviation; handles non-ASCII characters by transliterating them, then removes all punctuation
#' (anything not alphanumeric or a space) before calling the base `abbreviate()` function.
#'
#' @param string Character vector to abbreviate.
#' @param minlength Integer. The minimum length of the abbreviation, passed to `abbreviate()`.
#'
#' @return A character vector of abbreviated strings.
#'
#' @noRd
#'

strict_abbreviate <- function(string, minlength = 2) {
  
  safe_abbreviate <- function(s) {
    warning_triggered <- FALSE
    
    # --- Check and correct for non-ASCII chars ---
    withCallingHandlers({
      if (any(grepl("non-ASCII", iconv(s, "latin1", "ASCII")))) {
        warning_triggered <<- TRUE
      }
    }, warning = function(w) {
      invokeRestart("muffleWarning")
    })
    if (warning_triggered) {
      s <- stringi::stri_trans_general(s, "Latin-ASCII")
    }
    
    # Remove punctuation to correct 'abbreviate' behaviour
    s_no_punct <- gsub("[^[:alnum:] ]", "", s)
    
    # Abbbreviate
    result <- abbreviate(s_no_punct, minlength)
    
    return(result)
  }
  
  # Apply to each element
  out <- ifelse(!is.na(string),
                vapply(string, safe_abbreviate, character(1)),
                "XX") # Default for NA inputs
  return(out)
}
