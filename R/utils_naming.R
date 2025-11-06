#' Validate DSSAT Entity Codes
#'
#' @description
#' Checks whether a character string matches the standard format for a given DSSAT entity code.
#'
#' @details
#' The function uses regular expressions to validate codes against DSSAT conventions:
#' - `experiment`: institute (2L) site (2L) experiment year (2D) experiment number (2D) (e.g., "UFGA8201").
#' - `cultivar`: institute (2L) cultivar number (4D) (e.g., "IB0015").
#' - `field`: institute (2L) site (2L) field number (4D) (e.g., "UFGA0001").
#' - `soil`: institute (2L) site (2L) epxeriment year (2D) soil profile number (4D)
#' - `weather_station`: institute (2L) site (2L) (e.g., "UFGA").
#'
#' @param x A character vector of codes to validate.
#' @param item The type of entity. One of "experiment", "cultivar", "field", "soil", or "weather_station".
#' @param framework The modeling framework (currently only "dssat" is supported).
#'
#' @return A logical vector indicating `TRUE` for each valid code.
#'

is_valid_dssat_id <- function(x, item, framework = "dssat"){
  
  if (is.null(x)) {
    return(FALSE)
  }
  
  args <- c("experiment", "cultivar", "field", "soil", "weather_station")
  item <- match.arg(item, args)
  
  pattern <- switch(
    item,
    "experiment" = "^[A-Z]{4}[0-9]{4}$",
    "cultivar" = "^[A-Z]{2}[0-9]{4}$",
    "field" = "^[A-Z]{4}[0-9]{4}$",
    "soil" = "(^[A-Z]{4}[0-9]{6}$)|(^[A-Z]{2}[0-9]{8}$)",
    "weather_station" = "^[A-Z]{4}$"
  )
  is_valid <- grepl(pattern, x)
  is_valid[is.na(is_valid)] <- FALSE  # Return FALSE if NA
  
  return(is_valid)
}


# ------------------------------------------------------------------------------------------------------------------------

#' Generate a Standard DSSAT Identifier
#'
#' @description
#' Creates a standardized, 8-character DSSAT-compliant code for an experiment, field, cultivar, soil profile,
#' or weather station.
#'
#' @param type The type of entity code to generate.
#' @param institution The name of the institution (used for a 2-letter abbreviation).
#' @param site The name of the site (used for a 2-letter abbreviation).
#' @param year The year of the experiment (used for a 2-digit code).
#' @param sequence_no A numeric sequence or ID to ensure uniqueness.
#'
#' @return An 8-character, uppercase DSSAT identifier string.
#'
#' @importFrom dplyr case_when
#' 


generate_dssat_id <- function(type, institution, site = NA, year = NA, sequence_no = NA) {
  
  institution <- ifelse(is.na(institution), "XX", institution)
  site <- ifelse(is.na(site), "XX", site)
  year <- ifelse(is.na(year), "XX", as.character(year)) # Ensure year is character
  
  inst_abbr <- strict_abbreviate(institution, 2)
  site_abbr <- ifelse(!is.na(site), strict_abbreviate(sub(" .*", "", site), 2), NA_character_)
  
  ids <- case_when(
    type == "experiment" ~ paste0(inst_abbr, site_abbr, substr(year, 3, 4), sprintf("%02d", sequence_no)),
    type == "field" ~ paste0(inst_abbr, site_abbr, sprintf("%04d", sequence_no)),
    type == "cultivar" ~ paste0(inst_abbr, sprintf("%04d", sequence_no)),
    type == "soil" ~ paste0(inst_abbr, site_abbr, substr(year, 3, 4), sprintf("%04d", sequence_no)),
    type == "weather_station" ~ paste0(inst_abbr, site_abbr),
    TRUE ~ NA_character_
  )
  
  toupper(ids)
}