#' Identify Production Season from Management Dates
#'
#' @description
#' Finds the earliest and latest dates across a list of crop management data frames to define the production season.
#'
#' @param mngt_list A list of data frames containing date columns.
#' @param period Character string. Reserved for future use.
#' @param output Character string specifying return format: `"bounds"` (default) for start/end dates
#'   or `"date_sequence"` for the full daily sequence.
#'
#' @return A vector of `Date` objects, either the two boundary dates or the full sequence of dates between them.
#'
#' @importFrom purrr reduce map
#' @importFrom dplyr across bind_rows mutate coalesce select filter distinct pull
#' @importFrom tidyr everything
#'
#' @examples
#' # Helper function for the example to work
#' is_date <- function(x) inherits(x, 'Date')
#'
#' # Sample list of management data frames
#' mngt <- list(
#'   data.frame(planting_date = as.Date("2023-05-10")),
#'   data.frame(harvest_date = as.Date("2023-09-25"))
#' )
#'
#' # Get the start and end dates
#' identify_production_season(mngt)
#' 
#' @export
#'

identify_production_season <- function(mngt_list, period = "cultivation_season", output = "bounds",
                                       dssat_date_fmt = FALSE) {
  
  output_handlers <- c("bounds", "date_sequence")
  output <- match.arg(output, output_handlers)
  period_handlers <- c("growing_season", "cultivation_season")
  period <- match.arg(period, period_handlers)
  
  date_list <- lapply(mngt_list, function(df) df[sapply(df, is_date, dssat_fmt = dssat_date_fmt)])
  
  # Merge all unique management dates in a single vector
  date_list <- date_list[sapply(date_list, function(df) ncol(df) != 0)]
  
  # Convert as dates
  if (dssat_date_fmt){
    date_list_fmt <- map(date_list, ~ {
      .x %>% 
        mutate(across(everything(), function(x) as.Date(as.character(x), format = "%y%j")))
    })
  } else {
    date_list_fmt <- map(my_list, ~ {
      .x %>% 
        mutate(across(everything(), as.Date))
    })
  }
  
  # Gather all dates in a single vector
  mngt_dates <- reduce(date_list_fmt, bind_rows) %>%
    mutate(mngt_dates = coalesce(!!!.)) %>%
    filter(!is.na(mngt_dates)) %>%
    distinct() %>%
    pull(mngt_dates)
  
  # Identify growing season bounds
  if (period == "growing_season") {
    planting_date <- date_list_fmt$PLANTING_DETAILS$PDATE
    if (is.null(planting_date)) {
      stop("Planting date is missing! The growing season cannot be identified.")
    }
  }
  if (period == "growing_season" & !is.null(date_list_fmt$HARVEST)) {
    eos_date <- date_list_fmt$HARVEST$HDATE
  } else {
    eos_date <- max(mngt_dates)
  }
  
  # Define season bounds based on user input
  if (period == "growing_season") {
    season_bounds <- c(planting_date, max(mngt_dates))
  } else {
    season_bounds <- c(min(mngt_dates), max(mngt_dates))
  }
  
  # Format output
  if (output == "bounds") {
    return(season_bounds)
  } else if (output == "date_sequence") {
    season_dates <- seq(from = as.Date(season_bounds[1]),
                        to = as.Date(season_bounds[2]),
                        by = "day")
    return(season_dates)
  }
}
