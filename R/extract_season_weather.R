#' Split Weather Data by Cultivation Year
#'
#' Filters weather tables within an experiment list (dssat_exp) 
#' for the relevant cultivation season years, then returns 
#' a flattened list of data frames, one for each year, 
#' with the full year as a suffix in the name.
#'
#' @param dssat_exp A list element representing a single experiment, 
#'            containing weather tables (names matching "WEATHER") 
#'            and data needed by identify_production_season.
#'
#' @return A flat list of weather data frames (e.g., 
#'         $WEATHER_DAILY_2023, $WEATHER_DAILY_2024).
#'
#' @noRd
#' 

extract_season_weather <- function(dssat_exp) {
  
  # Find weather tables
  wth_tbls <- dssat_exp[grepl("WEATHER", names(dssat_exp))]
  
  # Identify bounds of the cultivation season
  cs_dates <- identify_production_season(
    dssat_exp,
    period = "cultivation_season",
    dssat_date_fmt = TRUE
  )
  cs_years <- unique(lubridate::year(cs_dates))
  cs_years_short <- substr(cs_years, 3, 4)
  year_map <- setNames(as.character(cs_years), cs_years_short)
  
  wth_split <- list()
  
  for (wth_name in names(wth_tbls)) {
    df <- wth_tbls[[wth_name]]
    
    # Filter focal years
    df_filtered <- filter(df, YEAR %in% cs_years_short)
    
    # Split into year-level data frames
    split_list <- split(df_filtered, f = df_filtered$YEAR)
    
    # Return as flat list with years as suffix
    for (short_year in names(split_list)) {
      full_year <- year_map[short_year]
      nm <- paste(wth_name, full_year, sep = "_")
      wth_split[[nm]] <- split_list[[short_year]]
    }
  }
  
  # Calculate yearly statistics
  wth_data <- wth_split[grepl("DAILY", names(wth_split))]
  wth_metadata <- wth_split[grepl("METADATA", names(wth_split))]
  wth_tstats <- lapply(wth_data, calculate_wth_stats)
  wth_metadata <- mapply(
    function(x, y) left_join(x, y, by = intersect(colnames(x), colnames(y))),
    wth_metadata, wth_tstats,
    SIMPLIFY = FALSE
  )
  
  # Update dataset
  dssat_exp[grepl("WEATHER", names(dssat_exp))] <- NULL
  out <- c(dssat_exp, wth_metadata, wth_data)
  
  return(out)
}