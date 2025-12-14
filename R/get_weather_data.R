#' Wrapping function for downloading, formatting and imputing daily weather time series
#' 
#' Source repositories are currently limited to DWD Open Data Server, and imputation method to linear interpolation
#'  
#' @param lon (numeric) Longitude (x coordinate) of the target location [decimal degrees E/W, range 6:15]
#' @param lat (numeric) Latitude (y coordinate) of the target location [decimal degrees N/S, range 47:55]
#' @param from (character or date) Start date of the desired time range as YYYY-MM-DD (inclusive).
#' @param to (character or date) End date of the desired time range as YYYY-MM-DD (inclusive).
#' @param pars (character) A character string of length >= 1 specifying the target variables among:
#' \itemize{
#'   \item `"air_temperature"` (default)
#'   \item `"precipitation"`
#'   \item `"solar_radiation"`
#'   \item `"dewpoint"`
#'   \item `"relative_humidity"`
#'   \item `"wind_speed"`
#'   \item `"par"` (photosynthetically active radiation)
#'   \item `"evaporation"`
#'   }
#' @param res (character vector) Temporal resolution for each weather variable.Must match length of `pars`. Supported values:
#'   \itemize{
#'     \item `"daily"` (default)
#'     \item `"hourly"`
#'   }
#' @param src (character) Source repository (currently only "nasa_power" is supported).
#' @param output_path (character) Optional file path to save the output.
#'     
#' @return a list of dataframes containing daily weather for the requested location, time frame and variables.
#' 
#' @importFrom nasapower get_power
#' 
#' @export
#'

get_weather_data <- function(lon, lat, from, to, pars, res, src, output_path = NULL){
  
  # Check arguments
  src_handlers <- c("dwd", "nasa_power")
  src <- match.arg(src, src_handlers)
  pars_handlers <- c("air_temperature", "precipitation", "solar_radiation", "dewpoint",
                     "relative_humidity", "wind_speed", "par", "evaporation")
  pars <- match.arg(pars, pars_handlers, several.ok = TRUE)
  res_handlers <- c("daily", "hourly")
  res <- match.arg(res, res_handlers)
  
  # --- Fetch data from source ---
  switch(src,
         "dwd" = {
           return(message("dwd currently just a placeholder..."))
         },
         "nasa_power" = {
           
           # --- Define parameters ---
           params <- unlist(
             lapply(pars, function(x) {
               switch(x,
                      air_temperature = c("T2M", "T2M_MAX", "T2M_MIN"), 
                      precipitation = "PRECTOTCORR",
                      solar_radiation = "ALLSKY_SFC_SW_DWN",
                      wind_speed = "WS2M",
                      dewpoint = "T2MDEW",
                      relative_humidity = "RH2M",
                      par = "ALLSKY_SFC_PAR_TOT",
                      evaporation = "EVLAND",
                      x)
             })
           )
           # --- Downdload data ---
           wth_raw <- get_power(
             community = "ag",
             pars = params,
             temporal_api = res,
             lonlat = c(lon, lat),
             dates = c(as.character(from), as.character(to))
           )
         })
  
  # Remove external pointer attributes (not serialized at output resolution)
  attr(wth_raw, "problems") <- NULL
  # Format as a named list for mapping
  wth_out <- list(
    WEATHER_DAILY = wth_raw
  )
  
  wth_out <- export_output(wth_out, output_path = output_path)
  
  return(wth_out)
}