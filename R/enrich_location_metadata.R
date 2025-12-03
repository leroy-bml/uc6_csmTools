#' Enriches a data frame with geocoding data
#'
#' Finds unique coordinates, calls the geocoding API, joins the
#' results back, and coalesces elevation data.
#'
#' @param data The data frame to enrich (e.g., mngt_data, soil_data).
#' @param lat_col A string with the name of the latitude column.
#' @param lon_col A string with the name of the longitude column.
#'
#' @return The enriched data frame.
#'
#' @noRd
#' 

enrich_spatial_data <- function(data, lat_col, lon_col) {

  # Get unique coordinates
  coord_cols <- c(lat_col, lon_col)
  coords <- data |>
    dplyr::filter(!is.na(.data[[lat_col]]) & !is.na(.data[[lon_col]])) |>
    dplyr::distinct(across(all_of(coord_cols)))
  
  # Enrich spatial metadata
  message(paste0("Enriching ", nrow(coords), " unique locations..."))
  api_data <- .enrich_location_data(coords, lat_col = lat_col, long_col = lon_col)
  geocode_data <- dplyr::bind_cols(coords, api_data)
  
  # Join back to the original data frame
  out <- dplyr::left_join(data, geocode_data, by = coord_cols)
  
  return(out)
}


#' Enrich location data using geocoding APIs
#'
#' Fetches elevation (AWS) and reverse-geocoded address information (ArcGIS) for a data frame containing latitude and
#' longitude geocoordinates.
#' 
#'
#' @param df A data frame containing geocoordinate information.
#' @param lat_col The name of the latitude column.
#' @param long_col The name of the longitude column.
#'
#' @return The original data frame with new columns for geocoded data  (e.g., `ELEV`, `ADDR_FULL`, `City`, `Region`).
#'   
#' @noRd
#' 

.enrich_location_data <- function(coords, lat_col, long_col) {
  
  # --- Define output terms mappings ---
  name_map <- list(
    # Raw API Name = list(icasa = "...", dssat = "...")
    elevation = list(icasa = "field_elevation", dssat = "ELEV"),
    ShortLabel = list(icasa = "site_name", dssat = "SITE"),
    City = list(icasa = "field_sub_sub_country", dssat = "FLL3"),
    Region = list(icasa = "field_sub_country", dssat = "FLL2"),
    CntryName = list(icasa = "field_country", dssat = "FLL1"),
    CountryCode = list(icasa = "tmp_country_code", dssat = "TMP_COUNTRY_CODE")
  )
  raw_geo_names <- c("elevation", "ShortLabel", "City", "Region", "CntryName", "CountryCode")
  
  # --- Standardize input coords for APIs ---
  std_coords <- as.data.frame(coords[, c(long_col, lat_col)])
  names(std_coords) <- c("x", "y")
  
  # --- Get elevation (elevatr) ---
  elev_data <- tryCatch({
    elevatr::get_elev_point(std_coords, prj = 4326, src = "aws")
  }, error = function(e) {
    warning("Elevation API call (elevatr) failed.", call. = FALSE)
    # Return a compliant NA data frame
    return(data.frame(elevation = rep(NA_real_, nrow(std_coords))))
  })
  
  # --- Get address information via reverse geocoding (tidygeocoder) ---
  addr_data <- tryCatch({
    tidygeocoder::reverse_geocode(std_coords, long = "x", lat = "y", 
                                  method = 'arcgis', 
                                  full_results = TRUE, quiet = TRUE)
  }, error = function(e) {
    warning("Reverse geocode API call (tidygeocoder) failed.", call. = FALSE)
    return(NULL) # Handled below
  })
  
  # --- Combine results ---
  api_results <- elev_data
  if (!is.null(addr_data)) {
    available_geo_names <- intersect(names(addr_data), raw_geo_names)
    api_results <- dplyr::bind_cols(api_results, addr_data[, available_geo_names])
  }
  
  return(api_results)
}
