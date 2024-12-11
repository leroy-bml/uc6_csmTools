#' Get SoilGrids interpolated profiles from country-specific DSSAT files
#' 
#' Data obtained from Harvard Dataverse publication
#' DOI: ####
#' 
#' @export
#' 
#' @param ##
#' 
#' @importFrom countrycode countrycode
#' @importFrom
#' 

library(tidygeocoder)
library(countrycode)
library(DSSAT)
library(dplyr)
lat <- 49.378
lon <- 10.12
soil_path <- "C:/DSSAT48/SoilGrids/dataverse_files/SoilGrids-for-DSSAT-10km v1.0 (by country)"


get_soilGrids <- function(lat, lon, soil_path) {
  
  # Find location
  coords <- data.frame(x = lon, y = lat)  
  addr <- reverse_geocode(coords, long = x, lat = y, method = 'osm',
                          full_results = TRUE, quiet = TRUE)
  cc <- toupper(addr$country_code)
  
  filename <- paste0(soil_path, "/", cc, ".SOL")
  
  sg_cc <- read_sol(filename)  # TODO: files as package data OR API call
  
  profile <- sg_cc %>%
    mutate(LAT_diff = LAT - coords$y,
           LON_diff = LONG - coords$x) %>%
    mutate(dd = abs(LAT_diff) + abs(LON_diff)) %>%
    filter(dd == min(dd)) %>%
    select(-c(LAT_diff, LON_diff, dd))
  
  return(profile)
}




#' Download Soil Grids interpolation soil data for a given set of coordinates
#' 
#' ###
#' 
#' @export
#'
#' @param vars a character vector specifying the variable(s) to download
#' @param id ###
#' @param lat ###
#' @param lon ###
#' @param src ###
#' 
#' @return ###
#' 
#' @importFrom soilDB fetchSoilGrids
#' @importFrom dplyr "%>%" select
#' @importFrom tidyr separate
#' 


download_sg <- function(metadata = NULL, lat = NULL, lon = NULL, src = "isric"){
  
  if (is.null(metadata)){
    if(length(lat)!=length(lon)) {
      stop("Lengths of lat and lon arguments differ. Please provide lattitude and longitude for all locations.")
    } else {
      id <- seq_along(lat)
      fields <- data.frame(id = as.character(id), lat = lat, lon = lon)
    }
  } else {
    fields <- data.frame(id = as.character(metadata$id), lat = metadata$latitude, lon = metadata$longitude)
  }
  
  # Query SoilGrids to retrieve raw data for the experimental site
  raw_data <- fetchSoilGrids(fields)
  
  # Extract horizon data
  horizons <- as.data.frame(raw_data@horizons) %>%
    separate(label, into = c("updep","lodep"), sep = "-") %>%  # set low depth as the standard soil depth value
    #left_join(fields, by = "id") %>%  # append coordinates
    select(id, lodep, contains("mean")) %>%  # use mean statistic
    mutate_all(as.numeric)
  
  names(horizons) <- gsub(pattern = "mean", replacement = "", x = names(horizons))
  
  # Append metadata
  if (!is.null(metadata)) {
    attr(horizons, "metadata") <- metadata
  } else {
    attr(horizons, "metadata") <- fields
  }
  attr(horizons, "soil_source") <- src
  
  # Split soil profiles if multiple locations
  if (length(unique(horizons$id)) > 1) horizons <- split(horizons, f = horizons$id)
  
  return(horizons)
}



#' Format DWD weather data into a specified data standard
#' 
#' Transform DWD weather data into a specified data standard (as of 14.12.23: ICASA or DSSAT)
#' 
#' @export
#'
#' @param data a character vector specifying the variable(s) to download
#' @param lookup a map for a target standards (TODO: replace by standard name, fetch the map internally)
#' 
#' @return a data frame; weather data tabes formatted into the DSSAT standard structure
#' ##DPLYR TIDYR STRINGR soilDB MAPS
#' @importFrom lubridate is.POSIXct is.POSIXlt is.POSIXt as_date 
#' 


format_soil <- function(data, lookup) {  # only works with provided metadata
  
  metadata <- attr(data, "metadata")
  
  # Create empty dataframe with model-standard naming
  lookup_sub <- lookup[lookup$repo_var %in% names(data),]
  sol_data <- as.data.frame(matrix(ncol = nrow(lookup_sub), nrow = nrow(data)))
  names(sol_data) <- lookup_sub$std_var
  
  for (i in 1:nrow(lookup_sub)) {
    
    old_name <- lookup_sub$repo_var[i]
    new_name <- lookup_sub$std_var[i]
    
    if (old_name %in% names(data)) {
      sol_data[[new_name]] <- data[[old_name]]
    } else {
      sol_data[[new_name]] <- NA
    }
  }
  
  # Convert into standard units
  for (i in 1:ncol(sol_data)) {
    
    #i <- 1 
    conv_fct <- lookup_sub$conv_fct[i]
    var <- as.numeric(sol_data[[i]])
    
    sol_data[i] <- var * conv_fct
  }
  
  # Drop empty columns
  na_cols <- colSums(is.na(sol_data)) == nrow(sol_data)
  sol_data <- sol_data[, !na_cols]
  
  # Format header (soil metadata)
  header <- data.frame(
    id = metadata$id,
    ins = ifelse(!is.null(metadata$trial_institution),
                 paste(str_extract_all(metadata$trial_institution, "[A-Z]+")[[1]], collapse = ""),
                 NA),
    SOIL_SOURCE = toupper(attr(data, "soil_source")),
    SITE = ifelse(!is.null(metadata$site),
                  paste(str_extract_all(metadata$site, "[A-Z]+")[[1]], collapse = ""),
                  NA),
    COUNTRY = map.where(database = "world", x = unique(data$lon), y = unique(data$lat)),
    DESCRIPTION = "placeholder", ##!
    SMHB = "IB001", SMPX = "IB001", SMKE = "IB001"  ## DSSAT placeholders
  ) %>%
    mutate(
      SOIL_DEPTH = max(sol_data$SLB),
      SOIL_LAT = unique(data$lat), SOIL_LONG = unique(data$lon),
      SOIL_NR = sprintf("%02s", as.character(id)),  # problem: 3 digits? 2 digits limit on SOL file (to test)
      SOIL_ID = paste0(substr(ins, 1, 2), substr(toupper(SITE), 1, 2), format(Sys.time(), "%Y"), SOIL_NR)
    ) %>%
    select(SOIL_ID, SOIL_SOURCE, SITE, COUNTRY, DESCRIPTION, SOIL_DEPTH, SOIL_LAT, SOIL_LONG, SMHB, SMPX, SMKE)

  # 
  out <- list(SOL_Surface = header, SOL_Layers = sol_data)
  return(out)
}

#format_soil(tmp2[[1]], sg_icasa)
# TODO: comments (cf. weather)
# TODO: XY in metadata rather than data?
# Process when multiple locations are downloaded (id)
