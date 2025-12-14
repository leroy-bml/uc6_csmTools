#' Retrieve and map a DSSAT soil profile for a target location
#'
#' Downloads, extracts, and maps the closest SoilGrids soil profile to a specified latitude and longitude, returning the data in ICASA format.
#' 
#' @param lon (numeric) Longitude (x coordinate) of the target location [decimal degrees E/W, range 6:15]
#' @param lat (numeric) Latitude (y coordinate) of the target location [decimal degrees N/S, range 47:55]
#' @param src (character) Source repository of the soil data (currently only "soil_grids" is supported)
#' @param dir (character) Directory in which to store and extract the profile data. If 'NULL', the raw files are saved in a temporary location
#'   created in the working directory.
#' @param output_path (character) Optional file path to save the output.
#'     
#' @details
#' The function downloads and extracts the SoilGrids by-country dataset if not already present, determines the country for the given coordinates using reverse geocoding, and loads the corresponding soil profile file. It selects the profile closest to the specified coordinates, splits the data into metadata, profile, and layer sections, and maps each section to the ICASA data model using a mapping file. The institution is set to "ISRIC" for provenance.
#'
#' The function uses \code{get_soilGrids_dataverse}, \code{reverse_geocode}, \code{read_sol}, \code{map_data}, and \code{load_map} for data retrieval and transformation.
#'
#' @return A list with elements \code{SOIL_METADATA}, \code{SOIL_PROFILES}, and \code{SOIL_PROFILE_LAYERS}, each as a data frame or list of data frames in ICASA format. A comment attribute is attached describing data provenance.
#'
#' @examples
#' \dontrun{
#' soil_profile <- get_soil_profile(lat = 40.7128, lon = -74.0060)
#' str(soil_profile)
#' }
#'
#' @importFrom magrittr %>%
#' @importFrom tidygeocoder reverse_geocode
#' @importFrom dplyr mutate filter row_number select
#' @importFrom DSSAT as_DSSAT_tbl read_sol

#'
#' @export
#' 

get_soil_profile <- function(lon, lat, src = "soil_grids", dir = NULL, output_path = NULL, delete_raw_files = FALSE) {
  
  # =================================================================
  # 1- Resolving download directory
  # =================================================================
  
  if (is.null(dir)) {
    dir <- "./temp"
    if (!dir.exists(dir)) {
      dir.create(dir)
    }
  }
  
  # =================================================================
  # 1- Downloading DSSAT SoilGrids profiles from dataverse
  # =================================================================
  
  # Get Soil Grids DSSAT profile by country
  soil_dir <- .get_soilGrids_dataverse(dir = dir)
  
  
  # =================================================================
  # 2- Locating soil profile for the target location
  # =================================================================
  
  # Retrieve the country matching the target coordinates
  coords <- data.frame(x = lon, y = lat)
  message("Retrieving country...")
  addr <- reverse_geocode(coords, long = x, lat = y, method = 'osm', full_results = TRUE, quiet = TRUE)
  cc <- toupper(addr$country_code)  # get country code (ISO-###)
  
  # Read soil profile for the target country
  message("Loading soil profile...")
  profilename <- paste0(cc, ".SOL")
  profilename <- file.path(soil_dir, profilename)
  sg_cc <- lapply(profilename, read_sol)
  
  # Filter out target profile
  profile <- list()
  for (i in 1:length(sg_cc)){
    profile[[i]] <- sg_cc[[i]] %>%
      mutate(LAT_diff = LAT - coords[i,]$y,
             LON_diff = LONG - coords[i,]$x,
             dd = abs(LAT_diff) + abs(LON_diff)) %>%
      filter(dd == min(dd)) %>%
      filter(row_number() == 1) %>% # arbitrary filtering for cases when multiple profile match distance
      select(-LAT_diff, -LON_diff, -dd) %>%
      as_DSSAT_tbl()
  }
  # If two profiles incidentally matching, takes first
  if (length(profile) > 1) profile <- profile[1]
  names(profile) <- "SOIL"
  
  
  # =================================================================
  # 3- Mapping to ICASA (soilGrids dataverse always as DSSAT tables)
  # =================================================================
  
  # Map to ICASA format (MOVE OUTSIDE?)
  profile_icasa <- convert_dataset(
    dataset = profile,
    input_model = "dssat",
    output_model = "icasa"
  )
  
  
  # =================================================================
  # 3- Output formatting
  # =================================================================
  
  # Attach comment on data source
  method_notes <- c(
    "Soil profile generated using ISRIC SoilGrid1km (see Han et al. [2015] 10.1016/j.envsoft.2019.05.012)",
    paste("Data extracted from the Harvard Dataverse (10.7910/DVN/1PEEY0) on", Sys.Date())
  )
  # profile_icasa[["PROFILE_METADATA"]]  <- profile_icasa[["PROFILE_METADATA"]] %>%
  #   group_by(soil_profile_ID) %>%
  #   mutate(soil_profile_methods = list(method_notes)) %>%
  #   ungroup()
  attr(profile_icasa, "comments") <- method_notes
  
  # Write JSON if file name provided
  out <- export_output(profile_icasa, output_path)
  
  # Delete raw files if requested
  if (!file.remove(soil_dir)) {
    warning("Could not delete raw files (check output path or permissions).")
  }
  
  return(out)
}


#' Download and extract SoilGrids profile from Han et al. (2015); Harvard Dataverse
#'
#' Downloads and extracts the SoilGrids by-country soil profile dataset
#' from the Harvard Dataverse (Han et al. 2015; https://doi.org/10.7910/DVN/1PEEY0), if not already present.
#'
#' @param dir Character. Directory in which to store and extract the SoilGrids profile. Defaults to a temporary directory.
#'
#' @details
#' The function checks for the presence of the SoilGrids by-country zip file and its extracted directory in the specified
#'    location. If not found, it downloads the file from the Harvard Dataverse using the dataset DOI, saves it, and
#'    extracts its contents. If the data is already present, it simply returns the path to the extracted directory.
#'
#' The function uses \code{dataset_files} and \code{get_file} to interact with the Dataverse API.
#'
#' @return The path to the directory containing the extracted SoilGrids profile.
#'
#' @noRd
#'

.get_soilGrids_dataverse <- function(dir = tempdir()) {
  
  # TODO: replace tempdir() by some custom command to create temp in wd
  # Get metadata of the dataverse Soil Grids data publication (Han et al. 2015)
  metadata <- dataverse::dataset_files(
    dataset = "10.7910/DVN/1PEEY0",
    server  = "dataverse.harvard.edu"
  )
  
  by_country <- lapply(metadata, function(x) grepl("by country", x$dataFile$filename))
  ind <- which(unlist(by_country))  # identify the file containing the soil profile
  filename <- metadata[[ind]]$dataFile$filename  # get the file name
  
  zip_file <- file.path(dir, filename)  # zip file path
  unzip_dir <- file.path(dir, sub(".zip", "", filename))  # unzipped directory
  unzip_dir <- suppressWarnings(
    normalizePath(unzip_dir)
  )
  
  if (!dir.exists(unzip_dir)) {
    
    if (!file.exists(zip_file)) {
      
      message("Downloading SoilGrids soil profiles...")
      file <- dataverse::get_file(
        file = filename,
        dataset = "10.7910/DVN/1PEEY0",
        server  = "dataverse.harvard.edu"
      )
      
      writeBin(file, zip_file)  # save the zip file in specified directory
      rm(file)  # remove file from environment
      
      if (dir == tempdir()) {
        outmsg <- paste("The downloaded SoilGrids profile are in temp directory", 
                        dir, 
                        "as 'dir' is unspecified", 
                        sep = "\n")
      } else {
        outmsg <- paste("The downloaded SoilGrids profile are in", 
                        dir, 
                        sep = "\n")
      }
    } else {
      outmsg <- paste("SoilGrids profiles were located in", dir, sep = "\n")
    }
    
    message("Extracting soil profile...")
    zip::unzip(zip_file, exdir = dir)  # extract the profile
  } else {
    outmsg <- paste("SoilGrids profile were located in", dir, sep = "\n")
  }
  
  message(outmsg)
  return(unzip_dir)
}
