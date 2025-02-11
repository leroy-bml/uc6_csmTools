#' Get SoilGrids DSSAT soil profiles published by Han et al., 2015, https://doi.org/10.7910/DVN/1PEEY0
#' 
#' The function first controls whether the dataset already exists in the specified directory 
#' 
#' @param dir a character vector specifying the target directory to save the data (default tempdir)
#' 
#' @importFrom dataverse dataset_files get_file
#' 
#' @export
#'

get_soilGrids_dataverse <- function(dir = tempdir()) {
  
  # Get metadata of the dataverse Soil Grids data publication (Han et al. 2015)
  metadata <- dataset_files(
    dataset = "10.7910/DVN/1PEEY0",
    server  = "dataverse.harvard.edu"
  )
  
  by_country <- lapply(metadata, function(x) grepl("by country", x$dataFile$filename))
  ind <- which(unlist(by_country))  # identify the file containing the soil profiles
  filename <- metadata[[ind]]$dataFile$filename  # get the file name
  
  zip_file <- file.path(dir, filename)  # zip file path
  unzip_dir <- file.path(dir, sub(".zip", "", filename))  # unzipped directory
  unzip_dir <- suppressWarnings(
    normalizePath(unzip_dir)
  )
  
  if (!dir.exists(unzip_dir)) {
    
    if (!file.exists(zip_file)) {
      
      message("Downloading soil profiles...")
      file <- get_file(
        file = filename,
        dataset = "10.7910/DVN/1PEEY0",
        server  = "dataverse.harvard.edu"
      )
      
      writeBin(file, zip_file)  # save the zip file in specified directory
      rm(file)  # remove file from environment
      
      if (dir == tempdir()) {
        outmsg <- paste("The downloaded SoilGrids profiles are in temp directory", 
                        dir, 
                        "as 'dir' is unspecified", 
                        sep = "\n")
      } else {
        outmsg <- paste("The downloaded SoilGrids profiles are in", 
                        dir, 
                        sep = "\n")
      }
    } else {
      outmsg <- paste("SoilGrids profiles were located in", dir, sep = "\n")
    }
    
    message("Extracting soil profiles...")
    unzip(zip_file, exdir = dir)  # extract the profiles
  } else {
    outmsg <- paste("SoilGrids profiles were located in", dir, sep = "\n")
  }
  
  message(outmsg)
  return(unzip_dir)
}

#' Get SoilGrids DSSAT soil profile for a specific location
#' 
#' 
#' @param lat numeric; target latitude in decimal degrees
#' @param lon numeric; target longitude in decimal degrees
#' @param dir a character vector specifying the directory where the data is located or should be saved
#' 
#' @importFrom tidygeocoder reverse_geocode
#' @importFrom DSSAT read_sol as_DSSAT_tbl
#' @importFrom dplyr mutate filter select
#' 
#' @export
#' 

get_soilGrids_profile(lat = lat, lon = lon)

get_soilGrids_profile <- function(lat, lon, dir = tempdir()) {
  
  # Get Soil Grids DSSAT profiles by country
  soil_dir <- get_soilGrids_dataverse(dir = dir)
  
  # Retrieve the country matching the target coordinates
  coords <- data.frame(x = lon, y = lat)
  message("Retrieving country...")
  addr <- reverse_geocode(coords, long = x, lat = y, method = 'osm', full_results = TRUE, quiet = TRUE)
  cc <- toupper(addr$country_code)  # get country code (ISO-###)
  
  # Read soil profiles for the target country
  message("Loading soil profiles...")
  profilename <- paste0(cc, ".SOL")
  profilename <- file.path(soil_dir, profilename)
  sg_cc <- read_sol(profilename)
  
  # Filter out target profile
  profile <- sg_cc %>%
    mutate(LAT_diff = LAT - coords$y,
           LON_diff = LONG - coords$x,
           dd = abs(LAT_diff) + abs(LON_diff)) %>%
    filter(dd == min(dd)) %>%
    select(-LAT_diff, -LON_diff, -dd) %>%
    as_DSSAT_tbl()
  
  return(profile)
}


# TODO: comments (cf. weather)
# TODO: XY in metadata rather than data?
# TODO: Get soil profiles for multiple locations are downloaded (id)
