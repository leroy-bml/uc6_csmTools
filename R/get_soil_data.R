#' Download and Extract SoilGrids Profiles from Han et al. (2015); Harvard Dataverse
#'
#' Downloads and extracts the SoilGrids by-country soil profile dataset
#' from the Harvard Dataverse (Han et al. 2015; https://doi.org/10.7910/DVN/1PEEY0), if not already present.
#'
#' @param dir Character. Directory in which to store and extract the SoilGrids profiles. Defaults to a temporary directory.
#'
#' @details
#' The function checks for the presence of the SoilGrids by-country zip file and its extracted directory in the specified location. If not found, it downloads the file from the Harvard Dataverse using the dataset DOI, saves it, and extracts its contents. If the data is already present, it simply returns the path to the extracted directory.
#'
#' The function uses \code{dataset_files} and \code{get_file} to interact with the Dataverse API.
#'
#' @return The path to the directory containing the extracted SoilGrids profiles.
#'
#' @examples
#' \dontrun{
#' soil_dir <- get_soilGrids_dataverse()
#' list.files(soil_dir)
#' }
#'
#' @importFrom dataverse get_file dataset_files
#' @importFrom zip unzip
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

#' Retrieve and Map a SoilGrids Profile for a Given Location
#'
#' Downloads, extracts, and maps the closest SoilGrids soil profile to a specified latitude and longitude, returning the data in ICASA format.
#'
#' @param lat Numeric. Latitude of the target location.
#' @param lon Numeric. Longitude of the target location.
#' @param dir Character. Directory in which to store and extract the SoilGrids profiles. Defaults to a temporary directory.
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
#' soil_profile <- get_soilGrids_profile(lat = 40.7128, lon = -74.0060)
#' str(soil_profile)
#' }
#'
#' @importFrom magrittr %>%
#' @importFrom tidygeocoder reverse_geocode
#' @importFrom tidyselect everything
#' @importFrom dplyr mutate filter row_number select pull distinct
#' @importFrom tidyr unnest
#' @importFrom DSSAT as_DSSAT_tbl read_sol
#'
#' @export
#' 

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
  sg_cc <- lapply(profilename, read_sol)
  
  # Filter out target profile
  profiles <- list()
  for (i in 1:length(sg_cc)){
    profiles[[i]] <- sg_cc[[i]] %>%
      mutate(LAT_diff = LAT - coords[i,]$y,
             LON_diff = LONG - coords[i,]$x,
             dd = abs(LAT_diff) + abs(LON_diff)) %>%
      filter(dd == min(dd)) %>%
      filter(row_number() == 1) %>% # arbitrary filtering for cases when multiple profile match distance
      select(-LAT_diff, -LON_diff, -dd) %>%
      as_DSSAT_tbl()
  }
  
  # Reshape and map to ICASA format
  profile_metadata <- data_general <- data_layers <- list()
  
  for (i in 1:length(profiles)) {
    
    # Unnest layer data
    profile_full <- profiles[[i]] %>% unnest(everything())
    
    # Identify metadata, profile metadata and layer data attributes
    data_map <- load_map()
    
    attr_metadata <- data_map %>%
      filter(data_model == "dssat" & icasa_group == "PROFILE_METADATA") %>%
      pull(header)
    
    attr_general <- data_map %>%
      filter(data_model == "dssat" & icasa_group == "SOIL_PROFILE" & is.na(icasa_subgroup)) %>%
      pull(header)
    
    attr_layers <- data_map %>%
      filter(data_model == "dssat" & icasa_group == "SOIL_PROFILE" & icasa_subgroup == "SOIL_PROFILE_LAYERS") %>%
      pull(header)
    
    # Split sections
    profile_metadata[[i]] <-
      profile_full[names(profile_full) %in% attr_metadata] %>%
      distinct()
    data_general[[i]] <-
      profile_full[names(profile_full) %in% attr_general] %>%
      distinct()
    data_layers[[i]] <-
      profile_full[names(profile_full) %in% attr_layers]
    
    # Map to ICASA
    # TODO: replace eventually by updated routine convert_dataset
    profile_metadata[[i]] <- map_data(profile_metadata[[i]], input_model = "dssat", output_model = "icasa",
                                      header = "long", map = data_map, keep_unmapped = FALSE)
    data_general[[i]] <- map_data(data_general[[i]], input_model = "dssat", output_model = "icasa",
                                  header = "long", map = data_map, keep_unmapped = FALSE)
    data_layers[[i]] <- map_data(data_layers[[i]], input_model = "dssat", output_model = "icasa",
                                 header = "long", map = data_map, keep_unmapped = FALSE)
    
    # Add ISRIC as institution
    profile_metadata[[i]]$institute_name <- "ISRIC"
  }
  
  # Format output
  out <- list(PROFILE_METADATA = profile_metadata,
              SOIL_PROFILE = data_general,
              SOIL_PROFILE_LAYERS = data_layers)
  
  out <- lapply(out, function(ls) {
    if (length(ls)>1){
      return(ls)
    } else {
      return(ls[[1]])
    }
  })
  
  # Attach comment on data provenance
  method_notes <- c(
    "Soil profile generated using ISRIC SoilGrid1km (see Han et al. [2015] 10.1016/j.envsoft.2019.05.012)",
    paste("Data extracted from the Harvard Dataverse (10.7910/DVN/1PEEY0) on", Sys.Date())
  )
  out[["PROFILE_METADATA"]] <- out[["PROFILE_METADATA"]] %>%
    group_by(soil_profile_ID) %>%
    mutate(soil_profile_methods = list(method_notes)) %>%
    ungroup()
  
  return(out)
}

# TODO: XY in metadata rather than data?