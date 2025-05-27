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
#' @param lat a number vector; target latitude(s) in decimal degrees
#' @param lon a number vector; target longitude(s) in decimal degrees
#' @param dir a character vector specifying the directory where the data is located or should be saved
#' 
#' @importFrom tidygeocoder reverse_geocode
#' @importFrom DSSAT read_sol as_DSSAT_tbl
#' @importFrom dplyr mutate filter select
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
  prov_metadata <- profile_metadata <- layer_data <- list()
  
  for (i in 1:length(profiles)) {
    
    # Unnest layer data
    profile_full <- profiles[[i]] %>% unnest(everything())
    
    # Identify metadata, profile metadata and layer data attributes
    data_map <- load_map()
    
    prov_attr <- data_map %>%
      filter(dataModel == "dssat" & template_section == "SOIL_METADATA") %>%
      pull(header)
    
    profile_attr <- data_map %>%
      filter(dataModel == "dssat" & template_section == "SOIL_PROFILES") %>%
      pull(header)
    
    layers_attr <- data_map %>%
      filter(dataModel == "dssat" & template_section == "SOIL_PROFILE_LAYERS") %>%
      pull(header)
    
    # Split sections
    prov_metadata[[i]] <- profile_full[names(profile_full) %in% prov_attr] %>% distinct()
    profile_metadata[[i]] <- profile_full[names(profile_full) %in% profile_attr] %>% distinct()
    layer_data[[i]] <- profile_full[names(profile_full) %in% layers_attr]
    
    # Map to ICASA
    prov_metadata[[i]] <- map_data(prov_metadata[[i]], input_model = "dssat", output_model = "icasa",
                                   map = data_map, keep_unmapped = FALSE)
    profile_metadata[[i]] <- map_data(profile_metadata[[i]], input_model = "dssat", output_model = "icasa",
                                      map = data_map, keep_unmapped = FALSE)
    layer_data[[i]] <- map_data(layer_data[[i]], input_model = "dssat", output_model = "icasa",
                                map = data_map, keep_unmapped = FALSE)
    
    # Add ISRIC as institution
    prov_metadata[[i]]$INST_NAME <- "ISRIC"
  }
  
  # Format output
  out <- list(SOIL_METADATA = prov_metadata,
              SOIL_PROFILES = profile_metadata,
              SOIL_PROFILE_LAYERS = layer_data)
  
  # Attach comment on data provenance
  attr(out, "comments") <-
      c(
        "Soil profile generated using ISRIC SoilGrid1km (see Han et al. [2015] 10.1016/j.envsoft.2019.05.012)",
        paste(
          "Data extracted from the Harvard Dataverse (10.7910/DVN/1PEEY0) on", Sys.Date(), "using csmTools"
        )
      )
  
  out <- lapply(out, function(ls) {
    if (length(ls)>1){
      return(ls)
    } else {
      return(ls[[1]])
    }
  })
  
  return(out)
}

# TODO: XY in metadata rather than data?

# test---

#tmp <- get_soilGrids_profile(lat = 52.53998, lon = 5.56189)



