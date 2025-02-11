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


get_soilGrids_raw <- function(metadata = NULL, lat = NULL, lon = NULL, src = "isric"){
  
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
