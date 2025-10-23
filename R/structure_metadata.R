#' Validate DSSAT Entity Codes
#'
#' @description
#' Checks whether a character string matches the standard format for a given DSSAT entity code.
#'
#' @details
#' The function uses regular expressions to validate codes against DSSAT conventions:
#' - `experiment`: institute (2L) site (2L) experiment year (2D) experiment number (2D) (e.g., "UFGA8201").
#' - `cultivar`: institute (2L) cultivar number (4D) (e.g., "IB0015").
#' - `field`: institute (2L) site (2L) field number (4D) (e.g., "UFGA0001").
#' - `soil`: institute (2L) site (2L) epxeriment year (2D) soil profile number (4D)
#' - `weather_station`: institute (2L) site (2L) (e.g., "UFGA").
#'
#' @param x A character vector of codes to validate.
#' @param item The type of entity. One of "experiment", "cultivar", "field", "soil", or "weather_station".
#' @param framework The modeling framework (currently only "dssat" is supported).
#'
#' @return A logical vector indicating `TRUE` for each valid code.
#'

is_valid_dssat_code <- function(x, item, framework = "dssat"){
  
  if (is.null(x)) {
    return(FALSE)
  }
  
  args <- c("experiment", "cultivar", "field", "soil", "weather_station")
  item <- match.arg(item, args)
  
  pattern <- switch(
    item,
    "experiment" = "^[A-Z]{4}[0-9]{4}$",
    "cultivar" = "^[A-Z]{2}[0-9]{4}$",
    "field" = "^[A-Z]{4}[0-9]{4}$",
    "soil" = "(^[A-Z]{4}[0-9]{6}$)|(^[A-Z]{2}[0-9]{8}$)",
    "weather_station" = "^[A-Z]{4}$"
  )
  is_valid <- grepl(pattern, x)
  is_valid[is.na(is_valid)] <- FALSE  # Return FALSE if NA
  
  return(is_valid)
}


# ------------------------------------------------------------------------------------------------------------------------

#' Generate a Standard DSSAT Identifier
#'
#' @description
#' Creates a standardized, 8-character DSSAT-compliant code for an experiment, field, cultivar, soil profile,
#' or weather station.
#'
#' @param type The type of entity code to generate.
#' @param institution The name of the institution (used for a 2-letter abbreviation).
#' @param site The name of the site (used for a 2-letter abbreviation).
#' @param year The year of the experiment (used for a 2-digit code).
#' @param sequence_no A numeric sequence or ID to ensure uniqueness.
#'
#' @return An 8-character, uppercase DSSAT identifier string.
#'
#' @importFrom dplyr case_when
#' 


generate_dssat_id <- function(type, institution, site = NA, year = NA, sequence_no = NA) {
  
  institution <- ifelse(is.na(institution), "XX", institution)
  site <- ifelse(is.na(site), "XX", site)
  year <- ifelse(is.na(year), "XX", as.character(year)) # Ensure year is character
  
  inst_abbr <- strict_abbreviate(institution, 2)
  site_abbr <- ifelse(!is.na(site), strict_abbreviate(sub(" .*", "", site), 2), NA_character_)
  
  ids <- case_when(
    type == "experiment" ~ paste0(inst_abbr, site_abbr, substr(year, 3, 4), sprintf("%02d", sequence_no)),
    type == "field" ~ paste0(inst_abbr, site_abbr, sprintf("%04d", sequence_no)),
    type == "cultivar" ~ paste0(inst_abbr, sprintf("%04d", sequence_no)),
    type == "soil" ~ paste0(inst_abbr, site_abbr, substr(year, 3, 4), sprintf("%04d", sequence_no)),
    type == "weather_station" ~ paste0(inst_abbr, site_abbr),
    TRUE ~ NA_character_
  )
  
  toupper(ids)
}


# ------------------------------------------------------------------------------------------------------------------------

#' Structure DSSAT Management and Metadata
#'
#' @description
#' Processes and standardizes the core metadata and management metadata of a DSSAT dataset
#'
#' @details
#' This function performs several key operations:
#' 1.  **Enriches Field Data:** It uses external APIs (`elevatr`, `tidygeocoder`) to fetch and fill missing elevation
#'     and location details (city, region, country).
#' 2.  **Generates Standard IDs:** It calls `generate_dssat_id()` to create compliant identifiers for
#'     experiments (`EXP_ID`), fields (`ID_FIELD`), and cultivars (`INGENO`) if they are missing or invalid.
#' 3.  **Links Data:** It ensures that the generated IDs are consistent across the
#'     `GENERAL`, `FIELDS`, and `CULTIVARS` tables.
#'
#' @param dataset The mapped dataset as a list of data frames.
#'
#' @return The dataset with standardized `GENERAL`, `FIELDS`, and `CULTIVARS` tables.
#' 
#' @importFrom magrittr %>%
#' @importFrom dplyr group_by summarise across any_of mutate left_join cur_group_id ungroup select distinct coalesce
#' @importFrom tidyr unnest unite
#' @importFrom tidygeocoder reverse_geocode
#' @importFrom elevatr get_elev_point
#'

structure_dssat_mngt <- function(dataset) {
  
  # Drop NA columns to avoid failed joins due to unmatched mapping rules
  dataset <- lapply(dataset, remove_all_na_cols)
  
  
  # Extract main dataframes for easier handling
  provenance <- dataset[["GENERAL"]]
  locations  <- dataset[["FIELDS"]]
  cultivars  <- dataset[["CULTIVARS"]]
  notes <- lapply(dataset, function(df) attr(df, "comments"))
  # TODO: need comments to be mapped to individual sections upstream
  
  # --- Format and Enrich Metadata ---
  
  # Format provenance by collapsing list-like columns
  provenance <- provenance %>%
    # Group by EXP_ID and, if it exists, EXP_YEAR
    group_by(across(any_of(c("EXP_ID", "EXP_YEAR")))) %>%
    # Collapse attributes
    summarise(
      across(
        !any_of(c("EXP_ID", "EXP_YEAR")),
        ~paste(unique(na.omit(.x)), collapse = "; ")
      ),
      .groups = "drop"
    )
  
  # Deduplicate
  provenance <- unique(provenance)
  locations <- unique(locations)
  
  # Calculate mean coordinates for API calls
  coords <- data.frame(x = locations$XCRD, y = locations$YCRD)
  
  # Enrich missing elevation data via API call with error handling
  locations <- locations %>%
    # Create NA column if does not exist
    mutate(ELEV = if ("ELEV" %in% names(.)) ELEV else NA_real_) %>%
    # Retrieve elevation from geocoordinates with aws API
    mutate(ELEV = ifelse(is.na(ELEV),
                         tryCatch({
                           suppressMessages(get_elev_point(coords, prj = 4326, src = "aws"))$elevation
                         }, error = function(e) {
                           warning("Elevation API call failed. Returning NA.")
                           return(NA_character_)
                         }),
                         ELEV),
           # Fetch location information via reverse geocoding API
           ADDR_FULL = tryCatch({
             reverse_geocode(coords, long = x, lat = y, method = 'arcgis', full_results = TRUE, quiet = TRUE)
           }, error = function(e) {
             warning("Reverse geocode API call failed. Location metadata may be incomplete.")
             return(NA_character_)
           })) %>%
    unnest(ADDR_FULL) %>%
    # Fill gaps in location data
    mutate(FLNAME = if ("FLNAME" %in% names(.)) FLNAME else NA_character_) %>%
    mutate(FLNAME = ifelse(is.na(FLNAME), toupper(ShortLabel), FLNAME)) %>%
    mutate(SITE = if ("SITE" %in% names(.)) SITE else NA_character_) %>%
    unite("SITE", FLNAME, City, Region, CntryName, sep = ", ", na.rm = TRUE, remove = FALSE) %>%
    mutate(SITE = ifelse(is.na(SITE),
                             unite(., "SITE", FLNAME, City, Region, CntryName, sep = ", ", na.rm = TRUE, remove = FALSE)$WST_SITE,
                             SITE)) %>%
    mutate(XCRD_tmp = round(XCRD, 2), YCRD_tmp = round(YCRD, 2), ELEV_tmp = round(ELEV, 2)) %>%
    unite("SITE", SITE, XCRD_tmp, YCRD_tmp, ELEV_tmp, sep = "; ", na.rm = TRUE, remove = FALSE) %>%
    # Create DSSAT-format FIELDS id
    mutate(ID_FIELD = if ("ID_FIELD" %in% names(.)) ID_FIELD else NA_character_) %>%
    # Join institution name
    left_join(provenance, by = intersect(names(.), names(provenance))) %>%
    group_by(INSTITUTION, SITE, XCRD, YCRD) %>%
    mutate(ID_FIELD = ifelse(is_valid_dssat_code(ID_FIELD, "field", "dssat"),
                             ID_FIELD,
                             generate_dssat_id("field", INSTITUTION, SITE, sequence_no = cur_group_id()))) %>%
    ungroup() %>%
    select(colnames(locations), ELEV, ID_FIELD, FLNAME, SITE)
  
  # Generate standard cultivar identifier
  cultivars <- cultivars %>%
    # Create ID column if it does not exist yet
    mutate(INGENO = if ("INGENO" %in% names(.)) INGENO else NA_character_) %>%
    # Join institution name
    left_join(provenance, by = intersect(names(.), names(provenance))) %>%
    group_by(INSTITUTION, CNAME) %>%
    # Create standard identifier
    mutate(INGENO = ifelse(is_valid_dssat_code(INGENO, "cultivar", "dssat"),
                           INGENO,
                           generate_dssat_id("cultivar", INSTITUTION, sequence_no = cur_group_id()))) %>%
    ungroup() %>%
    select(colnames(cultivars), INGENO)
  
  # Generate standard experiment name
  provenance <- provenance %>%
    mutate(EXP_ID = if ("EXP_ID" %in% names(.)) EXP_ID else NA_character_) %>%
    mutate(EXP_YEAR = if ("EXP_YEAR" %in% names(.)) EXP_YEAR else NA_character_) %>%
    # Join site name
    left_join(locations, by = intersect(names(.), names(locations))) %>%
    group_by(INSTITUTION, SITE, EXP_YEAR) %>%
    # Create standard identifier
    mutate(EXP_ID_new = ifelse(is_valid_dssat_code(EXP_ID, "experiment", "dssat"),
                               EXP_ID,
                               generate_dssat_id("experiment", INSTITUTION, SITE, EXP_YEAR, sequence_no = row_number()))) %>%
    ungroup() %>%
    select(EXP_ID, EXP_YEAR, EXP_ID_new, SITE, colnames(provenance))
  
  # Format output
  dataset[["GENERAL"]] <- provenance
  dataset[["FIELDS"]] <- locations
  dataset[["CULTIVARS"]] <- cultivars
  
  # Update experiment ID globally
  exp_id_df <- provenance %>% select(EXP_ID, EXP_YEAR, EXP_ID_new) %>% distinct()
  out <- lapply(dataset, function(df) {
    if ("EXP_ID" %in% names(df)) {
      if ("EXP_YEAR" %in% names(df)) {
        df$EXP_YEAR <- as.character(df$EXP_YEAR)
      }
      
      df <- left_join(df, exp_id_df, by = intersect(names(df), names(exp_id_df)))
      
      df$EXP_ID <- coalesce(df$EXP_ID_new, df$EXP_ID)
      df[["EXP_ID_new"]] <- NULL
    }
    return(df)
  })
  
  return(out)
}


# ------------------------------------------------------------------------------------------------------------------------

#' Structure DSSAT Soil Data
#'
#' @description
#' Consolidates, enriches, and standardizes soil profile data for a DSSAT dataset.
#'
#' @details
#' This function merges the `SOIL_META`, `SOIL_GENERAL`, and `SOIL_LAYERS` tables into a single, comprehensive soil
#' data frame compatible with file writing functions from the 'DSSAT' package.
#' It then enriches this data by:
#' 1.  Fetching missing location metadata using external geocoding APIs.
#' 2.  Generating a standardized DSSAT soil ID (`PEDON`) if the existing one is invalid and updating the linked.
#'     `FIELDS` table accordingly
#'
#' @param dataset The mapped dataset as a list of data frames.
#'
#' @return The dataset with soil tables replaced by standardized `SOIL` and `SOIL_HEADER` data frames.
#' 
#' @importFrom magrittr %>%
#' @importFrom dplyr left_join bind_cols select any_of filter distinct group_by mutate coalesce first cur_group_id recode ungroup
#' @importFrom purrr compact reduce
#' @importFrom tidyr unite
#' @importFrom tibble deframe
#' @importFrom tidygeocoder reverse_geocode
#' @importFrom elevatr get_elev_point
#' @importFrom countrycode countrycode
#'

structure_dssat_sol <- function(dataset) {
  
  # Drop NA columns to avoid failed joins due to unmatched mapping rules
  dataset <- lapply(dataset, remove_all_na_cols)
  
  # --- 1. Extract and combine soil data sections ---
  sol_components <- list(
    meta = dataset[["SOIL_META"]],
    general = dataset[["SOIL_GENERAL"]],
    layers = dataset[["SOIL_LAYERS"]]
  )
  notes <- c(unlist(sol_components$meta$SL_METHODS_COMMENTS),
             unlist(sol_components$meta$SL_PROF_NOTES))
  
  sol_data <- sol_components %>%
    compact() %>%
    reduce(~{
      join_keys <- intersect(names(.x), names(.y))
      if (length(join_keys) > 0) {
        left_join(.x, .y, by = join_keys)
      } else {
        y_unique_cols <- select(.y, -any_of(names(.x)))
        bind_cols(.x, y_unique_cols)
      }
    })
  
  # Stop if core data is missing from the original dataset list
  if (is.null(dataset[["SOIL_META"]]) || is.null(dataset[["SOIL_LAYERS"]])) {
    stop("Core soil profile tables ('SOIL_META' or 'SOIL_LAYERS') are missing from the input dataset.")
  }

  # --- Format and enrich metadata ---
  if (all(is.na(sol_data$LAT)) || all(is.na(sol_data$LONG))) {
    warning("Location metadata (LAT, LONG) not found for any soil profile.")
    return(sol_data)
  }
  # Geocode unique coordinates once, with error handling
  # TODO: integrate in pipe to handle multiple profiles
  coords <- sol_data %>%
    filter(!is.na(LAT) & !is.na(LONG)) %>%
    distinct(LAT, LONG)
  sol_geodata <- tryCatch({
    reverse_geocode(coords, long = LONG, lat = LAT, method = 'arcgis', full_results = TRUE, quiet = TRUE)
  }, error = function(e) {
    warning("Reverse geocode API call failed. Location data may be incomplete.")
    return(NULL)
  })
  if (!is.null(sol_geodata)) {
    sol_data_full <- left_join(sol_data, sol_geodata, by = c("LAT", "LONG"))
  }
  
  # Standardize metadata attributes
  sol_data_fmt <- sol_data_full %>%
    group_by(PEDON) %>% # Process each soil profile as a group
    mutate(
      # Use coalesce() to fill in missing values from geocoded data
      COUNTRY = ifelse("COUNTRY" %in% names(.),
                       coalesce(COUNTRY, ShortLabel),
                       countrycode(CountryCode, origin = "iso3c", destination = "iso2c")),
      DESCRIPTION = ifelse("DESCRIPTION" %in% names(.), DESCRIPTION, NA_character_),
      SITE = ifelse("SITE" %in% names(.), coalesce(SITE, ShortLabel), ShortLabel),
      INST_NAME = ifelse("INST_NAME" %in% names(.),
                         INST_NAME,
                         first(dataset[["GENERAL"]]$INSTITUTION)),
      # TODO: add INST_NAME and DATE to DSSAT map
      YEAR = ifelse("DATE" %in% names(.), 
                    first(coalesce(as.character(year(DATE)), "0")),
                    9999),  # NOTE: Placeholder so ID generation does not fail
      DEPTH = ifelse("DEPTH" %in% names(.),
                     first(coalesce(DEPTH, max(SLB, na.rm = TRUE))),
                     max(SLB, na.rm = TRUE)),
      # TODO: function to estimate texture based on profile layers data; need to include sand?
      # Generate standardized soil ID
      PEDON_new = if (!is_valid_dssat_code(first(PEDON), "soil", "dssat")) {
        # Generate one new ID per group and apply it to all rows in that group
        generate_dssat_id(
          type = "soil",
          institution = first(INST_NAME),
          site = first(SITE),
          year = first(YEAR),
          sequence_no = cur_group_id() # Creates a unique ID for each PEDON group
        )
      } else {
        first(PEDON)
      }
    )
  
  # Update connection in FIELDS table
  sol_map <- sol_data_fmt %>%
    select(PEDON, PEDON_new) %>%
    distinct() %>%
    deframe()
  if ("ID_SOIL" %in% names(dataset[["FIELDS"]])) {
    dataset[["FIELDS"]] <- dataset[["FIELDS"]] %>%
      mutate(ID_SOIL = recode(ID_SOIL, !!!sol_map))
  } else {
    dataset[["FIELDS"]]$ID_SOIL <- sol_map[1]
    warnings(paste0("Soil profile connection to 'FIELDS' table missing! Defaulting to '", sol_map[1], "' ."))
  }
  
  # --- Format output ---
  sol_data_out <- sol_data_fmt %>%
    ungroup() %>%
    select(-any_of(c("YEAR", "INST_NAME", "SITE", "SL_METHODS_COMMENTS", "SL_PROF_NOTES")),
           -setdiff(names(sol_geodata), names(sol_data))) %>%
    mutate(PEDON = PEDON_new) %>%
    select(-PEDON_new)
  
  sol_header_metadata <- sol_data_fmt %>%
    ungroup() %>%
    mutate(tmp = ifelse(grepl("ISRIC", SL_METHODS_COMMENTS),
                        "ISRIC Soil Grids-derived synthetic profile",
                        INST_NAME)) %>%
    unite("TITLE", tmp, SITE, sep = ", ", na.rm = TRUE, remove = FALSE) %>%
    select(EXP_ID, TITLE, any_of(c("INST_NAME", "SITE", "SL_METHODS_COMMENTS", "SL_PROF_NOTES"))) %>%
    ungroup() %>%
    distinct()

  # Update dataset
  dataset[grepl("SOIL", names(dataset))] <- NULL
  out <- c(dataset, list(SOIL = sol_data_out, SOIL_HEADER = sol_header_metadata))
  
  return(out)
}


# ------------------------------------------------------------------------------------------------------------------------

#' Structure DSSAT Weather Data
#'
#' @description
#' Consolidates, filters, enriches, and standardizes weather data for a DSSAT dataset.
#'
#' @details
#' The function performs the following steps:
#' 1.  **Filters by Growing Season:** It removes daily weather data from outside the years covered by
#'     the experiment's management dates.
#' 2.  **Enriches Metadata:** It merges weather and field metadata, using external APIs to fill in missing location and
#'     elevation data.
#' 3.  **Calculates Summary Statistics:** It computes the annual average temperature (`TAV`) and monthly amplitude (`AMP`)
#'     required for the weather file header.
#' 4.  **Generates Standard IDs:** It creates a DSSAT-compliant weather station ID (`INSI`) and updates the linked
#'     `FIELDS` table accordingly.
#'
#' @param dataset The mapped dataset as a list of data frames.
#' @param data_model The target data model (default is "dssat").
#'
#' @return The dataset with weather tables replaced by standardized `WTH_DAILY` and `WTH_META` data frames.
#' 
#' @importFrom magrittr %>%
#' @importFrom dplyr bind_cols select any_of left_join full_join semi_join filter distinct mutate group_by ungroup cur_group_id
#' @importFrom purrr compact reduce
#' @importFrom lubridate month days
#' @importFrom tidyr pivot_longer unite
#' @importFrom tibble deframe
#' @importFrom tidygeocoder reverse_geocode
#' @importFrom elevatr get_elev_point
#'

structure_dssat_wth <- function(dataset, data_model = "dssat") {
  
  # Extract data sections
  wth_components <- list(
    meta = dataset[["WEATHER_METADATA"]],
    data = dataset[["WEATHER_DAILY"]]
  )
  notes <- lapply(wth_components, function(df) attr(df, "comments"))  # TODO: to be handled upstream
  
  wth_data <- wth_components %>%
    compact() %>%
    reduce(~{
      join_keys <- intersect(names(.x), names(.y))
      if (length(join_keys) > 0) {
        left_join(.x, .y, by = join_keys)
      } else {
        y_unique_cols <- select(.y, -any_of(names(.x)))
        bind_cols(.x, y_unique_cols)
      }
    })
  
  # --- Filter out data outside growing season for each DSSAT experiment ---
  
  # Get all management/measurement dates
  management_dfs <- dataset[!grepl("WEATHER", names(dataset))]
  year_list <- lapply(management_dfs, function(df) {
    date_cols <- grep("DAT", names(df), value = TRUE)
    if (length(date_cols) == 0 || !"EXP_ID" %in% names(df)) {
      return(NULL)
    }
    sub_df <- df[, c("EXP_ID", date_cols)]
    # Extract the 2-digit year from all date columns
    sub_df[, date_cols] <- lapply(sub_df[, date_cols], substr, 1, 2)
    return(unique(sub_df))
  })
  year_list <- year_list[!sapply(year_list, is.null)]
  
  # Identify the bounds of the growing season for each experiment (year level)
  merged_dates <- Reduce(function(x, y) full_join(x, y, by = "EXP_ID"), year_list)
  gs_years <- merged_dates %>%
    pivot_longer(cols = -EXP_ID, names_to = "source_column", values_to = "YEAR") %>%
    select(EXP_ID, YEAR) %>%
    filter(!is.na(YEAR) & YEAR != "") %>%
    distinct()
  
  # Filter weather time-series, keeping only the rows that match the years covered by growing season
  # CHECK: may imply duplicates weather data if batch writing (should overwrite?)
  wth_data_gs_filtered <- wth_components$data %>%
    mutate(YEAR = substr(DATE, 1, 2)) %>%
    semi_join(gs_years, by = c("EXP_ID", "YEAR"))
  
  
  # --- Enrich location metadata ---
  
  # Make flexible join keys between WEATHER and FIELDS
  join_keys <- intersect(names(wth_data), names(dataset[["FIELDS"]]))
  if ("INSI" %in% names(wth_data) && "WSTA" %in% names(dataset[["FIELDS"]])) {
    join_keys <- c(join_keys, INSI = "WSTA")
  }
  
  wth_meta_geo <- wth_components$meta %>%
    left_join(dataset[["FIELDS"]], by = join_keys) %>%
    # Create location information columns if missing
    mutate(LAT = if ("LAT" %in% names(.)) LAT else NA_real_) %>%
    mutate(LONG = if ("LONG" %in% names(.)) LONG else NA_real_) %>%
    mutate(ELEV = if ("ELEV" %in% names(.)) ELEV else NA_real_) %>%
    # Use FIELDS coordinates if no coordinates are provided
    mutate(LAT = ifelse(is.na(LAT), YCRD, LAT),
           # warning(" Weather station geocoordinates not found. Defaulting to the mean 'FIELDS' geocoordinates.")
           LONG = ifelse(is.na(LONG), XCRD, LONG),
           ELEV = ifelse(is.na(ELEV),
                         tryCatch({
                           suppressMessages(get_elev_point(data.frame(x = LONG, y = LAT), prj = 4326, src = "aws"))$elevation
                         }, error = function(e) {
                           warning("Elevation API call failed. Returning NA.")
                           return(NA_character_)
                         }),
                         ELEV)) %>%
    # Fetch location information via reverse geocoding API
    mutate(ADDR_FULL = tryCatch({
             reverse_geocode(data.frame(x = LONG, y = LAT), long = x, lat = y, method = 'arcgis', full_results = TRUE, quiet = TRUE)
           }, error = function(e) {
             warning("Reverse geocode API call failed. Location metadata may be incomplete.")
             return(NA_character_)
           })) %>%
    unnest(ADDR_FULL) %>%
    # Fill gaps in location data
    mutate(WST_NAME = if ("WST_NAME" %in% names(.)) WST_NAME else NA_character_) %>%
    mutate(WST_NAME = ifelse(is.na(WST_NAME), toupper(ShortLabel), WST_NAME)) %>%
    mutate(WST_SITE = if ("WST_SITE" %in% names(.)) WST_SITE else NA_character_) %>%
    mutate(WST_SITE = ifelse(is.na(WST_SITE),
                             unite(., "WST_SITE", WST_NAME, City, Region, CntryName, sep = ", ", na.rm = TRUE, remove = FALSE)$WST_SITE,
                             WST_SITE)) %>%
    # TODO: document in device/sensor metadata? Would need text mining workflow
    mutate(REFHT = if ("REFHT" %in% names(.)) REFHT else NA_real_,
           WNDHT = if ("WNDHT" %in% names(.)) WNDHT else NA_real_)


  # --- Calculate summary metadata ---
  wth_tstats <- wth_data_gs_filtered %>%
    mutate(TAVG = (TMAX + TMIN) / 2,
           # Breakdown DSSAT dates to extract year and month
           yr_2digit = as.numeric(substr(YEAR, 1, 2)),
           doy = as.numeric(substr(DATE, 3, 5)),
           YEAR = ifelse(yr_2digit > 50, 1900 + yr_2digit, 2000 + yr_2digit),
           MO = month(as.Date(paste0(YEAR, "-01-01")) + days(doy - 1),)) %>%
    group_by(EXP_ID, YEAR, MO) %>%
    mutate(MO_TAV = mean(TAVG, na.rm = TRUE)) %>%
    ungroup() %>%
    group_by(EXP_ID, YEAR) %>%
    summarise(AMP = (max(MO_TAV) - min(MO_TAV)) / 2,
              TAV = mean(TAVG, na.rm = TRUE), .groups = "drop") %>%
    distinct()
  wth_meta <- wth_meta_geo %>% left_join(wth_tstats, by = "EXP_ID")

  # --- Generate DSSAT standard weather station codes ---
  wth_meta_fmt <- wth_meta %>%
    mutate(INSI = if ("INSI" %in% names(.)) INSI else NA_character_) %>%
    # Join institution name
    left_join(dataset[["GENERAL"]], by = intersect(names(.), names(dataset[["GENERAL"]]))) %>%
    # Generate standard names
    group_by(INSTITUTION, WST_SITE) %>%
    mutate(INSI_new = ifelse(is_valid_dssat_code(INSI, "weather_station", "dssat"),
                         INSI,
                         generate_dssat_id("weather_station", INSTITUTION, WST_SITE, sequence_no = cur_group_id()))) %>%
    ungroup() %>%
    select(EXP_ID, WST_SITE, YEAR, INSI, INSI_new, LAT, LONG, ELEV, TAV, AMP, REFHT, WNDHT) %>%
    distinct()
  
  # Update connection in FIELDS table
  wth_map <- wth_meta_fmt %>%
    select(INSI, INSI_new) %>%
    distinct() %>%
    deframe()
  if ("WSTA" %in% names(dataset[["FIELDS"]])) {
    dataset[["FIELDS"]] <- dataset[["FIELDS"]] %>%
      mutate(WSTA = wth_map[match(WSTA, names(wth_map))] %||% WSTA)
  } else {
    dataset[["FIELDS"]]$WSTA <- wth_map[1]
    warnings(paste0("Weather station connection to 'FIELDS' table missing! Defaulting to '", wth_map[1], "' ."))
  }
  
  # --- Format output ---
  
  # TODO: add sensor metadata as comments upstream
  # Clear-up output
  wth_meta_out <- wth_meta_fmt %>%
    mutate(INSI = INSI_new) %>%
    select(EXP_ID, WST_SITE, YEAR, INSI, LAT, LONG, ELEV, TAV, AMP, REFHT, WNDHT) %>%
    distinct()
  
  # Update dataset
  out <- dataset
  out[["WTH_DAILY"]] <- wth_data_gs_filtered %>%
    select(any_of(c("EXP_ID", "YEAR", colnames(wth_components$data)))) %>%
    select(-any_of("EXP_YEAR"))
  # TODO: set correct DSSAT section in data map and subsequent code
  out[["WTH_META"]] <- wth_meta_out
  out[["WEATHER_METADATA"]] <- out[["WEATHER_DAILY"]] <- NULL  #tmp
  
  return(out)
}


# ------------------------------------------------------------------------------------------------------------------------

#' Normalize ICASA Management Data into Regimes
#'
#' @description
#' Restructures a list of ICASA management tables by aggregating unique sequences of events into standardized
#' "management regimes."
#'
#' @details
#' The core purpose of this function is to simplify complex, event-based management data. It processes each management
#' table (e.g., `FERTILIZERS`, `TILLAGE`) and identifies unique schedules of operations applied across plots or fields.
#' These unique schedules, or "regimes," are then assigned a new, consistent integer ID.
#' The function orders these new regime IDs based on the original `treatment_number` to maintain a logical sequence.
#'
#' The final output is a list containing two main components:
#' 1.  **`management`**: A list of the formatted management tables, now containing the new regime IDs.
#' 2.  **`management_matrix`**: The updated `TREATMENTS` table, which serves as a  cross-reference linking each
#'     treatment to its corresponding regime IDs for all management types.
#'
#' @param ls A named list of data frames containing the ICASA management sections
#'   (e.g., `TREATMENTS`, `FERTILIZERS`, `PLANTINGS`).
#' @param master_key The column name of the master identifier (e.g., "experiment_ID").
#' @param year_col The column name for the experimental year.
#' @param treatment_col The column name for the treatment number.
#' @param plot_col The column name for the plot identifier.
#'
#' @return A list containing the `management` list (formatted event tables) and  the `management_matrix`
#'   (the updated `TREATMENTS` table).
#'
#' @importFrom magrittr %>%
#' @importFrom dplyr mutate across any_of select distinct group_by left_join summarise arrange ungroup cur_group_id
#' @importFrom rlang !!! syms sym
#'

structure_icasa_mngt <- function(ls, master_key, year_col, treatment_col, plot_col) {
  
  # Add the master_key to the standard list of join columns
  str_cols <- c(master_key, year_col, treatment_col, plot_col)
  
  management_fmt <- list()
  management_matrix <- ls[["TREATMENTS"]]
  management <- ls[setdiff(names(ls), "TREATMENTS")]
  
  # Set treatment as numeric variable for ordering
  management_matrix <- management_matrix %>% 
    mutate(across(any_of(treatment_col), as.numeric))
  management <- lapply(management, function(df) {
    df %>% mutate(across(any_of(treatment_col), as.numeric))
  })
  
  # Standardize join key types in the main matrix to character to prevent join errors.
  management_matrix <- management_matrix %>% 
    mutate(across(any_of(str_cols), as.character))
  
  for (i in 1:length(management)) {
    
    df <- management[[i]]
    
    levels <- c("genotype_level", "field_level", "soil_analysis_level", "initial_conditions_level", "planting_level",
                "irrigation_level", "fertilizer_level", "org_materials_applic_lev", "mulch_level", "chemical_applic_level",
                "tillage_level", "environmental_modif_lev", "harvest_operations_level", "simulation_control_level")
    pkey <- intersect(colnames(df), levels)
    
    # Skip tables that don't have a primary key for management levels
    if (length(pkey) == 0) next
    
    comment_cols <- colnames(df[grepl("NOTE|COMMENT", colnames(df), ignore.case = TRUE)])
    str_cols_valid <- intersect(str_cols, names(df))
    
    # Standardize join key types in the event table to match the matrix.
    df <- df %>% mutate(across(any_of(str_cols_valid), as.character))
    
    grp_cols <- setdiff(colnames(df), c(str_cols, pkey, comment_cols))
    date_cols <- names(which(sapply(df, is_date)))
    
    # If no grouping columns exist, the table is a simple map between plot/treatment and a management level.
    if (length(grp_cols) == 0) {
      map_table <- df %>% 
        select(any_of(c(str_cols_valid, pkey))) %>% 
        distinct()
      management_matrix <- management_matrix %>%
        select(-any_of(pkey)) %>%
        left_join(map_table, by = str_cols_valid)
      management_fmt[[i]] <- df %>% 
        select(any_of(c(year_col, pkey, master_key))) %>% 
        distinct()
      next
    }
    
    df <- df %>%
      mutate(event_signature = do.call(paste, c(.[grp_cols], sep = "_"))) %>%
      # Identify unique event signature to check if identical across treatments
      group_by(event_signature) %>%
      # Compute ID to the event level rather than plot level
      mutate(!!pkey := cur_group_id()) %>%
      distinct() %>%
      ungroup()
    
    # Fork the logic for management (with dates) vs. resource (without dates) tables.
    if (length(date_cols) > 0) {
      # This is a MANAGEMENT table with a sequence of events.
      event_signature_map <- df %>%
        group_by(!!!rlang::syms(str_cols_valid)) %>%
        summarise(event_regime_signature = paste(event_signature, collapse = "|"), .groups = "drop")
      df <- df %>%
        arrange(across(all_of(c(str_cols_valid, date_cols)))) %>%
        left_join(event_signature_map, by = str_cols_valid)
      
    } else {
      # This is a RESOURCE table. Each unique resource is its own regime.
      df <- df %>%
        mutate(event_regime_signature = event_signature)
    }
    
    # Carry the master_key through the regime ID creation process.
    regime_keys <- c(master_key, year_col)
    
    # Set sorting variables for management regime numbering sequence
    sort_by_vars <- regime_keys
    if (treatment_col %in% names(df)) {
      sort_by_vars <- c(sort_by_vars, treatment_col)
    }
    
    regime_ids <- df %>%
      arrange(across(all_of(sort_by_vars))) %>%
      select(all_of(regime_keys), event_regime_signature) %>%
      distinct() %>%
      group_by(!!!rlang::syms(regime_keys)) %>%
      mutate(!!rlang::sym(pkey) := as.integer(factor(event_regime_signature))) %>%
      ungroup()
    
    # Replace pkey by the event regime ID
    df <- df %>%
      select(-!!rlang::sym(pkey)) %>%
      # Join using the master_key as well.
      left_join(regime_ids, by = c(regime_keys, "event_regime_signature"))
    
    mngt_event_regime <- df %>%
      select(any_of(str_cols_valid), !!rlang::sym(pkey)) %>%
      distinct()
    
    management_matrix <- management_matrix %>%
      select(-any_of(pkey)) %>%
      left_join(mngt_event_regime, by = str_cols_valid) %>%
      # NA levels are true 0s (no management)
      mutate(across(
        any_of(levels),
        ~ replace(., is.na(.), 0)
      ))
    
    management_fmt[[i]] <- df %>%
      select(any_of(master_key), !!rlang::sym(year_col), !!rlang::sym(pkey), all_of(grp_cols)) %>%
      arrange(!!rlang::sym(year_col), !!rlang::sym(pkey), if(length(date_cols) > 0) across(all_of(date_cols))) %>%
      distinct()
  }
  names(management_fmt) <- names(management)
  
  management_matrix <- management_matrix %>% distinct()
  
  list(management = management_fmt, management_matrix = management_matrix)
}

# ------------------------------------------------------------------------------------------------------------------------

#' Apply Post-Processing Transformations to a Mapped Dataset
#'
#' @description
#' A top-level function that applies model-specific structuring and aggregation logic to a dataset after the initial
#' mapping from a source model is complete.
#'
#' @details
#' This function acts as a dispatcher, calling the appropriate structuring functions based on the specified `data_model`.
#' - For **`data_model = "icasa"`**, it calls `structure_icasa_mngt()` to aggregate detailed management event data
#'   into standardized "regimes". It also  performs initial aggregations on field-level data.
#' - For **`data_model = "dssat"`**, it orchestrates a series of calls to other specialized functions
#'   (`structure_dssat_mngt`, `structure_dssat_sol`, `structure_dssat_wth`) to enrich metadata, generate standard IDs,
#'   split the dataset by experiment, and format the output for DSSAT files.
#'
#' @param dataset The mapped dataset as a list of named data frames.
#' @param data_model The target data model, either "icasa" or "dssat".
#' @param config_path The path to the `datamodels.yaml` configuration file.
#'
#' @return A list of data frames structured for the target data model; for DSSAT, a nested list with one sub-list
#'   for each experiment.
#'
#' @importFrom yaml read_yaml
#' @importFrom magrittr %>%
#' @importFrom dplyr group_by across all_of summarise first left_join select any_of mutate arrange
#' @importFrom rlang sym
#' @importFrom purrr imap compact
#' @importFrom tidyr unite
#' 
#' @export
#'

apply_transformations <- function(dataset, data_model = c("icasa", "dssat"),
                                  config_path = "./inst/extdata/datamodels.yaml") {
  
  if (data_model == "icasa") {

    # Get master key
    config <- yaml::read_yaml(config_path)
    model_config <- config[[data_model]]
    
    # NEW: Resolve the master key name via the design_keys section
    conceptual_master_key <- model_config$master_key
    master_key <- model_config$design_keys[[conceptual_master_key]]
    
    if (is.null(master_key)) {
      stop(paste("Master key for data model '", data_model, "' not found in config.", sep = ""))
    }
    # Get design keys
    year_col <- model_config$design_keys[["year"]]
    treatment_col <- model_config$design_keys[["treatment"]]
    plot_col <- model_config$design_keys[["plot"]]
    
    # Split data into major ICASA sections
    metadata <- dataset[intersect(c("GENERAL", "PERSONS", "INSTITUTIONS", "DOCUMENTS", "PLOT_DETAILS"), names(dataset))]
    measured <- dataset[intersect(c("SUMMARY", "TIME_SERIES"), names(dataset))]
    weather <- dataset[intersect(c("WEATHER_METADATA", "WEATHER_DAILY"), names(dataset))]
    soil <- dataset[intersect(c("SOIL_METADATA", "SOIL_LAYERS"), names(dataset))]
    #TODO: handle soil data: unpractical profile/analysis/measured split in ICASA
    management_sec <- c("GENOTYPES", "FIELDS", "SOIL_ANALYSES", "INITIAL_CONDITIONS", "PLANTINGS",
                        "IRRIGATIONS", "FERTILIZERS", "ORGANIC_MATERIALS", "MULCHES", "CHEMICALS",
                        "TILLAGE", "ENVIRON_MODIFICATIONS", "HARVESTS", "SIMULATION_CONTROLS","TREATMENTS")
    management <- dataset[intersect(management_sec, names(dataset))]
    
    # Initiate output objects
    out_dataset <- list()
    
    # --- Aggregate weather per day ---
    if (!is.null(weather[["WEATHER_DAILY"]])) {
    
      aggregated_daily <- weather[["WEATHER_DAILY"]] %>%
        mutate(weather_date = as.Date(weather_date)) %>%
        group_by(across(any_of(c("experiment_ID", "weather_station_id", "weather_station_name", "weather_date")))) %>%
        # Apply all variable specific aggrgation transformations
        summarise(
          across(any_of("maximum_temperature"), ~max(.x, na.rm = TRUE), .names = "{.col}"),
          across(any_of("minimum_temperature"), ~min(.x, na.rm = TRUE), .names = "{.col}"),
          # HACK: specific case for 10 minutes intervals (600 seconds) to get J/m2/d
          # TODO: define aggregation at the map level: different weather types
          #across(any_of("solar_radiation"), ~sum(.x, na.rm = TRUE)*600, .names = "{.col}"),
          across(any_of("solar_radiation"), ~.x, .names = "{.col}"),
          across(any_of(c("precipitation",  "sunshine_duration")),
                 ~sum(.x, na.rm = TRUE),
                 .names = "{.col}"),
          across(any_of(c("realtive_humidity_avg", "atmospheric_wind_speed", "temperature_dewpoint", "sensor_voltage")),
            ~ mean(.x, na.rm = TRUE),
            .names = "{.col}"),
          .groups = "drop"
        )
      # Append to output
      out_dataset <- c(out_dataset, weather)
      out_dataset[["WEATHER_DAILY"]] <- aggregated_daily
    }

    # TODO: Replace by aggregate phase in yaml
    if (length(management) > 0) {
      
      management[["FIELDS"]] <- management[["FIELDS"]] %>%
        # TODO: Add here function to split coordinates into distinct groups (regardless of coord system)
        group_by(across(all_of(c(master_key, year_col)))) %>%
        summarise(
          field_latitude = mean(plot_latitude , na.rm = TRUE),
          field_longitude = mean(plot_longitude, na.rm = TRUE),
          # More direct calculation for the mean of midpoint elevations
          field_elevation = mean(plot_elevation, na.rm = TRUE),
          coordinate_system = first(na.omit(coordinate_system)),  # TODO: create if does not exists
          .groups = "drop"
        ) %>%
        group_by(across(everything())) %>%
        summarise(field_level = n(), .groups = "drop")
      
      management[["TREATMENTS"]] <- management[["TREATMENTS"]] %>%
        # Remove old field ID column if it exists.
        select(-any_of("field_level")) %>%
        left_join(
          management[["FIELDS"]] %>% select(all_of(c(master_key, year_col)), field_level),
          by = c(master_key, year_col)
        )
      
      # Structure management data in ICASA structure
      management_fmt <- structure_icasa_mngt(management, master_key, year_col, treatment_col, plot_col)
      
      management <- management_fmt$management
      management_matrix <- management_fmt$management_matrix
      
      out <- list(metadata, management, list(TREATMENTS = management_matrix), weather, soil, measured)
      out <- do.call(c, out)
      
      # Sort values
      sort_vars <- c("experiment_year", "treatment_number", "plot_number")
      out_management <- lapply(out, function(df) {
        
        if (treatment_col %in% names(df)) {
          df <- df %>% mutate(!!rlang::sym(treatment_col) := as.numeric(!!rlang::sym(treatment_col)))
        }
        # Check which sorting variables exist in the current data frame
        existing_vars <- intersect(sort_vars, names(df))
        
        # If there are existing variables, sort the data frame by them
        if (length(existing_vars) > 0) {
          df <- arrange(df, across(all_of(existing_vars)))
        }
        
        return(df)
      })
      # Append to output
      out_dataset <- c(out_dataset, out_management)
    }
    
    out_dataset <- c(
      metadata,
      out_dataset, # Weather + management
      soil,
      measured
    )
  }
  
  else if (data_model == "dssat") {
    
    ### ------ Management & resources metadata and standard identifier --------
    dataset <- structure_dssat_mngt(dataset)
    
    ### ------ SOIL metadata + standard identifiers ---------------------------
    
    # Structure soil profile metadata and link to FIELDS table
    dataset <- structure_dssat_sol(dataset)
    
    ### ------ WEATHER metadata + standard identifiers ------------------------
    
    # Structure weather station metadata and link to FIELDS table
    dataset <- structure_dssat_wth(dataset)
    
    ### ------ Format output --------------------------------------------------
    
    # Split by experiment
    dataset_split <- split_dataset(dataset, key = "experiment", data_model = "dssat")
    
    # Split weather data files per calendar year for overlapping growing seasons
    dataset_split <- lapply(dataset_split, function(dataset) {
      wth_names <- names(dataset)[grepl("^WTH_", names(dataset))]
      
      for (nm in wth_names) {
        split_list <- split(dataset[[nm]], f = dataset[[nm]]$YEAR)
        # cleaned_list <- lapply(split_list, function(df) df[, setdiff(names(df), "YEAR")])
        for (yr in names(split_list)) {
          dataset[[paste0(nm, "_", yr)]] <- split_list[[yr]]
        }
        dataset[[nm]] <- NULL
      }
      dataset
    })
    
    # Deduplicate all dataframes
    dataset_split <- apply_recursive(dataset_split, dplyr::distinct)
    
    # Output formatting
    out_dataset <- lapply(dataset_split, function(ls) {
      
      weather_header <- ls[grepl("WTH_META", names(ls))]
      soil_header <- ls[["SOIL_HEADER"]]
      fields_table <- ls[["FIELDS"]]
      
      # Make base names
      wth_file_base <- sapply(weather_header, function(df){
        df %>% group_by(INSI) %>%
          mutate(YEAR_short = substr(YEAR, 3, 4),
                 group_id = sprintf("%02d", cur_group_id())) %>%
          unite("file_base", INSI, YEAR_short, group_id, sep = "", remove = FALSE) %>%
          pull(file_base)
      })
      
      sol_file_base <- fields_table %>%
        # TODO: add PEDON to header and replace fields table
        #group_by(PEDON) %>%  
        mutate(file_base = substr(ID_SOIL, 1, 2)) %>%
        pull(file_base)
      
      # Set section metadata
      components <- c(
        list(
          MANAGEMENT = ls[!names(ls) %in% c("SUMMARY", "TIME_SERIES", "SOIL", "SOIL_HEADER") & !grepl("^WTH_", names(ls))],
          SOIL = ls[["SOIL"]],
          SUMMARY = ls[["SUMMARY"]],
          TIME_SERIES = ls[["TIME_SERIES"]]
        ),
        # Add each separate weather file as an individual entry
        ls[grepl("^WTH_DAILY_", names(ls))]
      )
      exp_metadata  <- expand.grid(
        component = names(components),
        exp_code = unique(ls[["GENERAL"]]$EXP_ID),  # NOTE: should always be unique
        crop = unique(ls[["CULTIVARS"]]$CR)  # NOTE: should always be unique unless intercropping
      )
      
      # Assign filenames to the respective weather files
      weather_map <- tibble(
        component = names(ls)[grepl("^WTH_DAILY_", names(ls))],
        file_base = wth_file_base)
      
      exp_metadata <- exp_metadata %>%
        left_join(weather_map, by = "component") %>%
        mutate(ext = case_when(
          component == "MANAGEMENT" ~ paste0(crop, "X"),
          component == "SOIL" ~ "SOL",
          grepl("WTH", component) ~ "WTH",
          component == "SUMMARY" ~ paste0(crop, "A"),
          component == "TIME_SERIES" ~ paste0(crop, "T")
        ),
        filename = case_when(
          component %in% c("MANAGEMENT", "SUMMARY", "TIME_SERIES") ~ paste(exp_code, ext, sep = "."),
          component == "SOIL" ~ paste(strict_abbreviate(sol_file_base, 2), ext, sep = "."),
          str_detect(component, "^WTH_") ~ paste(file_base, ext, sep = ".")
        )) %>%
        select(-file_base)
      signature <- paste0("Dataset processed on ", Sys.Date(), " with csmtools.")
      
      # Append signature to each component
      data_fmt <- components %>%
        imap(~{
          # .x is the dataframe, .y is its name (e.g., "SUMMARY")
          # Drop data if NULL (i.e., contains only structural variables)
          is_empty <- switch(
            .y,
            "SUMMARY" = is.null(.x) || (identical(names(.x), "TRNO") && ncol(.x) == 1),
            "TIME_SERIES" = is.null(.x) || (all(names(.x) %in% c("TRNO", "DATE")) && ncol(.x) <= 2),
            is.null(.x)
          )
          if (is_empty) {
            NULL
          } else {
            # Otherwise attach corresponding attributes
            attr(.x, "experiment") <- exp_metadata %>%
              filter(component == .y) %>%
              pull(exp_code)
            attr(.x, "file_name") <- exp_metadata %>%
              filter(component == .y) %>%
              pull(filename)
            attr(.x, "comments") <- c(attr(.x, "comments"), signature)
            .x
          }
        }) %>%
        compact()  # Remove NULL data components
      
      # Header data to attributes
      attr(data_fmt$MANAGEMENT$GENERAL, "exp_title") <- ls[["GENERAL"]]$EXP_NAME
      
      attr(data_fmt$SOIL, "title") <- soil_header$TITLE
      #attr(data_fmt$SOIL, "institute") <- soil_header$INST_NAME  # Note: should always be single row
      attr(data_fmt$SOIL, "comments") <- c(attr(data_fmt$SOIL, "comments"), unlist(soil_header$SL_METHOD_COMMENTS))
      
      weather_data <- data_fmt[grepl("WTH_DAILY", names(data_fmt))]
      # Drop YEAR variable
      weather_data <- lapply(weather_data, function(df) df %>% select(-YEAR))
      weather_header <- lapply(weather_header, function(df) df %>% select(-YEAR))
      
      weather_data <- mapply(function(x, y){
        attr(x, "GENERAL") <- y
        attr(x, "location") <- toupper(y$WST_SITE)
        x
      }, weather_data, weather_header, SIMPLIFY = FALSE)
      if (length(weather_data) == 1) {
        data_fmt[["WEATHER"]] <- weather_data[[1]]
      } else {
        data_fmt[["WEATHER"]] <- weather_data
      }
      data_fmt[grepl("WTH_", names(data_fmt))] <- NULL
      
      
      return(data_fmt)
    })
  }
  return(out_dataset)
}


# ------------------------------------------------------------------------------------------------------------------------

#' Split a Dataset by a Key Column
#'
#' @description
#' Splits a dataset (a list of data frames) into a nested list, where each sub-list corresponds to a unique value
#' of a specified key column.
#'
#' @details
#' This function is used to split a multi-experiment or multi-year dataset into individual subsets.
#' For tables that do not contain the `split_key`, the entire table is replicated across all subsets
#' (e.g., a single `SOIL` table would be copied into each experiment's subset). The function also cleans the output by
#' removing empty data frames and columns containing only `NA` values.
#'
#' @param dataset The dataset to split, as a list of data frames.
#' @param key The conceptual name of the key to split by (e.g., "experiment", "year"). This is resolved to a specific
#'    column name via the config file.
#' @param data_model The data model of the dataset.
#' @param config_path The path to the `datamodels.yaml` file.
#'
#' @return A named list of lists, where each top-level element is named after a unique value of the split key
#' 
#' @importFrom yaml read_yaml
#' 
#' @export
#'

split_dataset <- function(dataset, key, data_model,
                          config_path = "./inst/extdata/datamodels.yaml" # TODO: Replace eventually (fixed path)
) { 

  key_handlers <- c("experiment", "year", "plot", "treatment")
  unmatched_key <- match.arg(key, key_handlers)
  
  # Load split key from config
  config <- yaml::read_yaml(config_path)
  model_config <- config[[data_model]]
  split_key <- model_config$design_keys[[key]]
  
  # Guard clause if no keys are defined for the focal data model
  if (is.null(split_key)) {
    stop(paste0("Cannot split dataset: the key '", key, "' is not defined in the 'design_keys' for the '", 
                data_model, "' model."),
         call. = FALSE)
  }
  
  # ---- Splitting logic ----
  # 1. Find all unique levels of the split key across all tables that have it.
  all_levels <- unique(unlist(lapply(dataset, function(df) {
    if (split_key %in% names(df)) {
      return(unique(df[[split_key]]))
    }
    return(NULL)
  })))
  
  # Handle case where the key is not found anywhere
  if (is.null(all_levels) || length(all_levels) == 0) {
    warning(paste0("Split key '", split_key, "' not found in any data frame."),
            call. = FALSE)
    return(dataset) 
  }
  
  # 2. Initialize the nested output list, with names set to the unique levels.
  output_list <- stats::setNames(lapply(all_levels, function(x) list()), all_levels)
  
  # 3. Loop through each data frame in the original dataset.
  for (table_name in names(dataset)) {
    df <- dataset[[table_name]]
    
    # Case A: The split key EXISTS in this table.
    if (split_key %in% names(df)) {
      # Split the table normally.
      split_df_list <- split(df, f = df[[split_key]], drop = TRUE)
      
      # Add each resulting piece to the correct level in the output list.
      for (level_name in names(split_df_list)) {
        if(level_name %in% names(output_list)) {
          output_list[[level_name]][[table_name]] <- split_df_list[[level_name]]
        }
      }
      
      # Case B: The split key IS MISSING from this table.
    } else {
      # Inform the user about the replication.
      message(paste0("'", split_key, "' not found in '", table_name, "'. Replicating this table across all subsets."))
      
      # Assign the *entire* table to every single subset.
      for (level_name in as.character(all_levels)) {
        output_list[[level_name]][[table_name]] <- df
      }
    }
  }
  
  # ---- Clean-up output
  # Remove empty data frames, e.g., management categories irrelevant for the focal year
  output_list <- lapply(output_list, function(ls) ls[lengths(ls) > 0])
  
  # Remove NA-only columns
  output_list <- lapply(output_list, function(ls){
    ls <- lapply(ls, function(df) {
      df[, colSums(is.na(df)) != nrow(df)]
    })
  })
  
  # Special case: measured data with only structural columns
  # TODO: set with config instead of hard-coding
  output_list <- lapply(output_list, function(ls){
    if (all(colnames(ls[["SUMMARY"]]) %in% c("TRTNO","EXP_YEAR","DATE"))) {
      ls[["SUMMARY"]] <- NULL
    }
    if (all(colnames(ls[["TIME_SERIES"]]) %in% c("TRTNO","EXP_YEAR","DATE"))) {
      ls[["TIME_SERIES"]] <- NULL
    }
    return(ls)
  })
  
  # Reset management IDs fun??
  
  return(output_list)
}



#' Generate an Experimental Design Code from a DSSAT management table list
#'
#' Constructs a compact code describing the experimental design, based on treatment and fertilizer structure, using ICASA abbreviations.
#'
#' @param dataset A named list of data frames, including at least \code{"TREATMENTS"} and \code{"FERTILIZERS"}.
#'
#' @details
#' The function maps treatment headers to ICASA abbreviations, removes managements with only one level, and groups columns by their sequence of levels. It then constructs a code summarizing the experimental design, indicating the number of levels and the management types involved. If fertilization is a treatment, the code includes the focal nutrient(s) (e.g., "FE(N)" for nitrogen).
#'
#' This code is useful for summarizing and comparing experimental designs in a standardized way.
#'
#' The function uses \code{map_headers} and \code{load_map} for header mapping.
#'
#' @return A character string representing the experimental design code.
#'
#' @examples
#' \dontrun{
#' code <- make_expd_code(dataset)
#' print(code)
#' }
#'
# 
# make_expd_code <- function(dataset) {
#   
#   trt <- dataset[["TREATMENTS"]]
#   fert <- dataset[["FERTILIZERS"]]
#   
#   # Map to ICASA (experimental design code use ICASA abbreviation for management types)
#   trt_icasa <- map_headers(trt, map = load_map(), direction = "to_icasa")$data
#   # TODO: move to ICASA metadata structure formatting
#   
#   matrix <- trt_icasa[, nchar(names(trt_icasa)) == 2]
#   
#   # Remove blanket managements (i.e., single-level treatments)
#   unique_counts <- sapply(matrix, function(x) length(unique(x)))
#   matrix <- matrix[, unique_counts > 1, drop = FALSE]  # Keep only columns with more than one level
#   
#   # Split headers per sequence of levels
#   seq <- apply(matrix, 2, paste, collapse = ".")
#   seq_mapping <- data.frame(trt = names(matrix), grp = seq)
#   seq_grouped <- split(seq_mapping, f = seq_mapping$grp)  # Group columns by unique sequences
#   
#   # Format output
#   levels_no <- sapply(names(seq_grouped), function(x) length(unique(strsplit(x, "\\.")[[1]])))
#   tmp <- sapply(seq_grouped, function(df) paste(df$trt, collapse = "/"))
#   tmp1 <- mapply(paste0, levels_no, tmp)
#   exp_design <- paste(tmp1, collapse = "*")
#   
#   # Add the focal nutrient(s) if fertilization is a treatment
#   if(grepl("FE", exp_design)){
#     fert <- apply(fert[,startsWith(colnames(fert),"FAM")], 2, function(x) length(unique(x)))
#     elem <- names(fert[fert > 1])
#     elem <- paste0(gsub("FAM", "", elem), collapse = "")
#     exp_design <- gsub("FE", paste0("FE(", elem, ")"), exp_design)
#   }
#   
#   return(exp_design)
# }

# FILL TRT MATRIX 0 WITH NAS --> MOVE TO FORMAT MNGT
# filex %>%  
#   mutate(across(where(is.numeric), ~ifelse(is.na(.x), 0, .x)))  # exception is simulation controls
