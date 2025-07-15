#' Validate DSSAT Entity names by Type
#'
#' Checks whether a code string is valid for a given DSSAT entity name (experiment, cultivar, field, soil, or weather station).
#'
#' @param x Character vector. The code(s) to validate.
#' @param item Character. The type of entity. One of \code{"experiment"}, \code{"cultivar"}, \code{"field"}, \code{"soil"}, or \code{"weather_station"}.
#' @param framework Character. The modeling framework (default: \code{"dssat"}). Currently only DSSAT is supported.
#'
#' @details
#' The function uses regular expressions to check if the input code(s) match the expected format for the specified entity type:
#' \itemize{
#'   \item \code{experiment}: 4 uppercase letters followed by 4 digits (e.g., "EXPT2023")
#'   \item \code{cultivar}: 2 uppercase letters followed by 4 digits (e.g., "CU1234")
#'   \item \code{field}: 4 uppercase letters followed by 4 digits
#'   \item \code{soil}: 4 uppercase letters and 6 digits, or 2 uppercase letters and 8 digits
#'   \item \code{weather_station}: 4 uppercase letters
#' }
#'
#' @return Logical vector. \code{TRUE} if the code is valid for the specified type, \code{FALSE} otherwise.
#'
#' @examples
#' isValidCode("EXPT2023", "experiment")        # TRUE
#' isValidCode("CU1234", "cultivar")            # TRUE
#' isValidCode("SOIL000001", "soil")            # TRUE
#' isValidCode("WXST", "weather_station")       # TRUE
#' isValidCode("BADCODE", "experiment")         # FALSE
#'

isValidCode <- function(x,
                        item = c("experiment", "cultivar", "field", "soil", "weather_station"),
                        framework = "dssat"){
  
  isValid <- switch(item,
                    "experiment" = grepl("^[A-Z]{4}[0-9]{4}$", x),
                    "cultivar" = grepl("^[A-Z]{2}[0-9]{4}$", x),
                    "field" = grepl("^[A-Z]{4}[0-9]{4}$", x),
                    "soil" = grepl("(^[A-Z]{4}[0-9]{6}$)|(^[A-Z]{2}[0-9]{8}$)", x),
                    "weather_station" = grepl("^[A-Z]{4}$", x)
  )
  
  return(isValid)
}


#' Format metadata documentation for ICASA/DSSAT datasets into the target data model
#' 
#' This function documents metadata (provenance, spatial and temporal coverage) and naming
#' conventions in compliance with a speicifed (meta)data model
#' 
#' 
#' @param dataset An ICASA/DSSAT dataset as a list of named data frames with unstructured metadata
#' @param data_model the target data model (ICASA or DSSAT)
#' @param section !deprecated, left from old version
#' 
#' @return An ICASA/DSSAT dataset as a list of named data frames with metadata structured
#' to the target data model
#' 
#' @importFrom magrittr %>%
#' @importFrom dplyr mutate left_join full_join group_by summarise pull select
#' @importFrom lubridate year month
#' @importFrom tidygeocoder reverse_geocode
#' @importFrom countrycode countrycode
#' @importFrom elevatr get_elev_point
#'
#' @export  ##! keep or embed into the mapping wrapper?
#'

# TODO: include weather/soil data processing / naming checks
structure_metadata <- function(dataset, data_model = c("icasa", "dssat")) {
  
  if (data_model == "icasa"){
    
    # Drop empty tables
    dataset <- Filter(function(x) nrow(x) > 0, dataset)
    
    # Identify core and provenance metadata tables
    metadata <- dataset[grepl("METADATA", names(dataset))]
    prov_meta <- dataset[grepl("PERSONS|INSTITUTIONS|DOCUMENTS", names(dataset))]
    
    # Remove template drop down list helpers (institute names appended to IDs)
    prov_meta[["PERSONS"]] <- prov_meta[["PERSONS"]] %>%
      mutate(institute_ID = sub("\\|.*", "", institute_ID)) %>%
      mutate(institute_ID = as.numeric(institute_ID))
    
    # Merge core metadata with provenance attributes
    metadata <- lapply(metadata, function(df){
      
      join_nm <- names(df)[1]  # Get the first column name dynamically
      
      prov_meta <- lapply(prov_meta, function(x){
        colnames(x)[1] <- join_nm
        return(x)
      })
      
      # Merge provenance attributes
      prov_merged <- Reduce(function(x, y) full_join(x, y, by = base::intersect(names(x), names(y))), prov_meta)
      # Append to metadata table
      df <- Reduce(function(x, y) left_join(x, y, by = join_nm), list(df, prov_merged))
    })
    
    # Rename merged provenance attributes in compliance with ICASA
    add_header_suffix <- function(df, suffix) {
      colnames(df) <- ifelse(grepl("^(digital|document)", colnames(df)), 
                             paste0(colnames(df), "_", suffix), 
                             colnames(df))
      return(df)
    }
    
    if (!is.null(metadata[["WEATHER_METADATA"]])) {
      dataset[["WEATHER_METADATA"]] <- add_header_suffix(metadata[["WEATHER_METADATA"]], "wst")
    }
    if (!is.null(metadata[["SOIL_METADATA"]])) {
      dataset[["SOIL_METADATA"]] <- add_header_suffix(metadata[["SOIL_METADATA"]], "sl")
    }
    dataset[["EXP_METADATA"]] <- metadata[["EXP_METADATA"]]
    
    out <- dataset[base::setdiff(names(dataset), names(prov_meta))]
  }
  
  else if (data_model == "dssat"){
    ## TODO: ADD ICASA METADATA / OTHER DATA SECTIONS

    #dataset <- fdata  #tmp
    
    ###------- SECTION MANAGEMENT ---------------------------------------------
    
    # Metadata sections
    prov_meta <- dataset[["GENERAL"]]  # Provenance
    loc_meta <- dataset[["FIELDS"]]  # Location
    cult_meta <- dataset[["CULTIVARS"]]  # Cultivar
    ipt_comments <- attr(dataset, "comments")  # Comments
    
    # Format provenance metadata ----------------------------------------------
    
    #prov_meta$LAST_NAME <- paste0(prov_meta$LAST_NAME, ",")
    prov_people <- mapply(function(...) paste(na.omit(c(...)), collapse = " "),
                          prov_meta$LAST_NAME, prov_meta$FIRST_NAME, prov_meta$MID_INITIAL)
    
    
    # Format location metadata ------------------------------------------------
    
    # Retrieve field coordinates (xyz)
    # TODO: data quality checks to handle cases where critical metadata is unavailable
    loc_coords <- data.frame(x = mean(loc_meta$XCRD), y = mean(loc_meta$YCRD)) # Retrieve experiment coordinates
    loc_elev <- ifelse(is.na(loc_meta$ELEV),
                       suppressMessages({get_elev_point(loc_coords, prj = 4326, src = "aws")})$elevation,
                       loc_meta$ELEV)
    
    # Retrieve location credentials using reverse geocoding if not in metadata
    if (is.na(loc_meta$FLL1) || is.na(loc_meta$FLL3)){
      addr <- reverse_geocode(loc_coords, long = x, lat = y, method = 'arcgis', full_results = TRUE, quiet = TRUE)
    }
    loc_inst <- prov_meta$INSTITUTION[1]  # pick the first one when more than one
    # TODO: map institution role, pick lead OR first (if no lead or data missing)
    loc_field <- ifelse(is.na(loc_meta$FLNAME), "Unnamed site", loc_meta$FLNAME)
    loc_city <- ifelse(is.na(loc_meta$FLL3), addr$City, loc_meta$FLL3)  # translate country?
    loc_region <- loc_meta$FLL2
    loc_country <- ifelse(is.na(loc_meta$FLL1), addr$CntryName, loc_meta$FLL1)
    
    # Concatenate address and site location in single strings
    loc_addr <- paste(na.omit(c(loc_inst, loc_city, loc_region, loc_country)), collapse = ", ")
    loc_site <- paste0(loc_field, ", ", loc_addr, "; ", loc_coords$x, "; ", loc_coords$y, "; ", loc_elev)
    
    
    # Other attributes --------------------------------------------------------
    
    # Retrieve experiment year based on harvest date
    ### Wont work when ongoing experiment, what to do then? Model knows but crop specific
    ### DSSAT default is "at maturity" -> find in DSSAT source code how maturity is fixed for different crops and use it when data missing
    exp_year <- year(dataset$HARVEST[nrow(dataset$HARVEST),]$HDATE)
    
    # Retrieve crop name in cultivar section
    crop <- unique(cult_meta$CR)
    
    
    # Compile metadata --------------------------------------------------------
    
    # Provenance metadata
    prov_meta$ADDRESS <- loc_addr
    prov_meta$PEOPLE <- prov_people
    prov_meta$SITE <- loc_site
    prov_meta <- prov_meta %>% select(-FIRST_NAME, -LAST_NAME, -MID_INITIAL)  #tmp -INSTITUTION?
    prov_meta <- concatenate_per_group(prov_meta)
    
    # Spatial metadata
    loc_meta$FLNAME <- toupper(loc_field)
    loc_meta$FLL1 <- toupper(loc_country)
    loc_meta$FLL2 <- toupper(loc_region)
    loc_meta$FLL3 <- toupper(loc_city)
    loc_meta$ELEV <- loc_elev
    
    
    # Generate identifier based on DSSAT naming conventions -------------------
    
    ## Experiment name --------------------------------------------------------
    
    experiment_code <- c()
    
    for (i in 1:nrow(prov_meta)) {
      
      experiment_code[i] <-
        if (!isValidCode(prov_meta[i,]$EXP_ID, "experiment", "dssat")){
          toupper(
            paste0(
              strict_abbreviate(prov_meta$INSTITUTION, 2),
              substr(exp_year, 3, 4),
              "01"  # TODO: figure out how to find experiment ID
            )
          )
        } else {
          prov_meta[i,]$EXP_ID
        }
    }
    # Add unique() to account for duplicate generated when 
    experiment_code <- unique(experiment_code)
    
    file_ext <- setNames(paste0(crop, c("X", "A", "T")), c("X", "A", "T"))
    xfilename <- paste0(experiment_code,  ".", file_ext["X"])
    afilename <- paste0(experiment_code,  ".", file_ext["A"])
    tfilename <- paste0(experiment_code,  ".", file_ext["T"])
    
    ## Experiment title -------------------------------------------------------
    
    exp_design <- make_expd_code(dataset)
    
    # Concatenate standard experiment title
    exp_details <- toupper(
      paste0(prov_meta$EXP_NAME, " ", experiment_code, crop, ", ", loc_city, ", ", loc_country, "  ", exp_design)
    )
    
    ## Field identifier -------------------------------------------------------
    
    field_codes <- c()
    
    for (i in 1:nrow(loc_meta)) {
      
      field_codes[i] <-
        if (!isValidCode(loc_meta[i,]$ID_FIELD, "field", "dssat")){
          toupper(
            paste0(
              strict_abbreviate(prov_meta$INSTITUTION, 2),
              strict_abbreviate(loc_city, 2),
              sprintf("%04d", loc_meta[i,]$L)
            )
          )
        } else {
          loc_meta[i,]$ID_FIELD
        }
    }
    
    ## Cultivar identifier ----------------------------------------------------
    
    cultivar_codes <- c()
    
    for (i in 1:nrow(cult_meta)) {
      
      cultivar_codes[i] <-
        if (!isValidCode(cult_meta[i,]$INGENO, "cultivar", "dssat")){
          toupper(
            paste0(
              strict_abbreviate(loc_inst, 2),
              sprintf("%04d", cult_meta[i,]$C)
            )
          )
        } else {
          cult_meta[i,]$INGENO
        }
    }
    
    ###------- SOIL metadata + standard identifiers ---------------------------
    
    structure_metadata_sol <- function(data, data_model = "dssat") {
      
      # Apply DSSAT format
      #data <- dataset
      #data <- lapply(data, function(df) apply_format(df, template = SOIL_template))
      
      # Get formatted ICASA data
      sol_meta <- data[["SOIL_METADATA"]]
      profile_data <- data[["SOIL_PROFILE_LAYERS"]]
      ipt_comments <- attr(data, "comments")
      
      # Combine all sections and collapse profile data
      if (!is.null(sol_meta) && !is.null(profile_data)) {
        # Merge metadata and layer data
        sol_data <- cbind(sol_meta, profile_data)
      } else {
        # Determine missing columns based on available data
        available_data <- sol_meta %||% profile_data
        missing_cols <- setdiff(
          union(names(available_data), names(SOIL_template)),
          intersect(names(available_data), names(SOIL_template))
        )
        sol_data <- cbind(available_data, SOIL_template[, names(SOIL_template) %in% missing_cols])
        sol_data <- sol_data[, colnames(SOIL_template)]
      }
      
      # Stop if location metadata is missing
      if (is.null(sol_meta) || is.na(sol_meta$LAT) || is.na(sol_meta$LONG)){
        
        out <- sol_data
        
        warning("Location metadata not found for soil profile.")
        return(out)
      }
      
      # Initiate vectors to loop metadata standardization across profiles
      sol_coords <- data.frame(x = numeric(), y = numeric())
      sol_country <- sol_year <- sol_max_depth <- c()
      out <- list()
      unique_pedons <- unique(sol_data$PEDON)
      
      for (pedon in unique_pedons) {
        
        # Subset only the rows that match the current PEDON
        idx <- sol_data$PEDON == pedon
        
        # Standardize location data (only compute once per PEDON)
        sol_coords_subset <- data.frame(x = sol_data[idx,]$LONG, y = sol_data[idx,]$LAT)
        sol_addr <- reverse_geocode(sol_coords_subset[1,], long = x, lat = y, method = 'arcgis', full_results = TRUE, quiet = TRUE)
        
        sol_country <- ifelse(is.na(sol_data[idx, "SITE"]), sol_addr$CountryCode, sol_data[idx, "SITE"])
        sol_data[idx, "COUNTRY"] <- countrycode(sol_country, origin = "iso3c", destination = "iso2c")
        sol_data[idx, "SITE"] <- ifelse(is.na(sol_data[idx, "SITE"]), sol_addr$City, sol_data[idx, "SITE"])
        
        if (!is.null(sol_data[idx, "INST_NAME"])) {
          sol_data[idx, "INST_NAME"] <- ifelse(is.na(sol_data[idx, "INST_NAME"]), loc_inst, sol_data[idx, "INST_NAME"])
        } else {
          sol_data[idx, "INST_NAME"] <- NA_character_
        }
        
        # Get soil profiling year
        if (!is.null(sol_data[idx, "DATE"])) {
          sol_data[idx, "YEAR"] <- ifelse(is.na(sol_data[idx, "DATE"]), exp_year, year(sol_data[idx, "DATE"]))
        } else {
          sol_data[idx, "YEAR"] <- 0
        }
       
        
        # Apply standard naming
        sol_data[idx, "PEDON_old"] <- sol_data[idx, "PEDON"]
        sol_data[idx, "PEDON"] <- if (!isValidCode(sol_data[idx, "PEDON"][1], "soil", "dssat")) {
          toupper(
            paste0(
              strict_abbreviate(sol_data[idx, "INST_NAME"][1], 2),
              strict_abbreviate(sol_data[idx, "SITE"][1], 2),
              substr(sol_data[idx, "YEAR"][1], 3, 4),
              sprintf("%04d", sum(sol_data$INST_NAME[idx] == sol_data[idx, "INST_NAME"][1] & 
                                    sol_data$SITE[idx] == sol_data[idx, "SITE"][1] & 
                                    sol_data$YEAR[idx] == sol_data[idx, "YEAR"][1]))
            )
          )
        } else {
          sol_data[idx, "PEDON"]
        }
        
        # Set soil depth based on profile
        sol_data[idx, "DEPTH"] <- ifelse(is.na(sol_data[idx, "DEPTH"]),
                                         max(unlist(sol_data[idx, "SLB"])),
                                         sol_data[idx, "DEPTH"])
        
        # Set soil texture based on profile
        # TODO: function to estimate texture based on profile layers data; need to include sand?
      }
      
      # Organize elements as DSSAT output
      sol_inst <- unique(sol_data$INST_NAME)[1]  # TODO: handle multiple institutes: multiple files?
      sol_site <- unique(sol_data$SITE)[1]
      out <- sol_data %>% select(-YEAR, -INST_NAME)
      
      # For SOILGRIDS data set inst to IS or SG
      if (any(grepl("ISRIC", ipt_comments))) {
        attr(out, "title") <- paste("Soil profile data from ISRIC soilgrids + SLOC from HC27")
        attr(out, "file_name") <- "IS.SOL"
      } else {
        attr(out, "title") <- paste(sol_inst, sol_site, sep = ", ")
        attr(out, "file_name") <- paste0(strict_abbreviate(toupper(sol_inst)), ".SOL")
      }
      
      attr(out, "comments") <- if (!is.null(ipt_comments)) {
        list(ipt_comments, paste0("Dataset generated with csmTools on ", Sys.Date()))
      } else {
        list(paste0("Dataset generated with csmTools on ", Sys.Date()))
      }
      
      return(out)
    }
    sol <- structure_metadata_sol(dataset)
    
    
    ###------- WEATHER metadata + standard identifiers ------------------------
    
    structure_metadata_wth <- function(data, data_model = "dssat") {
      
      # TODO: APPLY DSSAT FORMAT?
      # TODO: handle multiple weather stations (move number 2 to comment...; use soil fun as model)
      
      # Get formatted ICASA data
      wsta_meta <- data[["WEATHER_METADATA"]]
      wsta_data <- data[["WEATHER_DAILY"]]
      ipt_comments <- attr(data, "comments")
      
      # Assemble metadata table
      out_metadata <-
        data.frame(INSI = NA_character_,
                   LAT = NA_real_,
                   LONG = NA_real_,
                   ELEV = NA_real_,
                   TAV = wsta_data %>%
                     summarise(TAV = mean((TMAX + TMIN)/2, na.rm = TRUE)) %>%
                     pull(TAV),
                   AMP = wsta_data %>%
                     mutate(mo = month(DATE)) %>%
                     group_by(mo) %>%
                     summarise(mo_TAV = mean((TMAX + TMIN)/2, na.rm = TRUE)) %>%
                     summarise(AMP = (max(mo_TAV)-min(mo_TAV))/2) %>%
                     pull(AMP),
                   REFHT = NA_real_,  # document in device/sensor metadata? Would need text mining workflow
                   WNDHT = NA_real_  # document in device/sensor metadata?
        )
      
      # Stop if metadata not found
      if (is.null(wsta_meta) || is.na(wsta_meta$LAT) || is.na(wsta_meta$LONG)) {
        
        out <- wsta_data
        attr(out, "station_metadata") <- out_metadata
        
        warning("Metadata not found for weather data.")
        return(out)
      }
      
      # Standardize location data
      wth_coords <- data.frame(x = mean(wsta_meta$LONG), y = mean(wsta_meta$LAT))
      wth_elev <- suppressMessages(
        get_elev_point(wth_coords, prj = 4326, src = "aws")
      )
      wth_addr <- reverse_geocode(wth_coords, long = x, lat = y, method = 'arcgis', full_results = TRUE, quiet = TRUE)  # find location name
      wth_country <- wth_addr$CntryName
      wth_state <- wth_addr$Region
      wth_city <- wth_addr$City
      wth_inst <- ifelse(is.na(wsta_meta$INST_NAME), loc_inst, wsta_meta$INST_NAME)
      wth_loc <- ifelse(is.na(wsta_meta$WST_NAME), wth_city, wsta_meta$WST_NAME)
      
      # Retrieve year
      wth_year <- year(wsta_data$DATE[1])
      
      # Assemble metadata table
      out_metadata$INSI <- wsta_meta$INSI
      out_metadata$LAT <- wth_coords$y
      out_metadata$LONG <- wth_coords$x
      out_metadata$ELEV <- wth_elev$elevation
      out_metadata$REFHT <- wsta_meta$REFHT
      out_metadata$WNDHT <- wsta_meta$WNDHT

      # Apply standard naming
      for (i in 1:nrow(out_metadata)) {
        
        out_metadata[i, "INSI_old"] <- out_metadata[i, "INSI"]
        out_metadata[i, "INSI"] <-
          if (!isValidCode(out_metadata[i, "INSI"], "weather_station", "dssat")){
            toupper(
              paste0(
                strict_abbreviate(wth_inst, 2),  # weather institution?
                strict_abbreviate(wth_loc, 2)
              )
            )
          } else {
            out_metadata[i, "INSI"]
          }
      }
      
      # Organize elements as DSSAT output
      out <- wsta_data
      attr(out, "station_metadata") <- out_metadata
      attr(out, "location") <- toupper(
        paste(wth_loc, wth_state, wth_country, sep = ", ")  # weather inst?
      )
      # TODO: add sensor metadata as comments
      attr(out, "file_name") <- paste0(out_metadata$INSI, substr(wth_year, 3, 4), "01", ".WTH")
      attr(out, "comments") <- if (!is.null(ipt_comments)) {
        list(ipt_comments, paste0("Dataset generated with csmTools on ", Sys.Date()))
      } else {
        list(paste0("Dataset generated with csmTools on ", Sys.Date()))
      }

      return(out)
    }
    wth <- structure_metadata_wth(dataset)


    # Assign changes to data tables -----------------------------------------------
    
    # Cultivar identifiers
    cult_meta$INGENO <- cultivar_codes
    # Field identifier(s)
    loc_meta$ID_FIELD <- field_codes
    # Soil profiles identifier(s)
    sol_lookup <- setNames(sol$PEDON, sol$PEDON_old)
    loc_meta$ID_SOIL <- sol_lookup[loc_meta$ID_SOIL]
    # Weather "station" identifier(s)
    wth_lookup <- setNames(attr(wth, "station_metadata")$INSI, attr(wth, "station_metadata")$INSI_old)
    loc_meta$WSTA <- wth_lookup[loc_meta$WSTA]
    
    # Apply formatting to management files
    management <- dataset[!grepl("WEATHER|SOIL|SUMMARY|TIME_SERIES", names(dataset))]
    management[["GENERAL"]] <- prov_meta
    management[["CULTIVARS"]] <- cult_meta
    management[["FIELDS"]] <- loc_meta
    
    attr(management, "experiment") <- exp_details
    attr(management, "file_name") <- xfilename
    attr(management, "comments") <- if (!is.null(ipt_comments)) {
      list(ipt_comments, paste0("Dataset generated with csmTools on ", Sys.Date()))
    } else {
      list(paste0("Dataset generated with csmTools on ", Sys.Date()))
    }
    
    # Apply formatting to observed data - summary
    obs_a <- dataset[["SUMMARY"]]
    # Check if only str columns are present (i.e., cases when measured ICASA vars have no DSSAT equivalent)
    if (identical(names(obs_a), "TRNO") && ncol(obs_a) == 1) obs_a <- NULL
    
    if (!is.null(obs_a)) {
      attr(obs_a, "experiment") <- exp_details
      attr(obs_a, "file_name") <- afilename  # TODO: drop if only str vars
      attr(obs_a, "comments") <- if (!is.null(ipt_comments)) {
        list(ipt_comments, paste0("Dataset generated with csmTools on ", Sys.Date()))
      } else {
        list(paste0("Dataset generated with csmTools on ", Sys.Date()))
      }
    }
    
    # Apply formatting to observed data - time-series
    obs_t <- dataset[["TIME_SERIES"]]
    # Check if only str columns are present (i.e., cases when measured ICASA vars have no DSSAT equivalent)
    if (all(names(obs_t) %in% c("TRNO", "DATE")) && length(names(obs_t)) <= 2) obs_t <- NULL
    
    if (!is.null(obs_t)) {  
      attr(obs_t, "experiment") <- exp_details
      attr(obs_t, "file_name") <- tfilename  # TODO: drop if only str vars
      attr(obs_t, "comments") <- if (!is.null(ipt_comments)) {
        list(ipt_comments, paste0("Dataset generated with csmTools on ", Sys.Date()))
      } else {
        list(paste0("Dataset generated with csmTools on ", Sys.Date()))
      }
    }
    
    # Drop old IDs
    #attr(wth, "station_metadata")$INSI_old <- NULL
    #sol <- sol %>% select(-PEDON_old, -DATE)
    
    out <- list(MANAGEMENT = management,
                SOIL = sol,
                WEATHER = wth,
                SUMMARY = obs_a,
                TIME_SERIES = obs_t)
  }
  
  return(out)
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

make_expd_code <- function(dataset) {
  
  trt <- dataset[["TREATMENTS"]]
  fert <- dataset[["FERTILIZERS"]]
  
  # Map to ICASA (experimental design code use ICASA abbreviation for management types)
  trt_icasa <- map_headers(trt, map = load_map(), direction = "to_icasa")$data
  
  matrix <- trt_icasa[, nchar(names(trt_icasa)) == 2]
  
  # Remove blanket managements (i.e., single-level treatments)
  unique_counts <- sapply(matrix, function(x) length(unique(x)))
  matrix <- matrix[, unique_counts > 1, drop = FALSE]  # Keep only columns with more than one level
  
  # Split headers per sequence of levels
  seq <- apply(matrix, 2, paste, collapse = ".")
  seq_mapping <- data.frame(trt = names(matrix), grp = seq)
  seq_grouped <- split(seq_mapping, f = seq_mapping$grp)  # Group columns by unique sequences
  
  # Format output
  levels_no <- sapply(names(seq_grouped), function(x) length(unique(strsplit(x, "\\.")[[1]])))
  tmp <- sapply(seq_grouped, function(df) paste(df$trt, collapse = "/"))
  tmp1 <- mapply(paste0, levels_no, tmp)
  exp_design <- paste(tmp1, collapse = "*")
  
  # Add the focal nutrient(s) if fertilization is a treatment
  if(grepl("FE", exp_design)){
    fert <- apply(fert[,startsWith(colnames(fert),"FAM")], 2, function(x) length(unique(x)))
    elem <- names(fert[fert > 1])
    elem <- paste0(gsub("FAM", "", elem), collapse = "")
    exp_design <- gsub("FE", paste0("FE(", elem, ")"), exp_design)
  }
  
  return(exp_design)
}
