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
#' @importFrom dplyr mutate left_join full_join group_by summarise pull select
#' @importFrom lubridate year month
#' @importFrom tidygeocoder reverse_geocode
#' @importFrom elevatr get_elev_point
#'
#' @export  ##! keep or embed into the mapping wrapper?
#'

# TODO: include weather/soil data processing / naming checks
format_metadata <- function(dataset, data_model = c("icasa", "dssat"),
                            section = c("general", "experiment", "management", "soil", "weather")){   #tmp, delete eventually
  
  if (data_model == "icasa"){
    
    metadata <- dataset[c("EXP_METADATA","SOIL_METADATA","WEATHER_METADATA")]  # Core metadata
    prov_meta <- dataset[c("PERSONS","INSTITUTIONS","DOCUMENTS")]  # Provenance attributes
    
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
      
      prov_merged <- Reduce(function(x, y) full_join(x, y, by = base::intersect(names(x), names(y))), prov_meta)  # Merge provenance attributes
      df <- Reduce(function(x, y) left_join(x, y, by = join_nm), list(df, prov_merged))  # Append to metadata table
    })
    
    # Rename merged provenance attributes in compliance with ICASA
    add_header_suffix <- function(df, suffix) {
      colnames(df) <- ifelse(grepl("^(digital|document)", colnames(df)), 
                             paste0(colnames(df), "_", suffix), 
                             colnames(df))
      return(df)
    }
    dataset[["WEATHER_METADATA"]] <- add_header_suffix(metadata[["WEATHER_METADATA"]], "wst")
    dataset[["SOIL_METADATA"]]  <- add_header_suffix(metadata[["SOIL_METADATA"]], "sl")
    dataset[["EXP_METADATA"]] <- metadata[["EXP_METADATA"]]
    
    dataset <- dataset[base::setdiff(names(dataset), names(prov))]
  }
  
  else if (data_model == "dssat"){
    if (section == "weather"){
      
      metadata <- attr(data, "metadata")
      if (is.null(metadata)){
        message("Metadata table not found.")
        return(data)
      }
      
      coords <- data.frame(x = mean(metadata$LONG), y = mean(metadata$LAT))  # retrieve coordinates
      addr <- reverse_geocode(coords, long = x, lat = y, method = 'osm', full_results = TRUE, quiet = TRUE)  # find location name
      year <- year(data$DATE[1])  # retrieve year
      wst_id <- toupper(paste0(addr$country_code, abbreviate(addr$town, minlength = 2L)))  # make weather station id (DSSAT format)
      suppressMessages({elev <- get_elev_point(coords, prj = 4326, src = "aws")})
      
      attr(data, "station_metadata") <-
        data.frame(INSI = wst_id,
                   LAT = coords$y,
                   LONG = coords$x,
                   ELEV = elev$elevation,
                   TAV = data %>%
                     summarise(TAV = mean((TMAX + TMIN)/2, na.rm = TRUE)) %>%
                     pull(TAV),
                   AMP = data %>%
                     mutate(mo = month(DATE)) %>%
                     group_by(mo) %>%
                     summarise(mo_TAV = mean((TMAX + TMIN)/2, na.rm = TRUE)) %>%
                     summarise(AMP = (max(mo_TAV)-min(mo_TAV))/2) %>%
                     pull(AMP),
                   REFHT = 2,  # document in device/sensor metadata? Would need text mining workflow
                   WNDHT = NA_real_  # document in device/sensor metadata?
        )
      
      # Top elements
      attr(data, "location") <- toupper(paste(addr$town, addr$state, addr$country, sep = ", "))  # title
      attr(data, "comments") <- list("Sensor metadata: ###", paste0("Dataset generated with csmTools on ", Sys.Date())) ### add sensor/ppty metadata
      attr(data, "filename") <- paste0(wst_id, substr(year, 3, 4), "01", ".WTH")  # file name
      attributes(data)$metadata <- NULL  # remove old format
    }
    ## TODO: ADD ICASA METADATA / OTHER DATA SECTIONS
    if (section == "management"){
      
      #dataset <- uc6_dssat  #tmp
      
      # Format provenance metadata ----------------------------------------------
      
      prov_meta <- dataset$GENERAL
      prov_people <- paste(na.omit(c(prov_meta$LAST_NAME, prov_meta$FIRST_NAME, prov_meta$MID_INITIAL)), collapse = " ")
      
      
      # Format location metadata ------------------------------------------------
      
      loc_meta <- dataset$FIELDS
      # Retrieve field coordinates (xyz)
      # TODO: data quality checks to handle cases where critical metadata is unavailable
      loc_coords <- data.frame(x = mean(loc_meta$XCRD), y = mean(loc_meta$YCRD)) # Retrieve experiment coordinates
      loc_elev <- ifelse(is.na(loc_meta$ELEV),
                         suppressMessages({get_elev_point(coords, prj = 4326, src = "aws")})$elevation,
                         loc_meta$ELEV)
      
      # Retrieve location credentials using reverse geocoding if not in metadata
      if (is.na(loc_meta$FLL1) || is.na(loc_meta$FLL3)){
        addr <- reverse_geocode(loc_coords, long = x, lat = y, method = 'arcgis', full_results = TRUE, quiet = TRUE)
      }
      loc_inst <- prov_meta$INSTITUTION
      loc_field <- ifelse(is.na(loc_meta$FLNAME), "Unnamed site", loc_meta$FLNAME)
      loc_city <- ifelse(is.na(loc_meta$FLL3), addr$City, loc_meta$FLL3)  # translate country?
      loc_region <- loc_meta$FLL2
      loc_country <- ifelse(is.na(loc_meta$FLL1), addr$CntryName, loc_meta$FLL1)
      
      # Concatenate address and site location in single strings
      loc_addr <- paste(na.omit(c(loc_inst, loc_city, loc_region, loc_country)), collapse = ", ")
      loc_site <- paste0(loc_site, ", ", loc_addr, "; ", coords$x, "; ", coords$y, "; ", loc_elev)
      
      
      # Other attributes --------------------------------------------------------
      
      # Retrieve experiment year based on harvest date
      ### Wont work when ongoing experiment, what to do then? Model knows but crop specific
      ### DSSAT default is "at maturity" -> find in DSSAT source code how maturity is fixed for different crops and use it when data missing
      year <- year(dataset$HARVEST[nrow(dataset$HARVEST),]$HDATE)
      
      # Retrieve crop name in cultivar section
      crop <- dataset$CULTIVARS$CR
      
      
      # Compile metadata --------------------------------------------------------
      
      # Provenance metadata
      prov_meta$ADDRESS <- loc_addr
      prov_meta$PEOPLE <- prov_people
      prov_meta$SITE <- loc_site
      prov_meta <- concatenate_per_group(prov_meta)
      
      # Spatial metadata
      loc_meta$FLNAME <- toupper(loc_field)
      loc_meta$FLL1 <- toupper(loc_country)
      loc_meta$FLL2 <- toupper(loc_region)
      loc_meta$FLL3 <- toupper(loc_city)
      loc_meta$ELEV <- loc_elev
      
      
      # Generate standard DSSAT identifiers -------------------------------------
      
      # TODO: keep experiment ID in general metadata and check
      check_naming <- function(dataset, convention = "dssat"){
        ###
      }
      
      apply_standard_naming <- function(...){
        
        #dataset <- uc6_dssat  #tmp
        
        # Standardize experiment name ---------------------------------------------
        
        ### add the checkname here
        file_ext <- setNames(paste0(crop, c("X", "A", "T")), c("X", "A", "T"))
        
        filename <- toupper(
          paste0(
            strict_abbreviate(prov_meta$INSTITUTION),
            strict_abbreviate(loc_city),
            substr(year, 3, 4),
            "01",  # TODO: figure out how to find experiment ID
            ".",
            file_ext["X"]
          )
        )
        
        # Standardize experiment title --------------------------------------------
        
        treatments <- dataset$TREATMENTS
        # Map to ICASA (experimental design code use ICASA abbreviation for management types)
        trt_icasa <- map_headers(treatments, map = load_map(map_path), direction = "to_icasa")$data
        
        # Format the experimental design code (LEVN*TRT1*LEVN-TRT2)
        levels <- apply(trt_icasa[,4:ncol(trt_icasa)], 2, function(x) length(unique(x)))
        levels <- levels[levels > 1]
        exp_design <- lapply(names(levels), function(nm){
          x <- levels[nm]
          paste0(x, nm)
        })
        exp_design <- paste(exp_design, collapse = "*")
        
        # Add the focal nutrient(s) if fertilization is a treatment
        if(grepl("FE", exp_design)){
          fert <- dataset$FERTILIZERS
          fert <- apply(fert[,startsWith(colnames(fert),"FAM")], 2, function(x) length(unique(x)))
          elem <- names(fert[fert > 1])
          elem <- paste0(gsub("FAM", "", elem), collapse = "")
          exp_design <- gsub("FE", paste0("FE(", elem, ")"), exp_design)
        }
        
        # Concatenate standard experiment title
        exp_details <- toupper(
          paste0(prov_meta$EXP.DETAILS, ", ", loc_city, ", ", loc_country, "  ", exp_design)
        )
        
        # Standardize field identifier --------------------------------------------
        loc_meta <- loc_meta %>%
          mutate(ID_FIELD = paste0(
            strict_abbreviate(prov_meta$INSTITUTION),
            strict_abbreviate(FLL3),
            sprintf("%04d", L)
          )) %>%
          select(-c(FLL1, FLL2, FLL3))
        
        out <- list(filename, exp_details, loc_meta)
      }
      std_nms <- apply_standard_naming()
      
      # TODO: autofill experimental design if not done + fix mapping EXP.DETAILS
      
      # Assign changes to dataset -----------------------------------------------
      attr(dataset, "experiment") <- std_nms[[2]]
      attr(dataset, "file_name") <- std_nms[[1]]
      attr(dataset, "field_id") <- std_nms[[3]]$ID_FIELD  #tmp: necessary?
      attr(dataset, "comments") <- NULL #keep from ICASA?
    }
    if (section == "soil"){
      
      metadata <- attr(data, "metadata")
      if (is.null(metadata)){
        message("Metadata table not found.")
        return(data)
      }
      
      coords <- data.frame(x = mean(metadata$LONG), y = mean(metadata$LAT))  # retrieve coordinates
      addr <- reverse_geocode(coords, long = x, lat = y, method = 'osm', full_results = TRUE, quiet = TRUE)  # find location name
      institute <- "Technische Universität München"  # TODO: add to metadata
      year <- 2023  # TODO: add to metadata
      soil_nr <- "0001"  # TODO: figure out based on whether institute SOL file exists (if no==>0001, if yes, n+1)
      soil_id <- toupper(
        paste0(
          strict_abbreviate(institute),  # institute
          strict_abbreviate(addr$town),  # site
          substr(year, 3, 4),  # year
          soil_nr)
      )
      
      metadata <- metadata %>%
        mutate(PEDON = soil_id,  # TODO: add more elements in template!
               SOURCE = "SoilGrids", # TODO: attr(data, "soil_source") kept in mapping
               TEXTURE = NA,  # TODO: function to estimate texture based on profile data
               DEPTH = max(data$SLB),
               DESCRIPTION = NA,  # TODO: in fetch metadata
               SITE = addr$town,
               COUNTRY = addr$country,
               SMHB = "IB001", SMPX = "IB001", SMKE = "IB001")
      
      data <- list(SOIL_METADATA = metadata, SOIL_PROFILE_LAYERS = data)
      
      # TODO: autofill experimental design if not done + fix mapping EXP.DETAILS
      filename <- paste0(strict_abbreviate(institute), ".SOL")
      attr(data, "title") <- toupper(paste0(institute, ", ", addr$town, ", ", addr$state, ", ", addr$country))
      attr(data, "file_name") <- filename
      attr(data, "comments") <- NULL ##
      
    }
  }
  return(dataset)
}