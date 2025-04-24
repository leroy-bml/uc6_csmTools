#' Remove artefact columns from a data frame
#' 
#' This function processes a data frame by eliminating unnamed columns 
#' `NA` only columns, and duplicate names.
#' 
#' @param df a data frame
#' 
#' @return a clean data frame
#' 
#' @importFrom dplyr filter
#'

# Extract data
drop_artefacts <- function(df) {

  # Identify artefact columns
  colnames(df) <- ifelse(is.na(colnames(df)), "unnamed", colnames(df))
  
  # Ensure uniqueness without sanitizing special characters (in some ICASA names; alternative to make.names)
  colnames(df) <- make.unique(colnames(df))
  
  # Remove unnammed and full NA columns
  df <- df[ , !grepl("^unnamed", colnames(df))]
  df <- df %>% filter(rowSums(is.na(.)) != ncol(.))
  
  return(df)
}


#' Extract ICASA categorical variable codes from the Excel template
#' 
#' This function processes the ICASA template (Excel format) to extract ICASA codes 
#' for categorical variables from the dropdown list worksheet.
#' 
#' @param wb the template Excel workbook, as a named list of data frames
#' NB: names should be identical to worksheet names in the Excel file.
#' 
#' @return a named list of data frames, each representing providing level names and codes
#' for a given ICASA variable
#' 
#' @importFrom openxlsx2 wb_to_df
#' @importFrom dplyr select filter
#' @importFrom tidyr ends_with all_of
#'

get_template_codes <- function(wb){
  
  codes <- wb_to_df(wb, sheet = "DropDown", startRow = 2)
  
  # Drop template helper columns (only for data entry)
  codes <- codes %>% select(-ends_with("_sort"))
  
  codes_prefixes <- unique(sub("_.*", "", names(codes)))
  
  # split into separate dataframe for each code list
  codes_ls <- lapply(codes_prefixes, function(prefix) {
    
    cols <- grep(paste0("^", prefix, "(_|$)"), names(codes), value = TRUE)
    df <- codes %>% select(all_of(cols))
    
    if (ncol(df) >= 2) {
      names(df)[1:2] <- c("desc", "code")
      df <- df %>% filter(!is.na(.[[1]]))
    } else if (ncol(df) == 1) {
      #names(df)[1] <- "desc"
      df <- data.frame(desc = character(), codes = character())
    }
    
    return(df)
  })
  names(codes_ls) <- codes_prefixes

  codes_ls <- codes_ls[!names(codes_ls) %in% "country"]
  #codes_ls$country <- codes_df$country %>%
  #  mutate(code = countrycode(desc, origin = "country.name", destination = "genc2c"))
  
  return(codes_ls)
}


#' Extract data from the ICASA Excel template and return as an
#' ICASA-formatted dataset
#' 
#' This function loads the ICASA Excel template, extracts relevant sheets, 
#' removes workbook artefacts, maps dropdown descriptions to ICASA codes, 
#' and formats data to ICASA. Different experiments are returned as 
#' separate datasets. 
#' 
#' @param path
#' @param exp_id
#' @param headers 
#' 
#' @return A named list of data frames, each containing an experiment's extracted data.
#' 
#' @importFrom openxlsx2 wb_load wb_get_sheet_names wb_to_df
#' @importFrom dplyr filter mutate
#'
#' @export
#' 

extract_template <- function(path, exp_id = NA_character_, headers = c("short", "long")){
  
  # Load workbook and set name criterion for sheets to be loaded (capitalized sheet names)
  wb <- suppressWarnings(wb_load(path))
  nms <- wb_get_sheet_names(wb)
  nms_data <- nms[grepl("^[A-Z_]+$", nms)]
  
  # Extract all sections and section names
  dfs <- list()
  for (i in 1:length(nms_data)){
    df <- wb_to_df(wb, sheet = nms_data[i], startRow = 4)
    #attr(df, "section") <- nms_data[i]
    df <- drop_artefacts(df)
    
    dfs[[i]] <- df
    names(dfs)[i] <- nms_data[i]
  }

  # Map template dropdown list descriptions into ICASA codes
  codes <- get_template_codes(wb)
  dfs <- lapply(dfs, function(df){ desc_to_codes(df, codes) })
  
  # Delete empty dataframes
  # TODO: quality check if mandatory data is input
  #dfs <- dfs[!sapply(dfs, function(df) all(is.null(df) || dim(df) == c(0, 0)) || all(is.na(df)))]
  dfs <- Filter(function(df) !is.null(df) || !all(dim(df) == c(0, 0)) || !all(is.na(df)), dfs)
  
  # Replace treatment names by treatment number in the treatment matrix
  dfs <- format_envmod_tbl(dfs)
  dfs <- format_treatment_str(dfs)
  dfs <- format_metadata(dfs, data_model = "icasa")
  
  # Map column headers to short format if applicable
  # TODO: extensive testing (e.g., no provenance sheets or empty)
  dict <- wb_to_df(wb, sheet = "Dictionary", startRow = 1) %>%
    # Change provenance section to experiment (now that provenance info has been incorporated to SOIL and WEATHER metadata)
    mutate(Sheet = ifelse(Sheet %in% c("PERSONS","INSTITUTIONS","DOCUMENTS"), "EXP_METADATA", Sheet)) %>%
    filter(!var_order_custom == "-99" | is.na(var_order_custom))  # tmp: preserve NAs until measured data all sorted in template
  if(headers == "short") {
    dfs <- mapply(FUN = icasa_long_to_short,
                  df = dfs,
                  section = names(dfs),
                  MoreArgs = list(dict = dict, keep_unmapped = FALSE)
    )
  }

  # Split experiments
  out <- split_experiments(dfs)
  if(!all(is.na(exp_id))){
    out <- out[exp_id %in% names(out)]
  } 

  return(out)
}


#' Convert ICASA variables from long to short names
#' 
#' This function maps long-format variable names in a data frame to 
#' standardized ICASA short header codes by querying the ICASA dictionary.
#' 
#' @param df A data frame with ICASA long-format column names.
#' @param section A character string specifying the dictionary section to be queried.
#' @param dict A data frame containing the ICASA dictionary an DSSAT mappings.
#' @param keep_unmapped Logical; if FALSE, columns not mapped will be dropped.
#' 
#' @return A data frame with ICASA short column names.
#' 
#' @importFrom dplyr rename_width
#'

icasa_long_to_short <- function(df, section, dict, keep_unmapped = TRUE){

  # Generate mapping vector for the focal section
  subset <- dict[dict$Sheet == section,]
  rename_vector <- setNames(subset$Code_Query, subset$var_name)
  rename_vector <- rename_vector[!is.na(rename_vector)]  # remove additional terms with no ICASA code (added to structure the template)
  rename_vector <- rename_vector[names(rename_vector) %in% names(df)]  # keep only columns present in the dataframe
  
  # Perform the mapping
  df <- df %>% rename_with(~ rename_vector[.x], .cols = names(rename_vector))
  
  if (keep_unmapped == FALSE) df <- df[colnames(df) %in% rename_vector]
  
  return(df)
}


#' Convert categorical variable levels to ICASA codes
#' 
#' This function replaces level description used in drop down lists for
#' categorical variables with their corresponding ICASA codes using a reference dictionary.
#' 
#' @param df A data frame containing ICASA variables with descriptive categories
#' @param codes A named list of code mappings for ICASA variables
#' 
#' @return A data frame with descriptive names replaced by corresponding ICASA codes.
#'

desc_to_codes <- function(df, codes) {
  
  # TODO: homogenize with existing recoding function
  # Iterate through each column of the data df
  for (col in names(df)) {
    # Check if the column is categorical (character or factor)
    if (is.character(df[[col]]) || is.factor(df[[col]])) {
      # Iterate through each dataframe in the code list
      for (code_df in codes) {
        # Check if the column contains any of the variable names in the code dataframe
        if (any(df[[col]] %in% code_df$desc)) {
          # Replace the descriptive names with the corresponding codes
          
          if (length(df[[col]]) == 0) {
            message("Column ", col, " is empty.")
          } else {
            df[[col]] <- vapply(df[[col]], function(x) {
              match_index <- match(x, code_df$desc)
              if (!is.na(match_index)) {
                as.character(code_df$code[match_index])
              } else {
                as.character(x)
              }
            }, FUN.VALUE = character(1))
          }
        }
      }
    }
  }
  
  return(df)
}


#' Format reference identifiers for treatments and observation units
#' 
#' This function removes helper description from treatment/plot selection
#' drop down lists used in the template to leave only integer indentifiers
#' 
#' @param ls an ICASA dataset as a list of named data frames
#' 
#' @return an ICASA dataset as a list with cleaned treatment and measured data tables
#' 
#' @importFrom dplyr across where mutate
#' @importFrom tidyr everything
#'

format_treatment_str <- function(ls){

  # Format treatment matrix
  if("TREATMENTS" %in% names(ls)){
    df <- ls[["TREATMENTS"]]
  } else {
    error("Treatment matrix not found.")
  }
  
  df <- df %>%
    mutate(across(where(is.character), ~ sub("\\|.*", "", .)),
           across(everything(), ~ trimws(., which = "left")),
           across(genotype_level_number:mulch_level, ~ifelse(is.na(.x), 0, .x)),
           across(c(treatment_number, genotype_level_number:mulch_level), ~as.numeric(.))) %>%
    mutate(simulation_control_level = as.numeric(
      ifelse(is.na(simulation_control_level), 1, simulation_control_level))
    )
  
  ls[["TREATMENTS"]] <- df  # Replace original matrix
  
  # Format measured data
  obs <- ls[which(grepl("TS_|SM_|OBSERV_", names(ls)))]
  obs <- lapply(obs, function(df){
    df <- df %>%
      mutate(treatment_number = sub("\\|.*", "", treatment_number),
             treatment_number = as.numeric(treatment_number))
  })
  ls[names(obs)] <- obs  # Replace original tables
  
  return(ls)
}


#' Format environmental modification table
#' 
#' This function restructures the 'Environmental modification' table to the
#' ICASA standard by pivoting it into a wide format.
#' Returns the input when environmental modifications are missing.
#' 
#' @param ls an ICASA dataset as a list of named data frames
#' 
#' @return An ICASA dataset as a list with reformatted environmental modifications tables
#' 
#' @importFrom dplyr select
#' @importFrom tidyr pivot_wider
#'

format_envmod_tbl <- function(ls){
  
  if("ENVIRON_MODIF_LEVELS" %in% names(ls)){

    df <- ls[["ENVIRON_MODIF_LEVELS"]]
    
    df <- df %>%
      select(-environ_parameter_unit) %>%
      pivot_wider(names_from = environ_parameter, values_from = c(modif_code, environ_parameter_value))
    
    colnames(df) <- gsub(pattern = "modif_code", replacement = "environ_modif_code", colnames(df))
    colnames(df) <- gsub(pattern = "environ_parameter_value", replacement = "environ_modif", colnames(df))

    ls[["ENVIRON_MODIF_LEVELS"]] <- df
  }
  
  return(ls)
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
      
      dataset <- uc6_dssat  #tmp
      
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
        
        dataset <- uc6_dssat  #tmp
        
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


#' ###
#' 
#' 
#' @param df ###
#' 
#' @return a data frame ###
#' 
#' @importFrom dplyr summarise across select_if n_distinct all_of
#' @importFrom tidyr everything
#'

# # TODO: add_property_mapping <- function(name, unit, icasa = list(), agg_funs = list())
concatenate_per_group <- function(df) {
  
  constant_cols <- df %>%
    summarise(across(everything(), ~ n_distinct(.) == 1)) %>% 
    select_if(~ .) %>%
    colnames()
  
  other_cols <- base::setdiff(colnames(df), constant_cols)
  
  out <- df %>% 
    group_by(across(all_of(constant_cols))) %>%
    summarise(across(all_of(other_cols), ~ paste(., collapse = ", ")), .groups = 'drop')
  
  return(out)
}


#' Generate strict abbreviations for strings
#'
#' This function creates abbreviations for strings with a specified minimum length, 
#' ensuring strict truncation if necessary.
#' 
#' @param string A character vector to be abbreviated.
#' @param minlength An integer specifying the minimum length of the abbreviation.
#' 
#' @return A character vector of abbreviated strings
#'

strict_abbreviate <- function(string, minlength = 2) {
  abbr <- abbreviate(string, minlength = minlength, strict = TRUE)
  if (nchar(abbr) > minlength) {
    abbr <- substr(abbr, 1, minlength)
  }
  return(abbr)
}

#' Format and merge ICASA-format measured data
#' 
#' This function classifies and merges measured data tables from 
#' the ICASA template into SUMMARY and TIME_SERIES tables
#' 
#' @param ls An ICASA dataset as a list of data frames
#' 
#' @return An ICASA dataset as a list of data frames with tidy measured data
#' 
#' @importFrom dplyr left_join
#'

format_observed_data <- function(ls){
  
  merge_obs <- function(section_nm) {
    
    pattern <- paste0(section_nm, "|OBSERV_DATA_LINKS")
    section <- ls[grepl(pattern, names(ls))]
    
    section <- Reduce(function(x, y)
      left_join(x, y, by = intersect(names(x), names(y))),
      c(section[1], section[-1]))
    
    if (identical(colnames(section), colnames(ls$OBSERV_DATA_LINKS))) { 
      return(NULL) 
    } else { 
      return(section) 
    }
  }
    
    ls$SUMMARY <- merge_obs("SM_")
    ls$TIME_SERIES <- merge_obs("TS_")
    
    ls <- ls[!grepl("SM_|TS_|OBS", names(ls))]
    
    return(ls)
}


#' ###
#' 
#' 
#' @param df ###
#' 
#' @return a data frame ###
#' 
#' @importFrom ###
#'

reshape_to_model_dssat <- function(vector, map, input_model = "icasa", output_model = "dssat") {
  
  # NB: temporary approach; later integrate to a generic melt-explode-map sequence
  
  map <- load_map( # TODO: make internal data object
    "C:/Users/bmlle/Documents/0_DATA/TUM/HEF/FAIRagro/2-UseCases/UC6_IntegratedModeling/Workflows/csmTools/data/icasa_mappings.csv"
  ) %>%
    filter(dataModel == output_model) %>%
    select(template_section, section) %>%
    distinct()
  
  # Create a named vector for easy lookup
  rename_map <- setNames(map$section, map$template_section)
  
  # Rename elements of the input vector using the lookup table
  renamed_vector <- sapply(vector, function(x) {
    if (x %in% names(rename_map)) {
      rename_map[x]
    } else {
      x
    }
  })
  
  names(renamed_vector) <- NULL
  return(renamed_vector)
}


#' ###
#' 
#' 
#' @param df ###
#' 
#' @return a data frame ###
#' 
#' @importFrom ###
#'

combine_dual_tier <- function(ls) {
  
  unique_names <- unique(names(ls))  # find unique names
  
  out <- list()
  for (name in unique_names) {
    
    section_dfs <- ls[names(ls) == name]  # filter dataframes with the same name
    common_cols <- Reduce(intersect, lapply(section_dfs, colnames))
    
    joined_dfs <- Reduce(function(x, y) merge(x, y, by = common_cols, all = TRUE), section_dfs)  # merged sections
    out[[name]] <- joined_dfs
  }
  return(out)
}



#' ###
#' 
#' 
#' @param df ###
#' 
#' @return a data frame ###
#' 
#' @importFrom ###
#'

remap <- function(dataset, input_model = "icasa", output_model = "dssat"){
  
  # dataset <- uc6_icasa   #tmp
  # input_model = "icasa"  #tmp
  # output_model = "dssat"  #tmp
  
  # Reshape to data model (ICASA -> DSSAT)
  rdata <- format_observed_data(dataset)  # All measured data into SUMMARY and TIME_SERIES tables
  names(rdata) <- reshape_to_model_dssat(vector = names(rdata))  # Rename sections #TMP
  
  # Map data within target section
  mdata <- list()
  for (i in 1:length(rdata)){
    
    sec <- names(rdata)[i]
    map <- load_map(map_path)   # FIX definition
    sub <- map %>% filter(section %in% sec)
    
    mdata[[i]] <- map_data(rdata[[i]], input_model, output_model, map = sub, keep_unmapped = FALSE, col_exempt = NULL)
    names(mdata)[i] <- sec
  }
  
  # Combine dual tier sections (header + events/layers)
  fdata <- combine_dual_tier(mdata)
  
  
  return(fdata)
}


#' ###
#' 
#' 
#' @param df ###
#' 
#' @return a data frame ###
#' 
#' @importFrom ###
#'

# NB: fails when applied to the mapped dataset with unmapped variables kept, due to duplicate name in multiple table (ELEV.x, ELEV.y)
split_experiments <- function(ls) {
  
  # Set the splitting factor (IDs) for each data type (experiment, soil, weather)
  if(all(grepl("^[A-Z_]+$", colnames(ls[[1]])))){
    fcts <- c(exp = "EID", sol = "SOIL_SUBSET", wth = "WTH_SUBSET")
  } else {
    fcts <- c(exp = "experiment_ID", sol = "soil_identifier", wth = "weather_sta_identifier")
  }
  
  #1- split EID - nonEDI
  exp_dfs <- Filter(function(df) fcts[["exp"]] %in% names(df), ls)
  env_dfs <- Filter(function(df) !fcts[["exp"]] %in% names(df), ls)
  
  exp_dfs_split <- lapply(exp_dfs, function(df) split(df, f = df[[fcts[["exp"]]]]))
  exp_dfs_split <- revert_list_str(exp_dfs_split)

  # Initialize lists to store lookup values/dataframes
  sol_split <- wth_split <- c()
  
  for (i in 1:length(exp_dfs_split)){
    
    # Identify profile and station IDs for each experiment
    profile_id <- exp_dfs_split[[i]]$FIELDS[[fcts[["sol"]]]]
    station_id <- exp_dfs_split[[i]]$FIELDS[[fcts[["wth"]]]]
    
    sol_split[[i]] <- lapply(env_dfs, function(df) {
      if (nrow(df) == 0) {
        return(df)  # Return empty df unchanged
      } else {
        return(df[rowSums(df == profile_id, na.rm = TRUE) > 0, , drop = FALSE])
      }
    })
    sol_split[[i]] <- Filter(function(df) nrow(df) > 0, sol_split[[i]])  # Remove empty dataframes
    
    wth_split[[i]] <- lapply(env_dfs, function(df) {
      if (nrow(df) == 0) {
        return(df)  # Return empty df unchanged
      } else {
        return(df[rowSums(df == station_id, na.rm = TRUE) > 0, , drop = FALSE])
      }
    })
    wth_split[[i]] <- Filter(function(df) nrow(df) > 0, wth_split[[i]])  # Remove empty dataframes
    
    # Append data when it exists
    if (length(sol_split[[i]]) > 0 || length(wth_split[[i]]) > 0) {
      exp_dfs_split[[i]] <- c(exp_dfs_split[[i]], sol_split[[i]], wth_split[[i]])
    }
    
    # Remove empty data frames
    exp_dfs_split[[i]] <- Filter(function(df) !is.null(df) || 
                                   !all(dim(df) == c(0, 0)) || 
                                   !all(is.na(df)), 
                                 exp_dfs_split[[i]])
    
    # Remove NA columns generated by splitting in measured data
    nms <- names(exp_dfs_split[[i]])
    exp_dfs_split[[i]] <- lapply(names(exp_dfs_split[[i]]), function(name) {
      
      ls <- exp_dfs_split[[i]]
      df <- ls[[name]]
      
      if (grepl("^SM_|TS_", name)) {
        df <- df[, colSums(!is.na(df)) > 0, drop = FALSE]  # Remove all-NA columns
      }
      
      return(df)
    })
    names(exp_dfs_split[[i]]) <- nms  # Restore names
  }
  
  return(exp_dfs_split)
}



