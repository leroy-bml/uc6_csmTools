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

.standardize_icasa_data() <- function(dataset, data_model = c("icasa", "dssat"),
                                  config_path = "./inst/extdata/datamodels.yaml") {
  
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
