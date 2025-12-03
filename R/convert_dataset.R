#' Convert a Dataset Between Data Models
#'
#' @description
#' Top-level wrapper function to convert a dataset from a source to a target data model,
#' handling configuration, mapping, and post-processing steps.
#'
#' @param dataset The input dataset as a named list of data frames.
#' @param input_model The character name of the source data model (e.g., "bonares-lte_de").
#' @param output_model The character name of the target data model (e.g., "icasa").
#' @param unmatched_code A string defining how `map_values` handles unmapped codes: 
#'   `"na"`, `"pass_through"`, or `"default_value"`.
#' @param config_path The file path to the `datamodels.yaml` configuration file.
#'
#' @return The final, converted, and post-processed dataset.
#' 
#' @export
#'

convert_dataset <- function(dataset, input_model, output_model,
                            unmatched_code = "na", 
                            config_path = "./inst/extdata/datamodels.yaml") {
  
  # =================================================================
  # 1- Mapping configuration
  # =================================================================
  
  # Retrieve data model configuration
  config <- .config_input(dataset, config_path, input_model, output_model)
  data_config <- config$data
  data_map <- config$map
  rules <- data_map$rules
  
  # Check if mapping is implemented
  if (is.null(data_map)) {
    stop(paste("No mapping defined for '", input_model, "' to '", output_model, "'.", sep = ""))
  }
  
  # =================================================================
  # 2- Mapping execution
  # =================================================================
  
  message("Step 1: Mapping data terms...")
  # Converts columns from source names to canonical target names (e.g., Bonares -> ICASA)
  
  code_handlers <- c("na", "pass_through", "default_value")
  unmatched_code <- match.arg(unmatched_code, code_handlers)
  
  mapped_data <- .apply_mapping_rules(
    input_data = data_config,
    rules = rules,
    unmatched_code = unmatched_code
  )
  
  # =================================================================
  # 3- Output standardization
  # =================================================================
  
  # Applies model-specific logic like aggregations and calculations.
  message("Step 2: Standardizing output format...")
  
  # --- Deduplicate and drop NAs (CHECK: redundancy in format_to_model??)
  mapped_data_clean <- lapply(mapped_data, remove_all_na_cols)
  mapped_data_clean <- lapply(mapped_data, function(df) unique(df))

  # --- Apply post-processing logic, if applicable ---
  out <- standardize_data(dataset = mapped_data_clean, data_model = output_model)
  
  # OUT
  if (is.list(out) && length(out) == 1) {
    return(out[[1]])  # Single experiment
  } else {
    return(out)  # Multiple experiment
  }
}


#'
#'
#' @noRd
#'

.config_input <- function(dataset, config_path, input_model, output_model) {
  
  # Fetch configuration for the focal data model
  config <- yaml::read_yaml(config_path)
  output_model_config <- config[[output_model]]
  input_model_config  <- config[[input_model]]
  
  # Load the map file for the focal data mapping
  map_path <- output_model_config$mappings[[input_model]]
  map <- yaml::read_yaml(map_path)
  
  # Pre-process master keys
  # >> If the input model has a master key, apply to all tables
  has_mk <- !is.null(input_model_config$master_key) &&
    input_model_config$master_key != 'none'
  
  if (has_mk) {
    config_data <- .apply_master_key(dataset, input_model_config)
  } else {
    config_data <- dataset
  }
  out <- list(data = config_data, map = map)
  
  return(out)
}


# ' Propagate the Master Key Through a Dataset
#'
#' @description
#' A pre-processing step that intelligently links all tables in a dataset by ensuring the master key is present.
#'
#' @details
#' The function's behavior depends on the number of unique master keys found:
#' - **Single-Experiment:** For tables missing the key, it adds a column containing the single unique master key value.
#' - **Multi-Experiment:** For "shared" tables (e.g., `WEATHER`, `SOIL`), it performs a `cross_join` to link every
#'    master key to the shared data.
#'
#' @param dataset The input dataset as a list of data frames.
#' @param model_config The configuration block for the input data model.
#'
#' @return The dataset with the master key propagated to all relevant tables.
#' 
#' @noRd
#'

.apply_master_key <- function(dataset, model_config) {
  
  key_name_conceptual <- model_config$master_key
  key_source_table <- model_config$key_source_table
  key_name_actual <- model_config$design_keys[[key_name_conceptual]]
  shared_tables <- unlist(model_config$shared_tables)
  
  if (is.null(key_name_actual) || is.null(key_source_table) || !key_source_table %in% names(dataset)) {
    warning("Master key details not defined correctly in datamodels.yaml. Skipping linking.", call. = FALSE)
    return(dataset)
  }
  
  unique_keys <- unique(na.omit(dataset[[key_source_table]][[key_name_actual]]))
  
  if (length(unique_keys) == 0) {
    # This warning is expected if the key is missing in the data
    return(dataset)
  }
  
  is_single_experiment <- length(unique_keys) == 1
  
  # Propagate the master key to other tables
  for (table_name in names(dataset)) {
    if (table_name == key_source_table) next
    
    df <- dataset[[table_name]]
    has_key <- key_name_actual %in% names(df)
    is_shared <- table_name %in% shared_tables
    
    if (is_single_experiment && !has_key) {
      df[[key_name_actual]] <- unique_keys[1]
    } else if (!is_single_experiment && !has_key && is_shared) {
      key_df <- data.frame(!!key_name_actual := unique_keys)
      df <- dplyr::cross_join(df, key_df)
    } # Add other cases here...
    
    dataset[[table_name]] <- df
  }
  
  # Ensure the master key is the first column across all tables
  for (table_name in names(dataset)) {
    df <- dataset[[table_name]]
    
    if (key_name_actual %in% names(df)) {
      df <- dplyr::select(df, dplyr::all_of(key_name_actual), tidyr::everything())
      dataset[[table_name]] <- df
    }
  }
  
  return(dataset)
}
