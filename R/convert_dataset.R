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
#' @importFrom yaml read_yaml
#' 
#' @export
#'

convert_dataset <- function(dataset, input_model, output_model,
                            unmatched_code = "na", 
                            config_path = "./inst/extdata/datamodels.yaml") {
  
  # dataset = duernast_icasa
  # input_model = "icasa"
  # output_model = "dssat"
  # unmatched_code = "default_value"
  
  # --- 1. MAPPING CONFIGURATION ---
  config <- yaml::read_yaml(config_path)
  model_config <- config[[output_model]]
  
  if (is.null(model_config)) {
    stop(paste("Output model '", output_model, "' is not defined in the configuration file.", sep = ""))
  }
  
  map_path <- model_config$mappings[[input_model]]
  if (is.null(map_path)) {
    stop(paste("No mapping defined for '", input_model, "' to '", output_model, "'.", sep = ""))
  }
  
  # Dynamically set model properties using new key names
  master_key <- model_config$master_key
  key_source_table <- model_config$key_source_table
  data_tables <- unlist(model_config$data_tables)
  
  # --- 2. CORE MAPPING ---
  
  message("Step 1: Mapping data terms...")
  # Converts columns from source names to canonical target names (e.g., Bonares -> ICASA)
  
  code_handlers <- c("na", "pass_through", "default_value")
  unmatched_code <- match.arg(unmatched_code, code_handlers)
  
  mapped_data <- apply_mapping_rules(
    input_data = dataset,
    input_model = input_model,
    output_model = output_model,
    config = config,
    unmatched_code = unmatched_code
  )
  
  # --- 3. POST-PROCESSING ---
  
  # Applies model-specific logic like aggregations and calculations.
  message("Step 2: Structuring and formatting model data...")
  
  # --- Deduplicate and drop NAs (CHECK: redundancy in format_to_model??)
  mapped_data_clean <- lapply(mapped_data, remove_all_na_cols)
  mapped_data_clean <- lapply(mapped_data, function(df) unique(df))

  #return(mapped_data_clean)  #tmp
  
  # Takes the structured data and writes it to disk in the required format.
  message("Step 3: Writing output files...")
  out <- standardize_data(dataset = mapped_data_clean, data_model = output_model)
  
  # --- OUTPUT ---
  if (is.list(out) && length(out) == 1) {
    return(out[[1]])  # Single experiment
  } else {
    return(out)  # Multiple experiment
  }
}
