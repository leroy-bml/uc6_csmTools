#' Apply a Full Set of 3-Phase Mapping Rules to a Dataset
#'
#' @description
#' Orchestrates the entire data conversion process by running mapping, formatting,
#' and aggregation rules in three distinct phases.
#'
#' @details
#' This is the main engine for data conversion. It loads a single, unified map file
#' and executes the rules it contains in a strict order:
#' 1.  **Phase 1 (Mapping):** Runs rules with `type = NULL`.
#' 2.  **Phase 2 (Formatting):** Runs rules with `type = "apply_formats"`.
#' 3.  **Phase 3 (Aggregation):** Runs rules with `type = "aggregate"`.
#'
#' @param input_data The source dataset as a list of data frames.
#' @param input_model The character name of the source model.
#' @param output_model The character name of the target model.
#' @param config The parsed `datamodels.yaml` configuration object.
#' @param ... Optional arguments (like `unmatched_code`) passed to `process_actions`.
#'
#' @return The final, processed dataset as a list of data frames.
#' @importFrom yaml read_yaml
#' @importFrom tibble tibble as_tibble
#' @importFrom dplyr bind_cols setdiff
#'
#' @export
#'
apply_rules <- function(input_data, input_model, output_model, config, ...) {
  
  # --- 1. CONFIGURATION SETUP ---
  output_model_config <- config[[output_model]]
  input_model_config  <- config[[input_model]]
  
  # Load the unified map file
  map_path <- output_model_config$mappings[[input_model]]
  map <- yaml::read_yaml(map_path)
  
  # --- 2. MASTER KEY LOGIC ---
  master_key_df <- NULL
  input_master_key_name <- NULL
  
  if (!is.null(input_model_config$master_key) && input_model_config$master_key == 'none') {
    prepared_data <- input_data
  } else {
    prepared_data <- apply_master_key(input_data, input_model_config)
    
    input_master_key_name <- input_model_config$design_keys[[input_model_config$master_key]]
    output_master_key_name <- output_model_config$design_keys[[output_model_config$master_key]]
    key_source_table  <- input_model_config$key_source_table
    
    master_key_df <- tibble::tibble(
      !!output_master_key_name := prepared_data[[key_source_table]][[input_master_key_name]]
    )
  }
  
  # --- 3. EXECUTE PHASES IN ORDER ---
  
  # --- PHASE 1: MAPPING ---
  phase1_rules <- Filter(function(r) is.null(r$type), map$rules)
  mapped_data <- .apply_mapping_rules(
    rules = phase1_rules,
    prepared_data = prepared_data,
    master_key_df = master_key_df,
    master_key_name = input_master_key_name,
    ...
  )
  
  # --- PHASE 2: FORMATTING ---
  phase2_rules <- Filter(function(r) !is.null(r$type) && r$type == "apply_formats", map$rules)
  formatted_data <- .apply_formatting_rules(
    rules = phase2_rules,
    dataset = mapped_data
  )
  
  # --- PHASE 3: AGGREGATION ---
  phase3_rules <- Filter(function(r) !is.null(r$type) && r$type == "aggregate", map$rules)
  aggregated_data <- .apply_aggregation_rules(
    rules = phase3_rules,
    dataset = formatted_data
  )
  
  return(aggregated_data)
}


#' (Internal) Run Phase 1 Mapping Rules
#' @noRd
.apply_mapping_rules <- function(rules, prepared_data, master_key_df, master_key_name, ...) {
  
  output_data <- list()
  
  for (rule in rules) {
    full_input_df <- prepared_data[[rule$source$section]]
    if (is.null(full_input_df)) next
    
    transformed_df <- process_actions(
      actions = rule$actions,
      full_input_df = full_input_df,
      full_dataset = prepared_data,
      master_key_name = master_key_name,
      ...
    )
    
    if (nrow(transformed_df) == 0) next
    
    # Select and rename the final columns using the map
    final_cols_list <- list()
    if (!is.null(rule$select_final_columns)) {
      rename_map <- rule$select_final_columns
      for (new_name in names(rename_map)) {
        old_name_spec <- rename_map[[new_name]]
        found_col_name <- .resolve_column_name(old_name_spec, names(transformed_df))
        if (!is.null(found_col_name)) {
          final_cols_list[[new_name]] <- transformed_df[[found_col_name]]
        }
      }
      final_df <- as_tibble(final_cols_list)
    } else {
      warning(paste0("Rule '", rule$mapping_uid, "' has no 'select_final_columns' key."), call. = FALSE)
      final_df <- transformed_df
    }
    
    # Add keys to the final result
    if (!is.null(master_key_df)) {
      master_key_cols_to_add <- setdiff(names(master_key_df), names(final_df))
      if (length(master_key_cols_to_add) > 0) {
        final_df <- bind_cols(master_key_df, final_df)
      }
    }
    
    # Update the main output list
    output_data <- update_output_data(rule$mapping_uid, output_data, rule$target$section, final_df)
  }
  
  return(output_data)
}


#' (Internal) Run Phase 2 Formatting Rules
#' @noRd
.apply_formatting_rules <- function(rules, dataset) {
  
  if (is.null(rules) || length(rules) == 0) {
    return(dataset) # No formatting rules
  }
  
  processed_dataset <- dataset
  
  for (rule in rules) {
    for (action in rule$actions) {
      target_section <- action$target_section
      
      if (!target_section %in% names(processed_dataset)) next
      
      target_df <- processed_dataset[[target_section]]
      
      # Apply the formatting rules
      if (!is.null(action$formats)) {
        for (col_name in names(action$formats)) {
          if (col_name %in% names(target_df)) {
            format_string <- action$formats[[col_name]]
            # Simple check to differentiate date vs. numeric formatting
            if (grepl("%", format_string)) {
              target_df[[col_name]] <- format(as.Date(target_df[[col_name]]), format_string)
            } else {
              target_df[[col_name]] <- sprintf(format_string, as.numeric(target_df[[col_name]]))
            }
          }
        }
      }
      # Update the data frame in the main output list
      processed_dataset[[target_section]] <- target_df
    }
  }
  
  return(processed_dataset)
}


#' (Internal) Run Phase 3 Aggregation Rules
#' @noRd
.apply_aggregation_rules <- function(rules, dataset) {
  
  if (is.null(rules) || length(rules) == 0) {
    return(dataset) # No aggregation rules
  }
  
  processed_dataset <- dataset
  
  for (rule in rules) {
    target_section <- rule$target_section
    if (is.null(processed_dataset[[target_section]])) next
    
    # Call process_actions to execute the 'aggregate_by_group' action
    aggregated_table <- process_actions(
      actions = rule$actions,
      full_input_df = processed_dataset[[target_section]], # Input
      full_dataset = processed_dataset
    )
    
    # Overwrite the table in the dataset
    processed_dataset[[target_section]] <- aggregated_table
  }
  
  return(processed_dataset)
}