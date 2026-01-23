#' (Internal) Run Phase 2 Formatting Rules
#' @noRd

.apply_mapping_rules <- function(input_data, rules, ...) {
  
  output_data <- list()
  schema_rule_uid <- '999' # Define the UID for the special schema rule
  
  # =================================================================
  # PHASE 1: Execute all transformation rules in order
  # =================================================================
  
  transformation_rules <- rules[sapply(rules, function(r) r$mapping_uid != schema_rule_uid)]
  
  # Sort rules based on order
  orders <- sapply(transformation_rules, function(r) ifelse(is.null(r$order), 0, r$order))
  transformation_rules <- transformation_rules[order(orders)]
  # Note: rules without an 'order' key are assumed to be independent and run first (order = 0).
  
  if (length(transformation_rules) == 0) {
    output_data <- input_data
  } else {
    for (rule in transformation_rules) {
      # If source exists in intermediate output data, use as input (= chained rules)
      if (!is.null(rule$source$section) && rule$source$section %in% names(output_data)) {
        input_df <- output_data[[rule$source$section]]
      } else {
        input_df <- input_data[[rule$source$section]]
      }
      
      if (is.null(input_df)) next
      
      # Merge input and output datasets
      # Note: necessary for chained rules involving lookups
      merged_data <- c(output_data, input_data)
      # Remove duplicate names
      merged_data <- merged_data[!duplicated(names(merged_data))]
      
      # Execute data mapping actions
      transformed_df <- process_actions(
        mapping_uid = rule$mapping_uid,
        actions = rule$actions,
        input_df = input_df,
        dataset = merged_data,
        ...
      )
      if (nrow(transformed_df) == 0) next
      
      # Consolidate output table
      output_data <- .update_output_data(
        mapping_uid = rule$mapping_uid,
        output_data = output_data,
        target_section = rule$target$section,
        data_to_add = transformed_df
      )
    }
  }
  
  # =================================================================
  # PHASE 2: Final schema application select and rename columns
  # =================================================================
  
  schema_rule <- rules[[which(sapply(rules, function(r) r$mapping_uid == schema_rule_uid))]]
  
  if (!is.null(schema_rule)) {
    final_output_data <- list()
    
    for (action in schema_rule$actions) {
      if (action$type == "map_headers") {
        target_section <- action$target_section
        rename_map <- action$rename_map
        
        # Get the processed data frame from the mapping phase
        df_to_map <- output_data[[target_section]]
        if (is.null(df_to_map)) next
        
        final_cols_list <- list()
        for (new_name in names(rename_map)) {
          old_name_spec <- rename_map[[new_name]]
          found_col_name <- .resolve_column_name(old_name_spec, names(df_to_map))
          
          if (!is.null(found_col_name)) {
            final_cols_list[[new_name]] <- df_to_map[[found_col_name]]
          }
        }
        
        final_df <- tibble::as_tibble(final_cols_list)
        final_output_data[[target_section]] <- final_df %>%
          dplyr::distinct()
      }
    }
    return(final_output_data)
  }
  
  # If no schema rule, return the transformed but unmapped data
  return(output_data)
}


#' Update a Section in the Output Dataset
#'
#' @description
#' Intelligently merges or adds a data frame to a target section within the main output list.
#'
#' @details
#' This function updates the main `output_data` list. If the `target_section` does not yet exist, it is created.
#' If it does exist, the function joins the new data (`data_to_add`) with the existing data. The join is performed on all
#' common columns. If no common columns exist, a column-bind (`bind_cols`) is performed instead. It also suppresses and
#' replaces the default `dplyr` many-to-many join warning with a more informative message.
#'
#' @param mapping_uid The UID of the current mapping rule, used for context in warnings.
#' @param output_data The list object representing the entire output dataset.
#' @param target_section The character name of the list element (data frame) to update.
#' @param data_to_add The data frame to be added or merged.
#'
#' @return The `output_data` list with the target section updated.
#'
#' @noRd
#'

.update_output_data <- function(mapping_uid, output_data, target_section, data_to_add) {
  if (is.null(output_data[[target_section]])) {
    output_data[[target_section]] <- data_to_add
    return(output_data)
  }
  existing_df <- output_data[[target_section]]
  join_keys <- intersect(names(existing_df), names(data_to_add))
  
  if (length(join_keys) > 0) {
    # Intercept and muffle standard warning message
    output_data[[target_section]] <- withCallingHandlers(
      expr = {
        dplyr::full_join(existing_df, data_to_add, by = join_keys)
      },
      warning = function(w) {
        if (grepl("many-to-many relationship", w$message)) {
          warning(paste0("A many-to-many join occurred in rule '", mapping_uid, "' targeting section: '",
                         target_section, "'.\nThis may be expected of management regimes comprising of multiple events."),
                  call. = FALSE)
          invokeRestart("muffleWarning")
        }
      }
    )
  } else {
    output_data[[target_section]] <- dplyr::bind_cols(existing_df, data_to_add)
  }
  return(output_data)
}