#' Resolve an Input Map to a List of Data Vectors
#'
#' @description
#' Resolves a list of column specifications from a YAML rule against a data frame, extracting the corresponding data vectors.
#'
#' @details
#' This helper function is used by actions that require multiple inputs. It iterates through the `input_map`, uses
#' `resolve_column_name()` to find each column (handling synonyms), and returns a named list of the actual data vectors.
#' It is designed to fail gracefully by returning `NULL` if any specified column is not found.
#'
#' @param input_map A named list from a YAML rule where keys are local names and values are column specifications
#' (e.g., `{x: colA, y: {header: colB}}`).
#' @param df The data frame from which to pull the column vectors.
#'
#' @return A named list of data vectors (e.g., `list(x=c(1,2), y=c(3,4))`), or `NULL` if any column cannot be resolved.
#'

resolve_input_map <- function(input_map, df) {
  resolved_inputs <- list()
  
  for (local_name in names(input_map)) {
    col_spec <- input_map[[local_name]]
    found_col_name <- resolve_column_name(col_spec, names(df))
    
    if (is.null(found_col_name)) {
      # Safely get the column name for the warning message
      display_name <- if (is.list(col_spec)) col_spec$header else col_spec
      warning(paste0("Input column '", display_name, "' not found. Skipping action."), call. = FALSE)
      return(NULL)
    }
    resolved_inputs[[local_name]] <- df[[found_col_name]]
  }
  return(resolved_inputs)
}

# ------------------------------------------------------------------------------------------------------------------------

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
#' @importFrom dplyr full_join bind_cols
#'

update_output_data <- function(mapping_uid, output_data, target_section, data_to_add) {
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
        full_join(existing_df, data_to_add, by = join_keys)
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
    output_data[[target_section]] <- bind_cols(existing_df, data_to_add)
  }
  return(output_data)
}


# ------------------------------------------------------------------------------------------------------------------------

#' Aggregate a Vector to a Single Value
#'
#' @description
#' A helper function to consolidate a vector, typically used within `summarise()`. 
#' It returns a single unique value or a concatenated string if multiple exist.
#'
#' @param x The vector to consolidate.
#' @param sep The separator for concatenation if multiple unique values are found.
#'
#' @return A single value (original type, string, or `NA`).
#'

merge_row_values <- function(x, sep = "; ") {
  unique_vals <- unique(x[!is.na(x)])
  if (length(unique_vals) == 0) return(NA)
  if (length(unique_vals) == 1) return(unique_vals)
  return(paste(unique_vals, collapse = sep))
}


# ------------------------------------------------------------------------------------------------------------------------

#' Find a Column Name Using Synonyms
#'
#' @description
#' Finds a column in a list of available columns using a flexible specification that can include a primary header and
#' a list of synonyms.
#'
#' @param name_spec A character string or a list with `$header` and `$synonyms` elements defining the names to search for.
#' @param available_cols A character vector of column names to search within.
#'
#' @return The character string of the first matching column name, or `NULL` if no match is found.
#'

resolve_column_name <- function(name_spec, available_cols) {
  all_possible_names <- if (is.list(name_spec)) {
    unlist(c(name_spec$header, name_spec$synonyms))
  } else {
    name_spec
  }
  found_name <- intersect(all_possible_names, available_cols)
  if (length(found_name) > 0) return(found_name[1]) else return(NULL)
}


# ------------------------------------------------------------------------------------------------------------------------

#' Propagate the Master Key Through a Dataset
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
#' @importFrom dplyr cross_join
#'

apply_master_key <- function(dataset, model_config) {
  
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
  
  for (table_name in names(dataset)) {
    if (table_name == key_source_table) next
    
    df <- dataset[[table_name]]
    has_key <- key_name_actual %in% names(df)
    is_shared <- table_name %in% shared_tables
    
    if (is_single_experiment && !has_key) {
      df[[key_name_actual]] <- unique_keys[1]
    } else if (!is_single_experiment && !has_key && is_shared) {
      key_df <- data.frame(!!key_name_actual := unique_keys)
      df <- cross_join(df, key_df)
    } # Add other cases here...
    
    dataset[[table_name]] <- df
  }
  return(dataset)
}


# ------------------------------------------------------------------------------------------------------------------------

#' Process a Sequence of Data Transformation Actions
#'
#' @description
#' The core mapping engine that executes a sequence of declarative actions defined in a YAML rule to transform a data frame.
#'
#' @details
#' This function iterates through a list of actions and dispatches the appropriate logic based on `action$type`.
#' Supported actions include:
#' - **`lookup_and_add`**: Joins data from another table.
#' - **`map_values`**: Translates values using a key-value map.
#' - **`rowwise_transform`**: Applies a formula to generate a new column.
#' - **`concatenate_columns`**: Pastes multiple columns into one.
#' - **`coalesce_columns`**: Finds the first non-NA value across a set of columns.
#' - **`rename_column` / `delete_column`**: Explicit. Basic column manipulation.
#' - **`filter_rows`**: Subsets the data based on column presence.
#' - And others for creating or splitting columns.
#'
#' @param actions A list of action definitions from a YAML rule.
#' @param full_input_df The initial data frame for the rule.
#' @param full_dataset The complete dataset, used for lookups.
#' @param master_key_name The name of the master key, used for default joins.
#' @param ... Optional arguments passed down from `convert_dataset`.
#'
#' @return A single data frame representing the result of all applied actions.
#' 
#' @importFrom dplyr mutate filter select left_join rename if_any all_of coalesce bind_cols
#' @importFrom tibble as_tibble
#' @importFrom rlang parse_expr
#'

process_actions <- function(actions, full_input_df, full_dataset, master_key_name, ...) {
  
  current_df <- full_input_df
  
  # Pass down optional arguments via a list
  action_args <- list(...)
  
  for (action in actions) {
    
    # --- Main action dispatcher ---
    switch(action$type,
           
           "lookup_and_add" = {
             # 1. Get parameters from the YAML rule.
             lookup_section <- action$lookup_data_source$section
             cols_to_select <- action$select_columns
             
             # 2. Determine the join keys (handles implied master key).
             if (is.null(action$on_key)) {
               source_keys <- master_key_name
               lookup_keys <- master_key_name
             } else {
               source_keys <- unlist(action$on_key$source_key)
               lookup_keys <- unlist(action$on_key$lookup_key)
             }
             
             if (all(source_keys %in% names(current_df))) {
               
               # 3. Get the lookup table from the full input dataset.
               lookup_table <- full_dataset[[lookup_section]]
               
               # --- NEW: If the lookup table does not exist, warn the user and skip this action. ---
               if (is.null(lookup_table)) {
                 warning(paste0("Lookup section '", lookup_section,
                                "' not found in the dataset. Creating NA columns and proceeding."),
                         call. = FALSE)
                 if (!is.null(cols_to_select)) {
                   for (new_name in names(cols_to_select)) {
                     current_df[[new_name]] <- NA
                   }
                 }
               } else {
                 
                 # Filter lookup
                 if (!is.null(action$filter_lookup)) {
                   filter_params <- action$filter_lookup
                   col_to_filter <- filter_params$column
                   
                   # Ensure the column to filter on actually exists in the lookup table
                   if (!is.null(col_to_filter) && col_to_filter %in% names(lookup_table)) {
                     
                     # Currently supports 'equals'. You can add more operators here.
                     if (!is.null(filter_params$equals)) {
                       lookup_table <- filter(lookup_table, .data[[col_to_filter]] == filter_params$equals)
                     }
                     
                   } else {
                     warning(paste0("In filter_lookup, column '", col_to_filter, "' not found in lookup section '",
                                    lookup_section, "'. Filter not applied."),
                             call. = FALSE)
                   }
                 }
                 
                 # Apply filters to lookup tables
                 if (!is.null(action$filter)) {
                   if (!is.null(action$filter$on_presence_of_columns)) {
                     resolved_cols <- sapply(action$filter$on_presence_of_columns, resolve_column_name, names(lookup_table))
                     cols_to_check <- unlist(resolved_cols[!sapply(resolved_cols, is.null)])
                     
                     if (length(cols_to_check) > 0) {
                       lookup_table <- filter(lookup_table, if_any(all_of(cols_to_check), ~ !is.na(.)))
                     } else {
                       lookup_table <- lookup_table[0, ]
                     }
                   }
                   # Note: Add here logic for other filter types if needed in future
                 }
                 
                 # Prevent duplicate columns by dropping any column from the lookup table
                 # that already exists in the source table, unless it's a join key.
                 common_cols <- intersect(names(current_df), names(lookup_table))
                 cols_to_drop <- setdiff(common_cols, lookup_keys)
                 if (length(cols_to_drop) > 0) {
                   lookup_table <- select(lookup_table, -all_of(cols_to_drop))
                 }
                 
                 # 4. Perform the join on the current state of the data.
                 joined_df <- left_join(
                   current_df,
                   lookup_table,
                   by = stats::setNames(lookup_keys, source_keys)
                 )
                 
                 # 5. Create the new columns based on the 'select_columns' map.
                 mutated_df <- joined_df
                 if (!is.null(cols_to_select)) {
                   for (new_name in names(cols_to_select)) {
                     col_def <- cols_to_select[[new_name]]
                     
                     if (is.character(col_def)) {
                       if (col_def %in% names(mutated_df)) {
                         mutated_df <- mutate(mutated_df, !!new_name := .data[[col_def]])
                         
                         # --- NEW: Safely remove the original column after renaming ---
                         # This prevents it from causing name collisions in subsequent joins.
                         # We check that the original name is not a key for this join.
                         is_a_key <- col_def %in% source_keys || col_def %in% lookup_keys
                         if (new_name != col_def && !is_a_key) {
                           mutated_df <- select(mutated_df, -all_of(col_def))
                         }
                         # --- END NEW ---
                         
                       } else {
                         mutated_df <- mutate(mutated_df, !!new_name := NA)
                       }
                       
                     } else if (is.list(col_def) && !is.null(col_def$type)) {
                       
                       if (col_def$type == "static") {
                         # Case B: Static value
                         mutated_df <- mutate(mutated_df, !!new_name := col_def$value)
                         
                       } else if (col_def$type == "coalesce_map") {
                         # Case C: Prioritized mapping
                         coalesced_vector <- rep(NA_character_, nrow(mutated_df))
                         
                         for (map_item in col_def$maps) {
                           source_col_name <- map_item$from
                           map_vector <- unlist(map_item$map)
                           
                           # Check if the source column for this specific map exists
                           if (source_col_name %in% names(mutated_df)) {
                             source_vector <- as.character(mutated_df[[source_col_name]])
                             
                             # Perform this map's translation
                             mapped_values <- unname(map_vector[source_vector])
                             
                             # Use the new values to fill in NAs from previous steps
                             coalesced_vector <- coalesce(coalesced_vector, mapped_values)
                           }
                         }
                         mutated_df <- mutate(mutated_df, !!new_name := coalesced_vector)
                         
                       } else if (col_def$type == "map_values") {
                         # Case D: Nested value mapping
                         inputs <- resolve_input_map(col_def$input_map, mutated_df)
                         if (!is.null(inputs)) {
                           source_vector <- inputs[[1]]
                           final_map_vector <- unlist(col_def$map %||% list())
                           
                           data_to_map <- as.character(source_vector)
                           mapped_vector <- unname(final_map_vector[data_to_map])
                           
                           unmapped_indices <- which(!is.na(source_vector) & is.na(mapped_vector))
                           if (length(unmapped_indices) > 0 && !is.null(col_def$default_value)) {
                             mapped_vector[unmapped_indices] <- col_def$default_value
                           }
                           
                           mutated_df <- mutate(mutated_df, !!new_name := mapped_vector)
                         } else {
                           # If the input column for the map isn't found, create an NA column.
                           mutated_df <- mutate(mutated_df, !!new_name := NA)
                         }
                       }
                     }
                   }
                 }
                 
                 # 6. Select final columns and update the working data frame.
                 current_df <- mutated_df
               }
               
             } else {
               # If a key is missing, create NA columns for all expected outputs of this action
               if (!is.null(cols_to_select)) {
                 for (new_name in names(cols_to_select)) {
                   current_df[[new_name]] <- NA
                 }
               }
             }
             
           },
           
           "map_values" = {
             
             unmatched_code <- action_args$unmatched_code %||% "na"
             # 1. Get the single input column using the new helper function.
             # This allows 'map_values' to now handle synonyms.
             inputs <- resolve_input_map(action$input_map, current_df)
             if (is.null(inputs)) {
               break # Skip action if the input column isn't found
             }
             source_vector <- inputs[[1]] # map_values only uses the first input
             
             # 2. Build the dictionary (this logic is preserved from your original code).
             final_map_list <- list()
             if (!is.null(action$identity)) {
               identity_list <- unlist(action$identity)
               identity_map <- stats::setNames(as.list(identity_list), identity_list)
               final_map_list <- c(final_map_list, identity_map)
             }
             if (!is.null(action$map)) {
               final_map_list <- c(final_map_list, action$map)
             }
             if (length(final_map_list) == 0) {
               warning("map_values action has no map or identity keys. Skipping.", call. = FALSE)
               break
             }
             
             final_map_vector <- unlist(final_map_list)
             
             # # 3. Handle case sensitivity (preserved from your original code).
             # is_case_sensitive <- !is.null(action$case_sensitive) && action$case_sensitive == TRUE
             # data_to_map <- as.character(source_vector)
             # if (!is_case_sensitive) {
             #   names(final_map_vector) <- toupper(names(final_map_vector))
             #   data_to_map <- toupper(data_to_map)
             # }
             
             # 4. Perform the mapping (preserved from your original code).
             data_to_map <- as.character(source_vector)
             mapped_vector <- unname(final_map_vector[data_to_map])
             
             # 5. Handle unmapped values (preserved from your original code).
             unmapped_indices <- which(!is.na(source_vector) & is.na(mapped_vector))
             if (length(unmapped_indices) > 0) {
               # 'unmatched_code' is passed down from the main apply_mappings function
               switch(unmatched_code,
                      "pass_through" = {
                        mapped_vector[unmapped_indices] <- source_vector[unmapped_indices]
                      },
                      "default_value" = {
                        if (!is.null(action$default_value)) {
                          mapped_vector[unmapped_indices] <- action$default_value
                        }
                      }
               )
             }
             
             # 6. Add the new column to the working data frame.
             current_df[[action$output_header]] <- mapped_vector
           },
           
           "add_column" = {
             # 1. Conditionally check if the action should run.
             # This checks for the presence of columns in the original source data.
             if (!is.null(action$condition_on_presence_of)) {
               condition_cols <- unlist(action$condition_on_presence_of)
               if (!any(condition_cols %in% names(full_input_df))) {
                 break # If condition is not met, skip this action.
               }
             }
             
             # 2. Add the new column to the current working data frame.
             current_df <- mutate(current_df, !!action$output_header := action$value)
           },
           
           "add_column_conditional" = {
             # 1. Get parameters from the YAML.
             output_name <- action$output_header
             conditions  <- action$conditions
             separator   <- action$separator %||% "; " # Default to "; " if not provided
             
             # 2. Vectorized approach: Create a matrix of results, one column per condition.
             #    'sapply' loops through each condition in the YAML.
             results_matrix <- sapply(conditions, function(cond) {
               
               # Find the data column the condition applies to.
               on_col_name <- resolve_column_name(cond$on_column, names(current_df))
               if (is.null(on_col_name)) return(rep(NA_character_, nrow(current_df)))
               
               col_data <- current_df[[on_col_name]]
               
               # Determine which operator is being used (e.g., 'if_equals').
               operator_keys <- c("if_equals", "if_not_equals", "if_higher_than", "if_higher_equals", "if_lower_than", "if_lower_equals")
               operator <- intersect(operator_keys, names(cond))
               
               # Perform the comparison for the ENTIRE column at once.
               is_met_vector <- switch(
                 operator,
                 "if_equals" = as.character(col_data) == as.character(cond[[operator]]),
                 "if_not_equals" = as.character(col_data) != as.character(cond[[operator]]),
                 "if_higher_than" = as.numeric(col_data) > as.numeric(cond[[operator]]),
                 "if_higher_equals" = as.numeric(col_data) >= as.numeric(cond[[operator]]),
                 "if_lower_than" = as.numeric(col_data) < as.numeric(cond[[operator]]),
                 "if_lower_equals" = as.numeric(col_data) <= as.numeric(cond[[operator]]),
                 rep(FALSE, nrow(current_df)) # Default if no valid operator
               )
               is_met_vector[is.na(is_met_vector)] <- FALSE # Treat NAs in data as not meeting the condition
               
               # Return a vector: where the condition is met, use the 'then_value', otherwise NA.
               ifelse(is_met_vector, cond$then_value, NA_character_)
             })
             
             # 3. Collapse the matrix of results into a single vector.
             # For each row, paste together all the non-NA results from the different conditions.
             if (is.matrix(results_matrix)) {
               # If there were multiple conditions, sapply returns a matrix.
               result_vector <- apply(results_matrix, 1, function(row_values) {
                 paste(na.omit(row_values), collapse = separator)
               })
             } else {
               # If there was only one condition, sapply returns a vector.
               result_vector <- results_matrix
             }
             
             # If a row had no matches, it will be an empty string. Convert it to NA.
             result_vector[result_vector == ""] <- NA_character_
             
             # 4. Add the new column to the working data frame.
             current_df[[output_name]] <- result_vector
           },
           
           "rename_column" = {
             # Check if the input column exists before trying to rename it
             if (action$input_name %in% names(current_df)) {
               current_df <- rename(current_df, !!action$output_name := !!action$input_name)
             } else {
               warning(paste0("Column '", action$input_name, "' not found for renaming. Skipping action."), call. = FALSE)
             }
           },
           
           "delete_column" = {
             # Get the list of columns to delete from the YAML
             cols_to_delete <- unlist(action$columns)
             # Find which of those columns actually exist in the current data frame
             existing_cols_to_delete <- intersect(cols_to_delete, names(current_df))
             
             if (length(existing_cols_to_delete) > 0) {
               current_df <- select(current_df, -all_of(existing_cols_to_delete))
             }
           },
           
           "rowwise_transform" = {
             # 1. Get all necessary input vectors.
             inputs <- resolve_input_map(action$input_map, current_df)
             if (is.null(inputs) || length(inputs) == 0) {
               # If any input column is not found, create an empty NA column and stop.
               current_df[[action$output_header]] <- NA
               break
             }
             
             # 2. Evaluate the formula using a more robust method.
             # Bind the input vectors into a temporary tibble.
             inputs_tbl <- as_tibble(inputs)
             
             # Perform the mutation on the temporary tibble.
             transformed_vector <- mutate(
               inputs_tbl,
               .result = !!rlang::parse_expr(action$formula)
             )$.result
             
             # 3. Add the new column to the working data frame.
             current_df[[action$output_header]] <- transformed_vector
           },
           
           "concatenate_columns" = {
             # 1. Get parameters from the YAML.
             output_name <- action$output_header
             input_map   <- action$input_map
             separator   <- action$separator %||% " "
             on_missing  <- action$on_missing %||% "skip" # Default to 'skip'
             
             # 2. Resolve input columns, respecting the 'on_missing' flag.
             inputs <- list()
             all_found <- TRUE
             for (local_name in names(input_map)) {
               col_spec <- input_map[[local_name]]
               found_col_name <- resolve_column_name(col_spec, names(current_df))
               
               if (is.null(found_col_name)) {
                 all_found <- FALSE
               } else {
                 inputs[[found_col_name]] <- current_df[[found_col_name]]
               }
             }
             
             # 3. Decide whether to proceed based on the 'on_missing' flag.
             if (!all_found && on_missing == "skip") {
               warning(paste0("Skipping concatenate for '", output_name, "' because columns were missing."),
                       call. = FALSE)
               break
             }
             if (length(inputs) < 1) {
               break # Nothing to concatenate
             }
             
             # 4. Perform the concatenation (logic preserved from your original code).
             cols_to_paste <- as.data.frame(inputs)
             pasted_vector <- apply(cols_to_paste, 1, function(row_values) {
               paste(row_values[!is.na(row_values)], collapse = separator)
             })
             pasted_vector[pasted_vector == ""] <- NA_character_
             
             # 5. Add the new column to the working data frame.
             current_df[[output_name]] <- pasted_vector
           },
           
           "coalesce_columns" = {
             output_name <- action$output_header
             input_map <- action$input_map
             
             # Custom input resolution for coalesce:
             # Gather only the columns that are actually found, without failing if some are missing.
             inputs <- list()
             for (local_name in names(input_map)) {
               col_spec <- input_map[[local_name]]
               found_col_name <- resolve_column_name(col_spec, names(current_df))
               if (!is.null(found_col_name)) {
                 # Add the found column's vector to the list of inputs
                 inputs[[found_col_name]] <- current_df[[found_col_name]]
               }
             }
             
             if (length(inputs) < 1) {
               warning(paste0("For '", output_name, "', no valid input columns for coalesce were found. Skipping."), call. = FALSE)
               break
             }
             
             coalesced_vector <- do.call(coalesce, inputs)
             current_df[[output_name]] <- coalesced_vector
           },
           
           # TODO: generalize to various types of strsplits (now only by whole words...)
           "split_and_map_text" = {
             # 1. Get the single input column using the new helper function.
             inputs <- resolve_input_map(action$input_map, current_df)
             if (is.null(inputs)) {
               break # Skip action if the input column isn't found
             }
             source_vector <- inputs[[1]] # The vector of names to be split
             
             # 2. Core logic to split the names into component vectors (preserved from your original code).
             output_cols <- list()
             delimiter <- action$delimiter %||% " "
             
             for (map_item in action$output_map) {
               output_header_name <- map_item$output_header
               component_type <- map_item$component
               
               component_vector <- sapply(source_vector, function(full_name) {
                 if (is.na(full_name)) return(NA_character_)
                 
                 words <- unlist(strsplit(full_name, delimiter))
                 num_words <- length(words)
                 if (num_words == 0) return(NA_character_)
                 
                 switch(component_type,
                        "first" = return(words[1]),
                        "last" = return(words[num_words]),
                        "middle" = {
                          if (num_words > 2) {
                            return(paste(words[2:(num_words - 1)], collapse = " "))
                          } else {
                            return(NA_character_)
                          }
                        },
                        return(NA_character_)
                 )
               }, USE.NAMES = FALSE)
               
               output_cols[[output_header_name]] <- component_vector
             }
             
             # 3. Add the new columns to the working data frame.
             if (length(output_cols) > 0) {
               current_df <- bind_cols(current_df, as_tibble(output_cols))
             }
           },
           
           "filter_rows" = {
             if (!is.null(action$on_presence_of_columns)) {
               # Resolve all column names, correctly handling synonyms.
               resolved_cols <- sapply(action$on_presence_of_columns, resolve_column_name, names(current_df))
               cols_to_check <- unlist(resolved_cols[!sapply(resolved_cols, is.null)])
               
               if (length(cols_to_check) > 0) {
                 # Keep rows that have a non-NA value in AT LEAST ONE of the specified columns.
                 current_df <- filter(current_df, if_any(all_of(cols_to_check), ~ !is.na(.)))
               } else {
                 # If NONE of the specified columns exist, the condition can never be met. Filter out all rows.
                 current_df <- current_df[0, ]
               }
             }
           },
           
           stop(paste("In rule '", mapping_uid, "', unknown or unhandled action type: ", action$type, sep = ""))
    )
    
    # If the filter rule output an empty table, stop processing further rules
    if (nrow(current_df) == 0) {
      break
    }
  }
  return(current_df)
}


# ------------------------------------------------------------------------------------------------------------------------

#' Apply a Full Set of Mapping Rules to a Dataset
#'
#' @description
#' Apply a set of rules set in a YAML file to convert a dataset from a source to a target model.
#'
#' @details
#' This function manages the high-level transformation workflow. It first prepares the data by propagating the master key.
#' It then runs in two phases:
#' 1.  **Phase 1 (Transformation):** Executes all data generation and manipulation rules (e.g., lookups, calculations)
#' via `process_actions()`.
#' 2.  **Phase 2 (Formatting):** Applies all formatting rules (e.g., dates, numbers) to the newly created tables.
#'
#' @param input_data The source dataset as a list of data frames.
#' @param input_model The character name of the source model.
#' @param output_model The character name of the target model.
#' @param config The parsed `datamodels.yaml` configuration object.
#' @param ... Optional arguments to be passed down to `process_actions`.
#'
#' @return The final, mapped dataset as a list of data frames.
#'
#' @importFrom dplyr bind_cols
#' @importFrom tibble tibble as_tibble
#' @importFrom yaml read_yaml
#'
#' @export
#'

apply_mappings <- function(input_data, input_model, output_model, config, ...) {
  
  # --- 1. CONFIGURATION SETUP ---
  output_model_config <- config[[output_model]]
  input_model_config  <- config[[input_model]]
  
  if (input_model_config$master_key == 'none') {
    
    # If master_key is 'none', use the raw input data and skip key processing
    prepared_data <- input_data
    master_key_df <- NULL
    input_master_key_name <- NULL
    
  } else {
    
    # --- 2. PRE-PROCESSING: APPLY MASTER KEY ---
    # This function uses the input_model_config to ensure the key is present in all tables.
    prepared_data <- apply_master_key(input_data, input_model_config)
    
    # --- 3. PREPARE CONTEXT FOR THE LOOP ---
    input_master_key_name <- input_model_config$design_keys[[input_model_config$master_key]]
    output_master_key_name <- output_model_config$design_keys[[output_model_config$master_key]]
    key_source_table  <- input_model_config$key_source_table
    
    # Create the master key data frame for final binding.
    master_key_df <- tibble(
      !!output_master_key_name := prepared_data[[key_source_table]][[input_master_key_name]]
    )
  }
  
  # --- 4. PREPARE FOR MAIN LOOPS ---
  map_path <- output_model_config$mappings[[input_model]]
  map <- yaml::read_yaml(map_path)
  output_data <- list()
  
  phase1_rules <- Filter(function(r) {
    is.null(r$actions) || is.null(r$actions[[1]]$type) || r$actions[[1]]$type != "apply_formats"
  }, map$rules)
  
  for (rule in phase1_rules) {
    
    full_input_df <- prepared_data[[rule$source$section]]
    if (is.null(full_input_df)) next

    # Execute all actions for the rule to get the transformed data
    transformed_df <- process_actions(
      actions = rule$actions,
      full_input_df = full_input_df,
      full_dataset = prepared_data,
      master_key_name = input_master_key_name,
      ...
    )
    
    # Intercept empty dataframe and do not generate output
    if (nrow(transformed_df) == 0) {
      next
    }
    
    # Select and rename the final columns using the map
    final_cols_list <- list()
    if (!is.null(rule$select_final_columns)) {
      rename_map <- rule$select_final_columns
      for (new_name in names(rename_map)) {
        old_name_spec <- rename_map[[new_name]]
        found_col_name <- resolve_column_name(old_name_spec, names(transformed_df))
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
  
  # --- 6. PHASE 2: FINAL FORMATTING LOOP ---
  phase2_rules <- Filter(function(r) !is.null(r$actions[[1]]$type) && r$actions[[1]]$type == "apply_formats", map$rules)
  
  for (rule in phase2_rules) {
    for (action in rule$actions) {
      target_section <- action$target_section
      
      if (!target_section %in% names(output_data)) next
      
      target_df <- output_data[[target_section]]
      
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
      output_data[[target_section]] <- target_df
    }
  }
  
  return(output_data)
}


# ------------------------------------------------------------------------------------------------------------------------

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
  
  mapped_data <- apply_mappings(
    input_data = dataset,
    input_model = input_model,
    output_model = output_model,
    config = config,
    unmatched_code = unmatched_code
  )
  
  # --- 3. POST-PROCESSING ---
  
  # Applies model-specific logic like aggregations and calculations.
  message("Step 2: Structuring and formatting model data...")
  
  # --- Deduplicate (CHECK: redundancy in format_to_model??)
  mapped_data_clean <- lapply(mapped_data, function(df) unique(df))

  # Takes the structured data and writes it to disk in the required format.
  message("Step 3: Writing output files...")
  out <- apply_transformations(dataset = mapped_data_clean, data_model = output_model)
  
  # --- OUTPUT ---
  if (is.list(out) && length(out) == 1) {
    return(out[[1]])  # Single experiment
  } else {
    return(out)  # Multiple experiment
  }
}


# ------------------------------------------------------------------------------------------------------------------------

#' Format ICASA Column Headers
#'
#' @description
#' Swaps column headers between ICASA long 'variable name' format and short 'display code' format.
#'
#' @details
#' This utility function fetches the official ICASA dictionary from the URL specified in the configuration.
#' It then builds a lookup table to rename columns in the dataset to the desired `header_type`.
#'
#' @param dataset The ICASA dataset as a named list of data frames.
#' @param header_type The target header style, either `"short"` or `"long"`.
#' @param config_path The path to the `datamodels.yaml` file.
#'
#' @return The dataset with renamed headers.
#' 
#' @importFrom yaml read_yaml
#' @importFrom dplyr mutate select
#' 
#' @export
#'

format_icasa_headers <- function(dataset, header_type = "short", config_path = "./inst/extdata/datamodels.yaml") {
  
  # --- 1. Load ICASA Dictionary Configuration ---
  config <- yaml::read_yaml(config_path)
  dict_source_config <- config$icasa$dict_source
  
  if (is.null(dict_source_config)) {
    warning("ICASA dictionary source not found in config. Cannot format headers.")
    return(dataset)
  }
  
  # --- 2. Fetch and Prepare Dictionary ---
  header_dict <- NULL
  if (dict_source_config$type == "url_fetch_icasa") {
    # TODO: generic fetch_data_model
    dict <- fetch_icasa(dict_source_config$path)
    header_dict <- dict %>%
      mutate(long_name = Variable_Name, short_name = Code_Display) %>%
      select(long_name, short_name)
  } else {
    warning("Unsupported dictionary type. Cannot format headers.")
    return(dataset)
  }
  
  # --- 3. Create Name Lookup and Rename ---
  name_lookup <- if (header_type == "short") {
    setNames(header_dict$short_name, header_dict$long_name)
  } else {
    setNames(header_dict$long_name, header_dict$short_name)
  }
  
  for (i in seq_along(dataset)) {
    current_names <- names(dataset[[i]])
    # Find which of the current column names exist in our lookup table
    names_to_swap <- intersect(current_names, names(name_lookup))
    
    if (length(names_to_swap) > 0) {
      # Replace only the names that were found
      match_indices <- match(names_to_swap, current_names)
      current_names[match_indices] <- name_lookup[names_to_swap]
      names(dataset[[i]]) <- current_names
    }
  }
  
  return(dataset)
}
