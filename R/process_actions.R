#' Process a Sequence of Data Transformation Actions
#'
#' @description
#' The core mapping engine that executes a sequence of declarative actions defined
#' in a YAML rule to transform a data frame.
#'
#' @details
#' This function iterates through a list of actions and dispatches the appropriate
#' logic based on `action$type`. Supported actions include:
#' - **`lookup_and_add`**: Joins data from another table.
#' - **`map_values`**: Translates values using a key-value map.
#' - **`rowwise_transform`**: Applies a formula to generate a new column.
#' - **`aggregate_by_group`**: Aggregates the data frame using group_by/summarise.
#' - **`concatenate_columns`**: Pastes multiple columns into one.
#' - **`coalesce_columns`**: Finds the first non-NA value across a set of columns.
#' - **`rename_column` / `delete_column`**: Explicit. Basic column manipulation.
#' - **`filter_rows`**: Subsets the data based on column presence.
#' - **`split_and_map_text`**: Split column into multiple columns
#' - **`add_column`**: (Conditionally) add a single-value attribute (column)
#' - **`add_column_conditional`**: Add column and conditionally populate with set values
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
#' @noRd
#'

process_actions <- function(actions, full_input_df, full_dataset, master_key_name, ...) {
  
  current_df <- full_input_df
  
  # Pass down optional arguments via a list
  action_args <- list(...)
  
  for (action in actions) {
    
    # --- Main action dispatcher ---
    switch(action$type,
           
           "lookup_and_add" = {
             
             lookup_section <- action$lookup_data_source$section
             cols_to_select <- action$select_columns
             
             # --- Identify join keys ---
             if (is.null(action$on_key)) {
               source_keys <- master_key_name
               lookup_keys <- master_key_name
             } else {
               source_keys <- unlist(action$on_key$source_key)
               lookup_keys <- unlist(action$on_key$lookup_key)
             }
             
             if (all(source_keys %in% names(current_df))) {
               
               lookup_table <- full_dataset[[lookup_section]]
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
                 
                 # --- Filter lookup ---
                 if (!is.null(action$filter_lookup)) {
                   filter_params <- action$filter_lookup
                   col_to_filter <- filter_params$column
                   
                   if (!is.null(col_to_filter) && col_to_filter %in% names(lookup_table)) {
                     
                     # TODO: add more conditions!
                     if (!is.null(filter_params$equals)) {
                       lookup_table <- filter(lookup_table, .data[[col_to_filter]] == filter_params$equals)
                     }
                     
                   } else {
                     warning(paste0("In filter_lookup, column '", col_to_filter, "' not found in lookup section '",
                                    lookup_section, "'. Filter not applied."),
                             call. = FALSE)
                   }
                 }
                 
                 # --- Apply filters to lookup tables ---
                 if (!is.null(action$filter)) {
                   if (!is.null(action$filter$on_presence_of_columns)) {
                     resolved_cols <- sapply(action$filter$on_presence_of_columns, .resolve_column_name, names(lookup_table))
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
                         inputs <- .resolve_input_map(col_def$input_map, mutated_df)
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
             inputs <- .resolve_input_map(action$input_map, current_df)
             if (is.null(inputs)) {
               break # Skip action if the input column isn't found
             }
             source_vector <- inputs[[1]] # map_values only uses the first input
             
             # 2. Build the dictionary
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
             
             # --- Handle case sensitivity --- Note: drop?
             # is_case_sensitive <- !is.null(action$case_sensitive) && action$case_sensitive == TRUE
             # data_to_map <- as.character(source_vector)
             # if (!is_case_sensitive) {
             #   names(final_map_vector) <- toupper(names(final_map_vector))
             #   data_to_map <- toupper(data_to_map)
             # }
             
             # --- Apply mapping ---
             data_to_map <- as.character(source_vector)
             mapped_vector <- unname(final_map_vector[data_to_map])
             
             # --- Handle unmapped values ---
             unmapped_indices <- which(!is.na(source_vector) & is.na(mapped_vector))
             if (length(unmapped_indices) > 0) {
               # 'unmatched_code' is passed down from apply_mappings
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
             current_df[[action$output_header]] <- mapped_vector
           },
           
           "add_column" = {
             
             # --- Check if conditions is met ---
             if (!is.null(action$condition_on_presence_of)) {
               condition_cols <- unlist(action$condition_on_presence_of)
               if (!any(condition_cols %in% names(full_input_df))) {
                 break  # Skip if not met
               }
             }
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
               on_col_name <- .resolve_column_name(cond$on_column, names(current_df))
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
             inputs <- .resolve_input_map(action$input_map, current_df)
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
               found_col_name <- .resolve_column_name(col_spec, names(current_df))
               
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
             
             # --- Perform concatenation ---
             cols_to_paste <- as.data.frame(inputs)
             pasted_vector <- apply(cols_to_paste, 1, function(row_values) {
               paste(row_values[!is.na(row_values)], collapse = separator)
             })
             pasted_vector[pasted_vector == ""] <- NA_character_
             
             current_df[[output_name]] <- pasted_vector
           },
           
           "coalesce_columns" = {
             output_name <- action$output_header
             input_map <- action$input_map
             
             inputs <- list()
             for (local_name in names(input_map)) {
               col_spec <- input_map[[local_name]]
               found_col_name <- .resolve_column_name(col_spec, names(current_df))
               if (!is.null(found_col_name)) {
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
           
           # TODO: generalize to various types of strsplit (now only by whole words...)
           "split_and_map_text" = {
             inputs <- .resolve_input_map(action$input_map, current_df)
             if (is.null(inputs)) {
               break
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
               resolved_cols <- sapply(action$on_presence_of_columns, .resolve_column_name, names(current_df))
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
           
           "aggregate_by_group" = {
             
             # Parse 'group_by'
             group_cols <- unlist(action$group_by)
             safe_group_cols <- intersect(group_cols, names(current_df))
             
             if (length(safe_group_cols) == 0) {
               warning("Aggregation skipped: no grouping columns found.", call. = FALSE)
               break
             }
             
             # Build the summarisemlogic
             summarise_list <- list()
             for (op in action$summarise) {
               
               # Define the aggregation function based on the YAML
               func <- switch(op$function,
                              "max" = ~max(.x, na.rm = op$na_rm %||% TRUE),
                              "min" = ~min(.x, na.rm = op$na_rm %||% TRUE),
                              "sum" = ~sum(.x, na.rm = op$na_rm %||% TRUE),
                              "mean" = ~mean(.x, na.rm = op$na_rm %||% TRUE),
                              "first" = ~first(.x),
                              stop(paste("Unknown aggregation function:", op$function))
               )
               
               # 
               summarise_list[[op$function]] <- rlang::expr(
                 dplyr::across(
                   dplyr::any_of(!!op$columns),
                   !!func,
                   .names = "{.col}"
                 )
               )
             }
             
             # Run the aggregation
             current_df <- current_df %>%
               group_by(across(all_of(safe_group_cols))) %>%
               summarise(!!!summarise_list, .groups = "drop")
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


#' Resolve an Input Map to a List of Data Vectors
#'
#' @description
#' Resolves a list of column specifications from a YAML rule against a data frame, extracting the corresponding data vectors.
#'
#' @details
#' This helper function is used by actions that require multiple inputs. It iterates through the `input_map`, uses
#' `.resolve_column_name()` to find each column (handling synonyms), and returns a named list of the actual data vectors.
#' It is designed to fail gracefully by returning `NULL` if any specified column is not found.
#'
#' @param input_map A named list from a YAML rule where keys are local names and values are column specifications
#' (e.g., `{x: colA, y: {header: colB}}`).
#' @param df The data frame from which to pull the column vectors.
#'
#' @return A named list of data vectors (e.g., `list(x=c(1,2), y=c(3,4))`), or `NULL` if any column cannot be resolved.
#'
#' @noRd
#'

.resolve_input_map <- function(input_map, df) {
  resolved_inputs <- list()
  
  for (local_name in names(input_map)) {
    col_spec <- input_map[[local_name]]
    found_col_name <- .resolve_column_name(col_spec, names(df))
    
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

.resolve_column_name <- function(name_spec, available_cols) {
  all_possible_names <- if (is.list(name_spec)) {
    unlist(c(name_spec$header, name_spec$synonyms))
  } else {
    name_spec
  }
  found_name <- intersect(all_possible_names, available_cols)
  if (length(found_name) > 0) return(found_name[1]) else return(NULL)
}