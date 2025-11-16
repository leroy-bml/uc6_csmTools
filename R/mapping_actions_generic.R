# INPUTS: action, df = current_df, dataset = full_dataset, na_###
# apply_format -> format_column; split_and_map_text -> split_column
# aggregate_by_group -> summarise

#'
#'
#' @noRd
#' 

.action_lookup_and_add <- function(action, df, dataset) {
  
  lookup_section <- action$lookup_data_source$section
  cols_to_select <- action$select_columns
  
  # Identify join keys
  if (is.null(action$on_key)) {
    stop("In 'lookup_and_add', the 'on_key' section defining source and lookup keys must be specified.", call. = FALSE)
  }
  source_keys <- unlist(action$on_key$source_key)
  lookup_keys <- unlist(action$on_key$lookup_key)
  
  if (all(source_keys %in% names(df))) {
    
    lookup_table <- dataset[[lookup_section]]
    if (is.null(lookup_table)) {
      warning(paste0("Lookup section '", lookup_section,
                     "' not found in the dataset. Creating NA columns and proceeding."),
              call. = FALSE)
      if (!is.null(cols_to_select)) {
        for (new_name in names(cols_to_select)) {
          df[[new_name]] <- NA
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
            lookup_table <- dplyr::filter(lookup_table, .data[[col_to_filter]] == filter_params$equals)
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
            lookup_table <- dplyr::filter(lookup_table, dplyr::if_any(tidyr::all_of(cols_to_check), ~ !is.na(.)))
          } else {
            lookup_table <- lookup_table[0, ]
          }
        }
        # Note: Add here logic for other filter types if needed in future
      }
      
      # --- Prepare lookup table for joins ---
      values_to_get <- unique(unlist(cols_to_select))
      cols_to_keep <- unique(c(lookup_keys, values_to_get))
      
      # Check if all required columns  exist in the lookup table
      missing_cols <- setdiff(cols_to_keep, names(lookup_table))
      if (length(missing_cols) > 0) {
        stop(paste("The following columns specified in 'select_columns' or 'on_key' were not found in lookup section '", 
                   lookup_section, "': ", paste(missing_cols, collapse = ", ")), call. = FALSE)
      }
      lookup_table <- lookup_table %>%
        dplyr::select(tidyr::any_of(cols_to_keep)) %>%
        dplyr::distinct(dplyr::across(tidyr::all_of(lookup_keys)), .keep_all = TRUE)

      # Prevent duplicate columns by dropping any column from the lookup table
      # that already exists in the source table, unless it's a join key.
      # common_cols <- intersect(names(df), names(lookup_table))
      # cols_to_drop <- setdiff(common_cols, lookup_keys)
      # if (length(cols_to_drop) > 0) {
      #   lookup_table <- select(lookup_table, -all_of(cols_to_drop))
      # }
      
      # 4. Perform the join on the current state of the data.
      joined_df <- dplyr::left_join(
        df,
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
              mutated_df <- dplyr::mutate(mutated_df, !!new_name := .data[[col_def]])
              
              # Safely remove the original column after renaming
              # This prevents it from causing name collisions in subsequent joins.
              is_a_key <- col_def %in% source_keys || col_def %in% lookup_keys
              if (new_name != col_def && !is_a_key) {
                mutated_df <- dplyr::select(mutated_df, -tidyr::all_of(col_def))
              }
              
            } else {
              mutated_df <- dplyr::mutate(mutated_df, !!new_name := NA)
            }
            
          } else if (is.list(col_def) && !is.null(col_def$type)) {
            
            if (col_def$type == "static") {
              # Case B: Static value
              mutated_df <- dplyr::mutate(mutated_df, !!new_name := col_def$value)
              
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
                  coalesced_vector <- dplyr::coalesce(coalesced_vector, mapped_values)
                }
              }
              mutated_df <- dplyr::mutate(mutated_df, !!new_name := coalesced_vector)
              
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
                
                mutated_df <- dplyr::mutate(mutated_df, !!new_name := mapped_vector)
              } else {
                # If the input column for the map isn't found, create an NA column.
                mutated_df <- dplyr::mutate(mutated_df, !!new_name := NA)
              }
            }
          }
        }
      }
      
      # 6. Select final columns and update the working data frame.
      df <- mutated_df
    }
    
  } else {
    # If a key is missing, create NA columns for all expected outputs of this action
    if (!is.null(cols_to_select)) {
      for (new_name in names(cols_to_select)) {
        df[[new_name]] <- NA
      }
    }
  }
  
  return(df)
}

#'
#'
#'
#' @noRd
#' 

.action_map_values <- function(action, df, ...) {
  
  unmatched_code <- action_args$unmatched_code %||% "na"
  # 1. Get the single input column using the new helper function.
  # This allows 'map_values' to now handle synonyms.
  inputs <- .resolve_input_map(action$input_map, df)
  if (is.null(inputs)) {
    return(df)
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
    return(df)
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
  df[[action$output_header]] <- mapped_vector
  
  return(df)
}


#'
#'
#'
#' @noRd
#' 

.action_add_column <- function(action, df) {
  
  # --- Check if conditions is met ---
  if (!is.null(action$condition_on_presence_of)) {
    condition_cols <- unlist(action$condition_on_presence_of)
    if (!any(condition_cols %in% names(full_input_df))) {
      return(df)  # Skip if not met
    }
  }
  df <- mutate(df, !!action$output_header := action$value)
  
  return(df)
}


#'
#'
#'
#' @noRd
#' 

.action_add_column_conditional <- function(action, df) {
  
  # 1. Get parameters from the YAML.
  output_name <- action$output_header
  conditions  <- action$conditions
  separator   <- action$separator %||% "; " # Default to "; " if not provided
  
  # 2. Vectorized approach: Create a matrix of results, one column per condition.
  #    'sapply' loops through each condition in the YAML.
  results_matrix <- sapply(conditions, function(cond) {
    
    # Find the data column the condition applies to.
    on_col_name <- .resolve_column_name(cond$on_column, names(df))
    if (is.null(on_col_name)) return(rep(NA_character_, nrow(df)))
    
    col_data <- df[[on_col_name]]
    
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
      rep(FALSE, nrow(df)) # Default if no valid operator
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
  df[[output_name]] <- result_vector
  
  return(df)
}


#'
#'
#' @noRd
#' 

.action_rename_column <- function(action, df) {
  
  # Check if the input column exists before trying to rename it
  if (action$input_name %in% names(df)) {
    df <- rename(df, !!action$output_name := !!action$input_name)
  } else {
    warning(paste0("Column '", action$input_name, "' not found for renaming. Skipping action."), call. = FALSE)
  }
  
  return(df)
}


#'
#'
#' @noRd
#' 

.action_delete_column <- function(action, df) {
  
  # Get the list of columns to delete from the YAML
  cols_to_delete <- unlist(action$columns)
  # Find which of those columns actually exist in the current data frame
  existing_cols_to_delete <- intersect(cols_to_delete, names(df))
  
  if (length(existing_cols_to_delete) > 0) {
    df <- select(df, -all_of(existing_cols_to_delete))
  }
  
  return(df)
}


#'
#'
#' @noRd
#'

.action_rowwise_transform <- function(action, df) {
  
  # 1. Get all necessary input vectors.
  inputs <- .resolve_input_map(action$input_map, df)
  if (is.null(inputs) || length(inputs) == 0) {
    # If any input column is not found, create an empty NA column and stop.
    df[[action$output_header]] <- NA
    return(df)
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
  df[[action$output_header]] <- transformed_vector
  
  return(df)
}


#'
#'
#' @noRd
#'


.action_concatenate_columns <- function(action, df) {
  
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
    found_col_name <- .resolve_column_name(col_spec, names(df))
    
    if (is.null(found_col_name)) {
      all_found <- FALSE
    } else {
      inputs[[found_col_name]] <- df[[found_col_name]]
    }
  }
  
  # 3. Decide whether to proceed based on the 'on_missing' flag.
  if (!all_found && on_missing == "skip") {
    warning(paste0("Skipping concatenate for '", output_name, "' because columns were missing."),
            call. = FALSE)
    return(df)
  }
  if (length(inputs) < 1) {
    return(df) # Nothing to concatenate
  }
  
  # --- Perform concatenation ---
  cols_to_paste <- as.data.frame(inputs)
  pasted_vector <- apply(cols_to_paste, 1, function(row_values) {
    paste(row_values[!is.na(row_values)], collapse = separator)
  })
  pasted_vector[pasted_vector == ""] <- NA_character_
  
  df[[output_name]] <- pasted_vector
  
  return(df)
}


#'
#'
#' @noRd
#'

.action_coalesce_columns <- function(action, df) {
  
  output_name <- action$output_header
  input_map <- action$input_map
  
  inputs <- list()
  for (local_name in names(input_map)) {
    col_spec <- input_map[[local_name]]
    found_col_name <- .resolve_column_name(col_spec, names(df))
    if (!is.null(found_col_name)) {
      inputs[[found_col_name]] <- df[[found_col_name]]
    }
  }
  
  if (length(inputs) < 1) {
    warning(paste0("For '", output_name, "', no valid input columns for coalesce were found. Skipping."), call. = FALSE)
    return(df)
  }
  
  coalesced_vector <- do.call(coalesce, inputs)
  df[[output_name]] <- coalesced_vector
  
  return(df)
}


#'
#'
#' @noRd
#'

# TODO: generalize
.action_split_column <- function(action, df) {
  
  inputs <- .resolve_input_map(action$input_map, df)
  if (is.null(inputs)) {
    return(df)
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
    df <- dplyr::bind_cols(df, tibble::as_tibble(output_cols))
  }
  
  return(df)
}


#'
#'
#' @noRd
#' 

.action_sort_rows <- function(action, df) {
  
  sort_col <- action$by_col
  if (sort_col %in% names(df)) {
    df <- df %>%
      dplyr::arrange(!!rlang::sym(sort_col))
  } else {
    warning(paste0("sort_rows: Column '", sort_col, "' not found."), call. = FALSE)
  }
  
  return(df)
}


#'
#'
#' @noRd
#' 

.action_filter_rows <- function(action, df) {
  
  if (!is.null(action$on_presence_of_columns)) {
    # Resolve all column names, correctly handling synonyms.
    resolved_cols <- sapply(action$on_presence_of_columns, .resolve_column_name, names(df))
    cols_to_check <- unlist(resolved_cols[!sapply(resolved_cols, is.null)])
    
    if (length(cols_to_check) > 0) {
      # Keep rows that have a non-NA value in AT LEAST ONE of the specified columns.
      df <- dplyr::filter(df, dplyr::if_any(tidyr::all_of(cols_to_check), ~ !is.na(.)))
    } else {
      # If NONE of the specified columns exist, the condition can never be met. Filter out all rows.
      df <- df[0, ]
    }
  }
  
  return(df)
}


#'
#'
#' @noRd
#' 

.action_format_column <- function(action, df) {
  
  if (!is.null(action$formats)) {
    for (col_name in names(action$formats)) {
      if (col_name %in% names(df)) {
        
        format_details <- action$formats[[col_name]]
        
        # Check if format should overwrite input or create new column
        if (is.list(format_details)) {
          output_col <- format_details$output_header
          format_string <- format_details$format
        } else {
          output_col <- col_name
          format_string <- format_details
        }

        # Set specific formatting logic for dates
        if (grepl("%", format_string)) {
          # df[[col_name]] <- format(as.Date(df[[col_name]]), format_string)
          df[[col_name]] <- format(
            lubridate::parse_date_time(
              df[[col_name]],
              orders = c(
                # Date-only formats
                "dmy", "mdy", "ymd", "Ymd", "dmy HM", "dmy HMS",
                "mdy HM", "mdy HMS", "mdy I:M p", "mdy I:M:S p",
                "ymd HM", "ymd HMS", "Ymd HM", "Ymd HMS",
                "b d, Y", "d b Y", "Y b d", "d-b-Y", "Y-b-d",
                "dmy I:M p", "dmy I:M:S p", "Ymd I:M p", "Ymd I:M:S p",
                # ISO 8601 and compact formats
                "Ymd T", "YmdHMS", "YmdHM",
                # 2-digit year formats
                "dmY", "myY", "Ymd", "dmy", "mdy", "Ym", "dmy I p"
              ),
              tz = "UTC"  # Force UTC to avoid timezone issues
            ), format_string
          )
        } else {
          df[[col_name]] <- sprintf(format_string, as.numeric(df[[col_name]]))
        }
      }
    }
  }
  
  return(df)
}


#'
#'
#' @noRd
#' 

.action_summarise <- function(action, df) {
  
  # Parse grouping columns directly from the rule
  group_cols <- unlist(action$group_by)
  
  # Get safe columns that actually exist in the data
  safe_group_cols <- intersect(group_cols, names(df))
  
  if (length(safe_group_cols) == 0) {
    warning("Aggregation skipped: no grouping columns found.", call. = FALSE)
    return(df) 
  }
  
  # Rest of the aggregation logic remains the same...
  summarise_list <- list()
  for (op in action$summarise) {
    
    if (op$fun %in% c("max", "min", "sum", "mean", "first")) {
      for (col in op$columns) {
        if (col %in% names(df)) {
          output_name <- op$output_header %||% col
          summarise_list[[output_name]] <- switch(
            op$fun,
            "max" = rlang::expr(max(!!rlang::sym(col), na.rm = !!op$na_rm %||% TRUE)),
            "min" = rlang::expr(min(!!rlang::sym(col), na.rm = !!op$na_rm %||% TRUE)),
            "sum" = rlang::expr(sum(!!rlang::sym(col), na.rm = !!op$na_rm %||% TRUE)),
            "mean" = rlang::expr(mean(!!rlang::sym(col), na.rm = !!op$na_rm %||% TRUE)),
            "first" = rlang::expr(dplyr::first(na.omit(!!rlang::sym(col))))
          )
        } else {
          cat("Debug - Column", col, "not found for operation", op$fun, "\n")
        }
      }
    } else if (op$fun == "count_rows") {
      output_name <- op$output_name %||% "row_count"
      summarise_list[[output_name]] <- rlang::expr(dplyr::n())
    }
  }
  
  # Run the aggregation
  df <- df %>%
    dplyr::group_by(dplyr::across(dplyr::all_of(safe_group_cols))) %>%
    dplyr::summarise(!!!summarise_list, .groups = "drop")
  
  return(df)
}


#'
#'
#' @noRd
#' 

.action_replace_na <- function(action, df) {
  cols_to_process <- unlist(action$columns)
  value <- action$with_value
  
  # Find which of the specified columns actually exist in the data frame
  existing_cols <- intersect(cols_to_process, names(df))
  
  if (length(existing_cols) > 0) {
    df <- df %>%
      dplyr::mutate(dplyr::across(
        dplyr::all_of(existing_cols),
        ~ tidyr::replace_na(., value)
      ))
  }
  
  return(df)
}


#'
#'
#' @noRd
#' 

.action_deduplicate <- function(action, df) {
  return(dplyr::distinct(df))
}


#'
#'
#' @noRd
#' 

.action_apply_function <- function(action, df) {
  
  # Get the function name and parameters from the YAML action
  func_name <- action$function_name
  output_col <- action$output_header
  
  # Error handling: check if essential parameters exist
  if (is.null(func_name) || is.null(output_col)) {
    stop("Action 'apply_function' requires 'function_name' and 'output_header'.")
  }
  
  # Find the function in our registry
  func_to_call <- .custom_functions[[func_name]]
  
  # Error handling: check if the function was found in the registry
  if (is.null(func_to_call)) {
    stop(paste("Custom function '", func_name, "' not found in registry.", sep=""))
  }
  
  # Prepare the arguments for the function
  # 1. Start with the static parameters from the YAML (e.g., units, direction)
  args_list <- action$params %||% list()
  
  # 2. Add the data columns specified in the input_map
  if (!is.null(action$input_map)) {
    for (arg_name in names(action$input_map)) {
      col_name <- action$input_map[[arg_name]]
      args_list[[arg_name]] <- df[[col_name]]
    }
  }
  
  # Call the function with the prepared list of arguments
  result_vector <- do.call(func_to_call, args_list)
  
  # Assign the result to the output column in the dataframe
  df[[output_col]] <- result_vector
  
  return(df)
}
