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
      
      # Filter lookup
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
      
      # Apply filters to lookup tables
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
        # Other filter types to add here...
      }
      
      
      available_lookup_cols <- names(lookup_table)
      
      # Critical check: stop if join key missing from lookup table
      missing_keys <- setdiff(lookup_keys, available_lookup_cols)
      if (length(missing_keys) > 0) {
        stop(paste0("The join key(s) '", paste(missing_keys, collapse = ", "),
                    "' were not found in lookup section '", lookup_section, "'. Cannot proceed."), 
             call. = FALSE)
      }
      
      # Non-critical check: warn if requested data columns are missing
      values_to_get <- unique(unlist(cols_to_select))
      missing_select_cols <- setdiff(values_to_get, available_lookup_cols)
      
      if (length(missing_select_cols) > 0) {
        warning(paste0("In 'lookup_and_add', the following columns were not found in lookup section '", 
                       lookup_section, "' and will be skipped: ", 
                       paste(missing_select_cols, collapse = ", ")), 
                call. = FALSE)
      }
      
      # Create a "safe" list of columns that are confirmed to exist
      safe_values_to_get <- intersect(values_to_get, available_lookup_cols)
      safe_cols_to_keep <- unique(c(lookup_keys, safe_values_to_get))
      
      lookup_table <- lookup_table %>%
        dplyr::select(tidyr::all_of(safe_cols_to_keep)) %>%
        dplyr::distinct(dplyr::across(tidyr::all_of(lookup_keys)), .keep_all = TRUE)

      # Perform the join
      joined_df <- dplyr::left_join(
        df,
        lookup_table,
        by = stats::setNames(lookup_keys, source_keys)
      )
      
      # Create new column based on 'select_columns' param
      mutated_df <- joined_df
      if (!is.null(cols_to_select)) {
        for (new_name in names(cols_to_select)) {
          col_def <- cols_to_select[[new_name]]
          
          if (is.character(col_def)) {
            if (col_def %in% names(mutated_df)) {
              mutated_df <- dplyr::mutate(mutated_df, !!new_name := .data[[col_def]])
              
              # Remove original column after renaming to prevent name collisions in subsequent joins
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
                
                if (source_col_name %in% names(mutated_df)) {
                  source_vector <- as.character(mutated_df[[source_col_name]])
                  
                  mapped_values <- unname(map_vector[source_vector])
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
                mutated_df <- dplyr::mutate(mutated_df, !!new_name := NA)
              }
            }
          }
        }
      }
      
      # Select final columns and update the working data frame.
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
  
  unmatched_code <- action$unmatched_code %||% "na"

  # Resolve input maps to handle synonyms
  inputs <- .resolve_input_map(action$input_map, df)
  if (is.null(inputs)) {
    return(df)
  }
  source_vector <- inputs[[1]] # map_values only uses the first input
  
  # Build the values dictionary
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
    unmapped_values <- unique(source_vector[unmapped_indices])
    
    if (unmatched_code == "na") {
    } else if (unmatched_code == "pass_through") {
      mapped_vector[unmapped_indices] <- source_vector[unmapped_indices]
    } else if (unmatched_code == "default_value") {
      default_value <- action$default_value %||% NA
      mapped_vector[unmapped_indices] <- default_value
    } else {
      warning(paste("Unrecognized 'unmatched_code' value:", unmatched_code), call. = FALSE)
    }
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
  
  # Check if conditions is met, skip if not
  if (!is.null(action$condition_on_presence_of)) {
    condition_cols <- unlist(action$condition_on_presence_of)
    if (!any(condition_cols %in% names(df))) {
      return(df)
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
  
  output_name <- action$output_header
  conditions  <- action$conditions
  separator   <- action$separator %||% "; "
  

  results_matrix <- sapply(conditions, function(cond) {
    
    # Resolve condition column
    on_col_name <- .resolve_column_name(cond$on_column, names(df))
    if (is.null(on_col_name)) return(rep(NA_character_, nrow(df)))
    
    col_data <- df[[on_col_name]]
    
    # Determine operator/logic
    operator_keys <- c("if_equals", "if_not_equals", "if_higher_than", "if_higher_equals", "if_lower_than", "if_lower_equals")
    operator <- intersect(operator_keys, names(cond))
    
    is_met_vector <- switch(
      operator,
      "if_equals"      = as.character(col_data) == as.character(cond[[operator]]),
      "if_not_equals"  = as.character(col_data) != as.character(cond[[operator]]),
      "if_higher_than" = as.numeric(col_data) >  as.numeric(cond[[operator]]),
      "if_higher_equals"= as.numeric(col_data) >= as.numeric(cond[[operator]]),
      "if_lower_than"  = as.numeric(col_data) <  as.numeric(cond[[operator]]),
      "if_lower_equals" = as.numeric(col_data) <= as.numeric(cond[[operator]]),
      rep(FALSE, nrow(df))
    )
    # Handle NAs in comparison (treat as constraint not met)
    is_met_vector[is.na(is_met_vector)] <- FALSE
    
    # Resolve 'then value'
    raw_then <- cond$then_value
    final_val_to_use <- raw_then   # Default to literal value
    
    # Check if the config value maps to a column name
    if (!is.null(raw_then) && is.character(raw_then) && length(raw_then) == 1) {
      
      resolved_then_col <- .resolve_column_name(raw_then, names(df))
      if (!is.null(resolved_then_col)) {
        final_val_to_use <- df[[resolved_then_col]]
      }
    }
    
    # Create column
    ifelse(is_met_vector, as.character(final_val_to_use), NA_character_)
  })
  
  # collapse Matrix to Vector (handling multiple conditions)
  if (is.matrix(results_matrix)) {
    result_vector <- apply(results_matrix, 1, function(row_values) {
      paste(na.omit(row_values), collapse = separator)
    })
  } else {
    result_vector <- results_matrix
  }
  
  # Clean up
  result_vector[result_vector == ""] <- NA_character_
  # Fix numeric changed to character during the process
  result_vector <- type.convert(result_vector, as.is = TRUE)
  
  df[[output_name]] <- result_vector
  return(df)
}


#'
#'
#' @noRd
#' 

.action_rename_column <- function(action, df) {
  
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
  
  cols_to_delete <- unlist(action$columns)
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
  
  inputs <- .resolve_input_map(action$input_map, df)
  if (is.null(inputs) || length(inputs) == 0) {
    #df[[action$output_header]] <- NA
    return(df)
  }
  
  # Evaluate the formula in temp table
  inputs_tbl <- as_tibble(inputs)
  
  transformed_vector <- mutate(
    inputs_tbl,
    .result = !!rlang::parse_expr(action$formula)
  )$.result
  
  # Add new column to the working data frame.
  df[[action$output_header]] <- transformed_vector
  
  return(df)
}


#'
#'
#' @noRd
#'


.action_concatenate_columns <- function(action, df) {
  
  output_name <- action$output_header
  input_map   <- action$input_map
  separator   <- action$separator %||% " "
  on_missing  <- action$on_missing %||% "skip" # Default to 'skip'
  
  # Resolve input columns
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
  
  # Decide whether to proceed based on the 'on_missing' flag
  if (!all_found && on_missing == "skip") {
    warning(paste0("Skipping concatenate for '", output_name, "' because columns were missing."),
            call. = FALSE)
    return(df)
  }
  if (length(inputs) < 1) {
    return(df)
  }
  
  # Perform concatenation
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
  source_vector <- inputs[[1]] # Vector of names to be split
  
  # Core logic to split the names into component vectors
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
  
  # Add the new columns to the working data frame
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
    
    # Resolve all column names to handle synonyms
    resolved_cols <- sapply(action$on_presence_of_columns, .resolve_column_name, names(df))
    cols_to_check <- unlist(resolved_cols[!sapply(resolved_cols, is.null)])
    
    if (length(cols_to_check) > 0) {
      # Keep rows that have a non-NA value in at least one of the focal cols
      df <- dplyr::filter(df, dplyr::if_any(tidyr::all_of(cols_to_check), ~ !is.na(.)))
    } else {
      # Filter out all rows if column does not exist or contains only NA
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
          df[[output_col]] <- format(
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
          df[[output_col]] <- sprintf(format_string, as.numeric(df[[col_name]]))
        }
      }
    }
  }
  
  return(df)
}


#' (Action) Convert the data type of one or more columns
#'
#' @noRd
#' 

.action_convert_data_type <- function(action, df) {
  
  col_map <- action$columns
  if (is.null(col_map)) {
    warning("Action 'convert_data_type' called without a 'columns' map. Skipping.", call. = FALSE)
    return(df)
  }
  
  for (col_name in names(col_map)) {
    target_type <- col_map[[col_name]]
    
    resolved_name <- .resolve_column_name(col_name, names(df))
    if (is.null(resolved_name)) {
      warning(paste0("Column '", col_name, "' not found for data type conversion. Skipping."), call. = FALSE)
      next
    }
    
    # Type conversion logic
    if (target_type == "datetime") {
      df[[resolved_name]] <- parse_date_time(
        df[[resolved_name]],
        orders = c(
          "Ymd HMS",  # 2025-11-16 14:45:30
          "Ymd HM",   # 2025-11-16 14:45
          "Ymd",      # 2025-11-16
          "dmY HMS",  # 16-11-2025 14:45:30
          "dmY HM",   # 16-11-2025 14:45
          "mdY HMS",  # 11-16-2025 14:45:30
          "mdY HM",   # 11-16-2025 14:45
          "Y/m/d H:M:S", # 2025/11/16 14:45:30
          "d/m/Y H:M:S", # 16/11/2025 14:45:30
          "m/d/Y H:M:S"  # 11/16/2025 14:45:30
          #"Ymd GMS z", # ISO 8601 with timezone
          #"c"          # Full ISO 8601 format like "2025-11-16T14:45:30.123Z"
        ),
        tz = "UTC", # Always standardize to UTC
        quiet = TRUE # Prevents printing messages for each parsed format
      )
      
    } else if (target_type == "numeric") {
      df[[resolved_name]] <- as.numeric(df[[resolved_name]])
      
    } else if (target_type == "character") {
      df[[resolved_name]] <- as.character(df[[resolved_name]])
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
  
  func_name <- action$function_name
  output_col <- action$output_header
  if (is.null(func_name) || is.null(output_col)) {
    stop("Action 'apply_function' requires 'function_name' and 'output_header'.")
  }
  
  # Find the function in custom function registry
  func_to_call <- .custom_functions[[func_name]]
  if (is.null(func_to_call)) {
    stop(paste("Custom function '", func_name, "' not found in registry.", sep=""))
  }
  
  # Prepare the arguments for the function
  args_list <- action$params %||% list()  # Static parameters
  if (!is.null(action$input_map)) {
    # Resolve input map
    resolved_inputs <- .resolve_input_map(action$input_map, df)
    if (is.null(resolved_inputs)) {
      return(df) 
    }
    # Add the resolved data vectors to arg list
    args_list <- c(args_list, resolved_inputs)
  }
  
  # Function call
  result_vector <- do.call(func_to_call, args_list)
  
  df[[output_col]] <- result_vector
  
  return(df)
}


#'
#'
#' @noRd
#'

.action_pivot_wider <- function(action, df) {
  
  # Resolve columns
  map <- action$input_map
  col_names_source <- .resolve_column_name(map$names_from, names(df))
  col_values_source <- .resolve_column_name(map$values_from, names(df))
  
  if (is.null(col_names_source) || is.null(col_values_source)) {
    warning("pivot_wider: could not find specified columns.")
    return(df)
  }
  
  # Enforce other column preservation/selection based on config
  keep_only <- isTRUE(action$keep_only_pivot_cols)
  
  if (keep_only) {
    df <- df[, c(col_names_source, col_values_source), drop = FALSE]
    id_cols_arg <- NULL
  } else {
    # Keep ID columns (default)
    id_cols_arg <- setdiff(names(df), c(col_names_source, col_values_source))
  }
  
  df_pivoted <- tidyr::pivot_wider(
    data = df,
    id_cols = if(keep_only) NULL else all_of(id_cols_arg),
    names_from = all_of(col_names_source),
    values_from = all_of(col_values_source)
  )
  
  return(df_pivoted)
}


#'
#'
#' @noRd
#' 

.action_pivot_longer <- function(action, df) {
  
  # Resolve columns
  targets_raw <- action$columns_to_pivot
  real_cols_to_pivot <- unlist(lapply(targets_raw, function(x) {
    .resolve_column_name(x, names(df))
  }))
  
  if (length(real_cols_to_pivot) == 0) {
    warning("pivot_longer: No matching columns found to pivot.")
    return(df)
  }
  
  # Set output names
  name_dest <- action$output_map$names_to %||% "name"
  value_dest <- action$output_map$values_to %||% "value"
  
  # Enforce NA handling
  drop_na <- isTRUE(action$values_drop_na)
  
  df_long <- tidyr::pivot_longer(
    data = df,
    cols = all_of(real_cols_to_pivot),
    names_to = name_dest,
    values_to = value_dest,
    values_drop_na = drop_na
  )
  
  return(df_long)
}
