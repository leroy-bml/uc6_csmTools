#' Update a Section in a List with New Data
#'
#' Adds or merges a data frame into a specific element of a list. The function intelligently decides whether to perform
#' a full join or a column-bind.
#'
#' @details
#' This function updates a list (`output_data`) by adding or modifying the data frame within a `target_section`.
#'
#' - If the `target_section` does not exist, it is created.
#' - If the section exists, the function identifies common columns between the existing and new data.
#'   - A `dplyr::full_join()` is performed using these common columns as keys.
#'   - If no common columns exist, a `dplyr::bind_cols()` is performed instead.
#' - It specifically suppresses the default `dplyr` warning for many-to-many relationships and issues a more informative, context-aware warning.
#'
#' @param output_data A list object where each element is typically a data frame.
#' @param target_section A character string; the name of the list element to add or update.
#' @param data_to_add A data frame containing the data to be added or merged.
#'
#' @return The `output_data` list with the specified section updated.
#'
#' @importFrom dplyr full_join bind_cols
#'

update_output_data <- function(output_data, target_section, data_to_add) {
  if (is.null(output_data[[target_section]])) {
    output_data[[target_section]] <- data_to_add
    return(output_data)
  }
  existing_df <- output_data[[target_section]]
  join_keys <- intersect(names(existing_df), names(data_to_add))
  
  if (length(join_keys) > 0) {
    # Using withCallingHandlers is the correct way to intercept and muffle a specific warning.
    output_data[[target_section]] <- withCallingHandlers(
      expr = {
        # This is the code that might produce the warning
        dplyr::full_join(existing_df, data_to_add, by = join_keys)
      },
      warning = function(w) {
        # This function handles the warning if it occurs
        if (grepl("many-to-many relationship", w$message)) {
          # Issue our own, more informative warning
          warning(paste0("A many-to-many join occurred in section: '", target_section, "'.\nThis may be expected of management regimes comprising of multiple events."), call. = FALSE)
          # Suppress the original, verbose warning from being printed
          invokeRestart("muffleWarning")
        }
      }
    )
  } else {
    output_data[[target_section]] <- dplyr::bind_cols(existing_df, data_to_add)
  }
  return(output_data)
}


#' Aggregate a Vector to a Single Value
#'
#' Consolidates a vector into a single value. If only one unique non-NA value exists, it is returned as is. 
#' if multiple unique values exist, they are pasted into a single character string.
#'
#' @details
#' This is a helper function often used inside `dplyr::summarise()` to aggregate character or factor columns.
#' The logic is as follows:
#'
#' 1.  All `NA` values are removed from the input vector `x`.
#' 2.  The unique values are identified.
#' 3.  If there is exactly one unique value, it is returned in its original data type.
#' 4.  If there are multiple unique values, they are collapsed into a single
#'     string, separated by `sep`.
#' 5.  If the input contains no non-NA values, `NA` is returned.
#'
#' @param x A vector to consolidate.
#' @param sep A character string separator used for concatenation if multiple unique values are found. Defaults to `"; "`.
#'
#' @return A single value, which can be of the original data type (for a single unique value), a character string
#' (for multiple unique values), or `NA`.
#'

merge_row_values <- function(x, sep = "; ") {
  unique_vals <- unique(x[!is.na(x)])
  if (length(unique_vals) == 0) return(NA)
  if (length(unique_vals) == 1) return(unique_vals)
  return(paste(unique_vals, collapse = sep))
}


#' Find a Column Name from a Set of Possible Names
#'
#' Searches for a matching name in a vector of available column names, based on a flexible specification that can include
#' a primary name and synonyms.
#'
#' @details
#' This function resolves a desired column name against a list of existing columns. It provides a flexible way to find a 
#' column when its name might vary.
#'
#' - The `name_spec` argument defines the set of names to search for.
#' - If `name_spec` is a list, it is expected to have elements like `$header` (the canonical name) and `$synonyms`
#'   (a vector of alternative names). All are pooled together for the search.
#' - If `name_spec` is a character vector, it is used directly as the set of possible names.
#' - The function returns the *first* match found in `available_cols`.
#'
#' @param name_spec A character vector of possible names, or a list with `$header` and/or `$synonyms` elements.
#' @param available_cols A character vector of column names to search within (e.g., from `colnames(df)`).
#'
#' @return A character string of the first matching column name, or `NULL` if no match is found.
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


#' Process a Series of Data Transformation Actions
#'
#' @description
#' Acts as a dispatcher that iterates through a list of declarative actions, applying data transformations, mappings,
#' and routing logic based on each action's type.
#'
#' @details
#' This function is the core engine of a rule-based processing pipeline. It sequentially executes each action defined in
#' the `actions` list. The behavior is determined by the `action$type`.
#'
#' The supported action types are:
#' - **`pass_through`**: Copies the `source_vector` or other specified columns directly to the output.
#' - **`map_values`**: Translates values from the `source_vector` using a key-value map defined in `action$map`.
#' - **`add_column`**: Creates a new column filled with a static value from `action$value`.
#' - **`add_column_conditional`**: Creates a new column, placing a value from `action$value` only where the `source_vector`
#'   meets a specified condition.
#' - **`rowwise_transform`**: Applies a formula (`action$formula`) to one or more input columns to generate a new output column.
#'   Uses `rlang` for evaluation.
#' - **`split_and_map_text`**: Parses a text vector (e.g., full names), splitting by a delimiter and extracting components like "first", "last",
#'   or "middle" into separate output columns.
#' - **`conditional_routing`**: A meta-action that routes subsets of the input data to different target sections 
#'   based on conditions. It can apply sub-actions (`pass_through`, `map_values`, etc.) within each route. This
#'   action updates `output_data` directly.
#'
#' After each action, key columns from `key_df` are appended to the result before it is merged into the final `output_data` list.
#'
#' @param actions A list where each element is a list defining a single action (e.g., with `type`, `value`, `formula` fields).
#' @param source_vector The primary data vector (a column) that most actions operate upon.
#' @param primary_header The canonical name of the source column.
#' @param found_column_name The actual column name found in the input data frame that corresponds to the `source_vector`.
#' @param full_input_df The complete source data frame, required for actions needing access to multiple columns.
#' @param target_spec A list defining the default output section for the results.
#' @param key_df A data frame of key columns (e.g., IDs) to be joined to the results to maintain data relationships. Can be `NULL`.
#' @param output_data The list object that accumulates all generated data frames.
#'
#' @return The updated `output_data` list containing data frames generated by all processed actions.
#'
#' @importFrom rlang parse_expr
#' @importFrom dplyr mutate distinct bind_cols
#'

process_actions <- function(actions, source_vector, primary_header, found_column_name, full_input_df, target_spec, key_df, output_data) {
  # Add rlang to your library calls at the top of the script: library(rlang)
  
  for (action in actions) {
    result_data <- NULL
    
    # --- Robust logic to determine the temporary output column name ---
    output_col_name <- NULL
    # Actions that create new columns MUST define their name via output_header.
    if (action$type %in% c("add_column", "add_column_conditional", "summary_transform", "rowwise_transform")) {
      output_col_name <- action$output_header
      if (is.null(output_col_name)) {
        stop(paste("Action type '", action$type, "' in rule for source '", primary_header, "' requires an 'output_header' field.", sep=""))
      }
    } else {
      # Simpler actions (pass_through, map_values) default to the primary header from the rule's source.
      output_col_name <- if (!is.null(action$output_header)) action$output_header else primary_header
    }
    
    # --- Main action dispatcher ---
    switch(action$type,
           "pass_through" = {
             if (!is.null(action$headers)) {
               data_to_pass <- list()
               for (col_def in action$headers) {
                 found_name <- resolve_column_name(col_def, names(full_input_df))
                 if (!is.null(found_name)) {
                   primary_name <- if (is.list(col_def)) col_def$header else col_def
                   data_to_pass[[primary_name]] <- full_input_df[[found_name]]
                 }
               }
               if (length(data_to_pass) > 0) result_data <- as.data.frame(data_to_pass)
             } else {
               result_data <- data.frame(source_vector)
             }
           },
           "map_values" = {
             map_list <- unlist(action$map)
             result_data <- data.frame(unname(map_list[as.character(source_vector)]))
           },
           "add_column" = {
             result_data <- data.frame(rep(action$value, nrow(full_input_df)))
             if (is.null(key_df)) result_data <- dplyr::distinct(result_data)
           },
           "add_column_conditional" = {
             condition_met <- source_vector == action$condition$value
             new_vector <- rep(NA_character_, length(source_vector))
             new_vector[which(condition_met)] <- action$value
             result_data <- data.frame(new_vector)
             if (is.null(key_df)) result_data <- dplyr::distinct(result_data)
           },
           
           "rowwise_transform" = {
             resolved_map <- lapply(action$input_map, resolve_column_name, names(full_input_df))
             if (any(sapply(resolved_map, is.null))) { warning("Skipping rowwise_transform due to missing columns.", call. = FALSE); next }
             temp_df <- full_input_df[, unlist(resolved_map), drop = FALSE]
             names(temp_df) <- names(resolved_map)
             
             if (!is.null(action$formula)) {
               transformed_df <- dplyr::mutate(temp_df, .result = !!rlang::parse_expr(action$formula))
               result_data <- transformed_df[, ".result", drop = FALSE]
             }
           },
           
           "split_and_map_text" = {
             if (is.null(source_vector)) {
               result_data <- NULL
             } else {
               # This version assumes the source_vector contains one name per element (per row).
               output_cols <- list()
               delimiter <- if (!is.null(action$delimiter)) action$delimiter else " "
               
               # Process each component requested in the YAML's output_map
               for (map_item in action$output_map) {
                 output_header_name <- map_item$output_header
                 component_type <- map_item$component
                 
                 # Use sapply to iterate over each name in the source vector
                 component_vector <- sapply(source_vector, function(person_name) {
                   # Handle cases where a name might be missing (NA)
                   if (is.na(person_name)) return(NA_character_)
                   
                   words <- unlist(strsplit(person_name, delimiter))
                   num_words <- length(words)
                   if (num_words == 0) return(NA_character_)
                   
                   switch(component_type,
                          "first" = return(words[1]),
                          "last" = return(words[num_words]),
                          "middle" = {
                            if (num_words > 2) {
                              return(paste(words[2:(num_words - 1)], collapse = " "))
                            } else {
                              return(NA_character_) # No middle name if there are only 1 or 2 words
                            }
                          },
                          return(NA_character_) # Default case
                   )
                 }, USE.NAMES = FALSE)
                 
                 output_cols[[output_header_name]] <- component_vector
               }
               
               result_data <- as.data.frame(output_cols, stringsAsFactors = FALSE)
             }
           },
           
           "conditional_routing" = {
             if (!is.null(action$routes)) {
               for (route in action$routes) {
                 # Check conditions to determine which rows from the input apply to this route
                 resolved_condition_cols <- sapply(route$condition_on_presence_of, resolve_column_name, names(full_input_df))
                 condition_cols <- unique(unlist(resolved_condition_cols[!sapply(resolved_condition_cols, is.null)]))
                 if (length(condition_cols) == 0) next
                 
                 presence_matrix <- !is.na(full_input_df[, condition_cols, drop = FALSE])
                 row_indices <- which(rowSums(presence_matrix, na.rm = TRUE) > 0)
                 if (length(row_indices) == 0) next
                 
                 # Initialize a new data frame to hold the results for this specific route
                 route_data_output <- data.frame(row.names = 1:length(row_indices))
                 
                 # Process the columns defined within this route's map_columns
                 if (!is.null(route$map_columns)) {
                   for (col_def in route$map_columns) {
                     
                     # --- NEW PARSING LOGIC START ---
                     # This new block correctly handles all 3 ways a column can be defined.
                     source_header_spec <- NULL
                     sub_action <- NULL
                     if (is.list(col_def)) {
                       if (!is.null(col_def$action)) {
                         # Case 1: A complex transformation with source_header and action
                         source_header_spec <- col_def$source_header
                         sub_action <- col_def$action
                       } else {
                         # Case 2: A header/synonym object for a simple pass-through
                         source_header_spec <- col_def
                         sub_action <- list(type = "pass_through")
                       }
                     } else {
                       # Case 3: A simple string for a pass-through
                       source_header_spec <- col_def
                       sub_action <- list(type = "pass_through")
                     }
                     # --- NEW PARSING LOGIC END ---
                     
                     resolved_header <- resolve_column_name(source_header_spec, names(full_input_df))
                     if (is.null(resolved_header)) next
                     
                     current_rows_df <- full_input_df[row_indices, , drop = FALSE]
                     source_vec <- current_rows_df[[resolved_header]]
                     
                     result_vec <- NULL
                     # Expanded switch to handle all necessary sub-actions
                     if (sub_action$type == "pass_through") {
                       result_vec <- source_vec
                     } else if (sub_action$type == "map_values") {
                       result_vec <- unname(unlist(sub_action$map)[as.character(source_vec)])
                     } else if (sub_action$type == "rowwise_transform" && !is.null(sub_action$formula)) {
                       # Handle rowwise_transform sub-action correctly
                       resolved_map <- lapply(sub_action$input_map, resolve_column_name, names(current_rows_df))
                       if (any(sapply(resolved_map, is.null))) {
                         warning(paste("Skipping rowwise_transform for", resolved_header, "in conditional_routing due to missing columns."), call. = FALSE)
                         next
                       }
                       temp_df <- current_rows_df[, unlist(resolved_map), drop = FALSE]
                       names(temp_df) <- names(resolved_map)
                       transformed_df <- dplyr::mutate(temp_df, .result = !!rlang::parse_expr(sub_action$formula))
                       result_vec <- transformed_df$.result
                     }
                     
                     # Determine the final column name
                     output_col_name_sub <- if(is.list(col_def) && !is.null(col_def$action$output_header)) {
                       col_def$action$output_header
                     } else if(is.list(source_header_spec)) {
                       source_header_spec$header
                     } else {
                       source_header_spec
                     }
                     
                     if(!is.null(result_vec)) {
                       route_data_output[[output_col_name_sub]] <- result_vec
                     }
                   }
                 }
                 
                 # If data was generated for this route, add the keys and join it to the main output list
                 if (ncol(route_data_output) > 0) {
                   result_branch <- route_data_output
                   if (!is.null(key_df)) {
                     keys_branch <- key_df[row_indices, , drop = FALSE]
                     # Ensure keys are not re-added if they were passed through in map_columns
                     keys_to_add <- setdiff(names(keys_branch), names(result_branch))
                     if (length(keys_to_add) > 0) {
                       result_branch <- dplyr::bind_cols(keys_branch[, keys_to_add, drop=FALSE], result_branch)
                     }
                   }
                   output_data <- update_output_data(output_data, route$target_section, result_branch)
                 }
               }
             }
             result_data <- NULL # This action type does not return data to the main processing loop
           },
           stop(paste("Unknown or unhandled action type:", action$type))
    )
    
    if (!is.null(result_data)) {
      # Only rename the column if it's a single column AND output_col_name is set.
      # This prevents it from nullifying names set within certain actions (like pass_through).
      if (ncol(result_data) == 1 && !is.null(output_col_name)) {
        names(result_data) <- output_col_name
      }
      final_target_section <- if (!is.null(action$target_section)) action$target_section else target_spec$section
      
      # MODIFIED LOGIC: Always add keys if they exist, regardless of target section.
      if (!is.null(key_df)) {
        key_cols_to_add <- setdiff(names(key_df), names(result_data))
        if(length(key_cols_to_add) > 0) {
          result_data <- dplyr::bind_cols(key_df[, key_cols_to_add, drop = FALSE], result_data)
        }
      }
      
      output_data <- update_output_data(output_data, final_target_section, result_data)
    }
  }
  return(output_data)
}


#' Apply a Full Set of Mapping and Formatting Rules to Data
#'
#' @description
#' Orchestrates a multi-phase data transformation process. It reads a set of rules from a YAML file to process, reshape,
#' and format a list of input data frames into a structured final output.
#'
#' @details
#' The function operates in three distinct phases, driven by rules defined in a YAML mapping file (`map_path`).
#'
#' 1.  **Phase 1: Data Transformation**
#'     - Iterates through the mapping rules, skipping any header-specific rules.
#'     - For each rule, it identifies the source data and key columns.
#'     - It calls `process_actions()` to perform the core data generation and transformation, accumulating results in an intermediate list.
#'
#' 2.  **Phase 2: Schema Application & Header Mapping**
#'     - Re-iterates through the rules, this time processing only actions of type `map_headers`.
#'     - Within each targeted data frame, it performs a series of schema clean-up operations:
#'       - **Renaming**: Renames columns based on a `rename_list`.
#'       - **Merging**: Automatically finds and merges any duplicate columns that may result from renaming, using `merge_row_values()`.
#'       - **Formatting**: Applies `sprintf()` or date formatting to specified columns.
#'
#' 3.  **Phase 3: Final Header Swap**
#'     - If `header_type` is set to `"short"`, it performs a final pass over all output data frames.
#' 
#' @importFrom yaml read_yaml
#' 
#' @export
#' 

apply_mappings <- function(input_data, map_path, header_dict, header_type = "long") {
  map <- yaml::read_yaml(map_path)
  output_data <- list()
  
  # --- Phase 1: Data Transformation ---
  for (rule in map) {
    # Skip header mapping rules in this first pass
    if (!is.null(rule$actions[[1]]$type) && rule$actions[[1]]$type == "map_headers") next
    
    in_section <- rule$source$section
    in_df <- input_data[[in_section]]
    if (is.null(in_df)) next
    
    # Always get the keys if they exist for any rule type.
    key_df <- NULL
    if (!is.null(rule$source$keys)) {
      missing_keys <- setdiff(rule$source$keys, names(in_df))
      if (length(missing_keys) > 0) {
        warning(
          paste0("Rule ", rule$mapping_uid,": Skipping because key column(s) '", 
                 paste(missing_keys, collapse=", "), "' not found in source section '", in_section, "'."),
          call. = FALSE
        )
        next
      }
      key_df <- in_df[, rule$source$keys, drop = FALSE]
    }
    
    # Get the primary source column ONLY if it's a single-column rule.
    source_vector <- NULL
    primary_header <- NULL
    found_column_name <- NULL
    if (!is.null(rule$source$header)) {
      primary_header <- if(is.list(rule$source$header)) rule$source$header$header else rule$source$header
      found_column_name <- resolve_column_name(rule$source$header, names(in_df))
      
      if (is.null(found_column_name)) {
        # This rule's main header is not in the data, so skip.
        next
      }
      source_vector <- in_df[[found_column_name]]
    }
    
    # Call the processor with all necessary components
    output_data <- process_actions(
      actions = rule$actions,
      source_vector = source_vector,
      primary_header = primary_header,
      found_column_name = found_column_name,
      full_input_df = in_df,
      target_spec = rule$target,
      key_df = key_df,
      output_data = output_data
    )
  }
  
  # --- Phase 2: Schema Application & Header Mapping ---
  for (rule in map) {
    if (is.null(rule$actions[[1]]$type) || rule$actions[[1]]$type != "map_headers") next
    
    for(action in rule$actions) {
      target_df <- output_data[[action$target_section]]
      if (is.null(target_df)) next
      
      # Step 1: Apply all renames from the list
      if (!is.null(action$rename_list)) {
        for (rename_item in action$rename_list) {
          from_name <- resolve_column_name(rename_item$from, names(target_df))
          if (!is.null(from_name)) {
            names(target_df)[names(target_df) == from_name] <- rename_item$to
          }
        }
      }
      
      # Step 2: Automatically merge any duplicate columns
      all_names <- names(target_df)
      dup_names <- unique(all_names[duplicated(all_names)])
      if (length(dup_names) > 0) {
        unique_cols_df <- target_df[, !all_names %in% dup_names, drop = FALSE]
        for (d_name in dup_names) {
          dup_cols <- target_df[, all_names == d_name, drop = FALSE]
          merged_vector <- apply(dup_cols, 1, merge_row_values)
          unique_cols_df[[d_name]] <- merged_vector
        }
        target_df <- unique_cols_df
      }
      
      # Step 3: Apply formatting
      if (!is.null(action$format_map)) {
        for (col_name in names(action$format_map)) {
          if (!col_name %in% names(target_df)) next
          
          format_string <- action$format_map[[col_name]]
          
          if (grepl("[YmdBb]", format_string)) {
            target_df[[col_name]] <- format(as.Date(target_df[[col_name]]), format_string)
          } else {
            target_df[[col_name]] <- sprintf(format_string, as.numeric(target_df[[col_name]]))
          }
        }
      }
      
      output_data[[action$target_section]] <- target_df
    }
  }
  
  # --- Final Step: Swap to Short Headers if Requested ---
  if (header_type == "short") {
    name_lookup <- setNames(header_dict$short_name, header_dict$long_name)
    
    for (i in seq_along(output_data)) {
      current_names <- names(output_data[[i]])
      names_to_swap <- intersect(current_names, names(name_lookup))
      
      if (length(names_to_swap) > 0) {
        match_indices <- match(names_to_swap, current_names)
        current_names[match_indices] <- name_lookup[names_to_swap]
        names(output_data[[i]]) <- current_names
      }
    }
  }
  
  return(output_data)
}


#' Map a Dataset from a Source Model to a Target Model
#'
#' @description
#' A top-level wrapper function that configures and initiates a data mapping process. It selects the appropriate mapping rules and
#' data dictionaries based on the specified input and output models.
#'
#' @details
#' This function serves as the primary entry point for converting a dataset from one data model to another.
#' Its main responsibilities are:
#'
#' 1.  **Configuration**: Based on the `output_model` (currently implemented for `"icasa"`), it fetches the required data
#'     dictionary (e.g., via `fetch_icasa()`) and prepares a header lookup table.
#' 2.  **Rule Selection**: It uses the `input_model` to select the correct YAML file (`.yaml`) containing the specific transformation rules.
#' 3.  **Execution**: It passes the dataset and the prepared configuration to the `apply_mappings()` engine, which executes
#'     the transformation.
#'
#' @param dataset The input dataset to be transformed, structured as a named list of data frames.
#' @param input_model A character string identifying the source data model (e.g., `"bonares-lte_de"`).
#' @param output_model A character string identifying the target data model (e.g., `"icasa"`).
#' @param header_type A character string specifying the final header format (`"long"` or `"short"`).
#'
#' @return A named list of data frames, representing the dataset transformed into the `output_model` format.
#'
#' @importFrom dplyr mutate select
#' 
#' @export
#'

map_data2 <- function(dataset, input_model, output_model, header_type) {
  
  if (output_model == "icasa") {
    
    icasa_csv_url <- "https://raw.githubusercontent.com/DSSAT/ICASA-Dictionary/main/CSV/"
    dict <- fetch_icasa(icasa_csv_url)
    
    header_dict <- dict %>%
      mutate(long_name = Variable_Name, short_name = Code_Display) %>%
      select(var_uid, long_name, short_name)
    
    map_path <- switch(input_model,
                       "bonares-lte_de" = "./inst/extdata/map_bonares_icasa_v1.yaml",
                       "dssat" = "xxx",  # Placeholder
                       "agro" = "xxxx",
                       NULL  # Default if none match
    )
  }
  apply_mappings(input_data, map_path, header_dict, header_type = "short")
}