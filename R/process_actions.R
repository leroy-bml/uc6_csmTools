#' Process a sequence of data transformation actions
#'
#' @description
#' The core mapping engine that executes a sequence of declarative actions defined
#' in a YAML rule to transform a data frame.
#' Note: scripts for the actions are stored in mapping_actions.R
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
#' - **`split_column`**: Split column into multiple columns
#' - **`add_column`**: (Conditionally) add a single-value attribute (column)
#' - **`add_column_conditional`**: Add column and conditionally populate with set values
#' - TODO: ADD MISSING
#'
#' @param actions A list of action definitions from a YAML rule.
#' @param input_df The initial data frame for the rule.
#' @param dataset The complete dataset, used for lookups.
#' @param ... Optional arguments passed down from `convert_dataset`.
#'
#' @return A single data frame representing the result of all applied actions.
#'
#' @noRd
#'

process_actions <- function(actions, input_df, dataset, ...) {
  
  output_df <- input_df
  
  # Pass down optional arguments via a list
  action_args <- list(...)  # FIGURE OUT NA FOR MAP VALUES
  
  for (action in actions) {
    
    # --- Main action dispatcher ---
    output_df <- switch(action$type,
           
           # --- Generic actions (from mapping_actions_generic.R) ---
           "lookup_and_add" = { 
             .action_lookup_and_add(action, output_df, dataset)
           },
           
           "map_values" = {
             .action_map_values(action, output_df)
           },
           
           "add_column" = {
             .action_add_column(action, output_df)
           },
           
           "add_column_conditional" = {
             .action_add_column_conditional(action, output_df)
           },
           
           "rename_column" = {
             .action_rename_column(action, output_df)
           },
           
           "delete_column" = {
             .action_rename_column(action, output_df)
           },
           
           "rowwise_transform" = {
             .action_rowwise_transform(action, output_df)
           },
           
           "concatenate_columns" = {
             .action_concatenate_columns(action, output_df)
           },
           
           "coalesce_columns" = {
             .action_coalesce_columns(action, output_df)
           },
           
           "split_column" = {
             .action_split_column(action, output_df)
           },
           
           "sort_rows" = {
             .action_sort_rows(action, output_df)
           },
           
           "filter_rows" = {
             .action_filter_rows(action, output_df)
           },
           
           "deduplicate" = {
             .action_deduplicate(action, output_df)
           },
           
           "format_column" = {
             .action_format_column(action, output_df)
           },
           
           "convert_data_type" = {
             .action_convert_data_type(action, output_df)
           },
           
           "summarise" = {
             .action_summarise(action, output_df)
           },
           
           "replace_na" = {
             .action_replace_na(action, output_df)
           },
           
           'apply_function' = {
             .action_apply_function(action, output_df)
           },
           
           # --- Specific actions (from mapping_actions_specific.R) ---
           # TODO: move to apply_function?
           "define_icasa_management_id" = {
             .action_define_icasa_management_id(action, output_df)
           },
           
           stop(paste("In rule '", mapping_uid, "', unknown or unhandled action type: ", action$type, sep = ""))
    )
    
    # If the filter rule output an empty table, stop processing further rules
    if (nrow(output_df) == 0) {
      break
    }
  }
  return(output_df)
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
#' @noRd
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