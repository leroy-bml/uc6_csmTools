#' Test if Two Vectors are Identical (with NA Handling and Normalization)
#'
#' This function compares two vectors to determine if they are functionally identical.
#' The comparison includes handling of `NA` values and standardize values (e.g., by trimming whitespace)
#' before comparison.
#'
#' @param a The first vector to compare.
#' @param b The second vector to compare.
#'
#' @return Logical. `TRUE` if the vectors are identical in length, content, and `NA` positions, and `FALSE` otherwise.
#'
#' @details
#' The function operates with the following logic:
#'
#' * It first checks if the vectors have different lengths. If so, it returns `FALSE` immediately.
#' * It then handles the special case where both vectors are entirely composed of `NA` values, returning `TRUE`.
#' * The content of both vectors are then standardized (i.e., by trimming leading/trailing whitespace and coercing to character type).
#' * It performs an element-wise comparison. An `NA` in one vector is considered a match if and only if the other vector also has an `NA` in the same position.
#' * If any element-wise comparison results in `FALSE` (and is not an `NA`), the function returns `FALSE`. Otherwise, it returns `TRUE`.
#'
#' This function is useful for robustly comparing columns in a data frame, where minor formatting differences
#' (like whitespace) should be ignored and `NA` values should be treated as matches.
#'

are_identical_cols <- function(a, b) {
  
  # Return FALSE if vectors are of different lengths
  if (length(a) != length(b)) return(FALSE)
  # Return TRUE if both vectors are entirely NA
  if (all(is.na(a)) && all(is.na(b))) return(TRUE)
  
  a <- trimws(as.character(a))
  b <- trimws(as.character(b))
  # Compare elements, treating NA in the same position as equal
  same <- (a == b) | (is.na(a) & is.na(b))
  if (any(!same, na.rm = TRUE)) return(FALSE)
  return(TRUE)
}


#' Fuse Identical Columns in a Data Frame
#'
#' Identifies groups of columns in a data frame that share a common base name (e.g., "Var-x", "Var-y" would have base "Var")
#' and consolidates them based on whether their values are identical or not. It also adds a flag column
#' indicating rows where values across columns within the same base name group are inconsistent.
#'
#' @param df A data frame in which to fuse columns. Expected column names may contain a separator and suffix
#'           (e.g., "variable-suffix").
#' @param sep A character string used as the separator between the base name and suffix in column names (default: "-").
#'            This separator helps in identifying columns belonging to the same logical variable.
#'
#' @return A data frame with columns consolidated.
#'         If columns sharing a base name are identical across all rows, one representative is kept, named after the base name.
#'         If they are not identical, their values are concatenated into a single column, named after the base name.
#'         A `flag_discrepancy` column is added (if discrepancies exist) listing the base names
#'         for rows where values were inconsistent. If no discrepancies are found, `flag_discrepancy` is removed.
#'
#' @details
#' The function processes columns in the input data frame (`df`) by grouping them based on their "base name"
#' (the part of the column name before the first occurrence of `sep`).
#'
#' **Processing steps for each base name group:**
#'
#' 1.  **Discrepancy Flagging:** It first examines all columns within a base name group (e.g., `A`, `A-x`, `A-y`).
#'     For each row, if non-missing values across these columns are not all identical, a discrepancy is noted for that row and base name.
#' 2.  **Column Fusion:**
#'     * If **all** columns within a base name group are strictly identical across all rows (as determined by an internal `are_identical_cols()` helper, also assumed to be available), only the first column in the group is retained, and it is renamed to the `current_base_name`.
#'     * If **any** columns within a base name group are not strictly identical, their row-wise non-NA values are concatenated into a single new column for that base name group. The concatenated values are separated by `" | "`.
#' 3.  **Single Columns:** Columns that do not have any siblings with the same base name (i.e., they are not part of a "group" with a separator-suffixed name) are kept as is.
#' 4.  **Flag Column Generation:** After processing all base name groups, a `flag_discrepancy` column is constructed. For each row, this column lists all base names for which discrepancies were detected (concatenated with ";").
#' 5.  **Flag Column Cleanup:** If, after processing, the `flag_discrepancy` column contains only `NA` values (meaning no discrepancies were found across the entire data frame), the column is removed from the final output.
#'
#' This function relies on the internal helper functions `are_identical_cols()`.
#'

fuse_duplicate_columns <- function(df, sep = "-") {
  
  # Helper function to concatenate values in a row, handling NAs
  combine_values <- function(row_vals, separator) {
    char_vals <- as.character(row_vals)
    non_na_vals <- char_vals[!is.na(char_vals)]
    if (length(non_na_vals) == 0) {
      return(NA_character_)
    } else {
      return(paste(non_na_vals, collapse = separator))
    }
  }
  
  original_col_names <- names(df)
  base_names <- gsub(paste0("(", sep, "[^-]+)+$"), "", original_col_names)
  
  flag_matrix <- matrix(FALSE, nrow = nrow(df), ncol = 0)
  colnames(flag_matrix) <- character(0)
  
  result_cols_list <- list() # This list will build the new dataframe's columns
  processed_base_names_set <- new.env(hash = TRUE) # To efficiently track processed base names
  
  
  for (i in seq_along(original_col_names)) {
    current_base_name <- base_names[i]
    
    # Skip if this base name group has already been processed
    if (exists(current_base_name, envir = processed_base_names_set)) {
      next
    }
    
    # Find all original column indices for this base name
    current_bn_indices <- which(base_names == current_base_name)
    assign(current_base_name, TRUE, envir = processed_base_names_set) # Mark as processed
    
    if (length(current_bn_indices) > 1) { # This is a group of columns (e.g., var-A, var-B, var-C)
      # --- Flagging for row-wise discrepancies within this base name group ---
      # This part is from your original function for discrepancy flagging
      vals_for_flagging <- df[current_bn_indices]
      vals_for_flagging[] <- lapply(vals_for_flagging, function(x) trimws(as.character(x))) # Normalize
      
      flag <- apply(vals_for_flagging, 1, function(row) {
        non_na <- row[!is.na(row)]
        if (length(non_na) <= 1) return(FALSE)
        length(unique(non_na)) > 1
      })
      flag_matrix <- cbind(flag_matrix, setNames(data.frame(flag), current_base_name))
      
      # --- Logic for handling identical vs. non-identical columns ---
      # Check if ALL columns in this base name group are strictly identical
      first_col_data <- df[[current_bn_indices[1]]]
      all_are_strictly_identical <- TRUE
      for (j in 2:length(current_bn_indices)) {
        if (!are_identical_cols(first_col_data, df[[current_bn_indices[j]]])) {
          all_are_strictly_identical <- FALSE
          break
        }
      }
      
      if (all_are_strictly_identical) {
        # If all columns in the group are strictly identical, keep only the first one
        result_cols_list[[current_base_name]] <- first_col_data
      } else {
        # If NOT all columns in the group are strictly identical, concatenate them
        result_cols_list[[current_base_name]] <- apply(df[current_bn_indices], 1, combine_values, separator = " | ")
      }
      
    } else { # This is a single column (e.g., 'ID' or 'SingleVar-X' that has no other 'SingleVar' siblings)
      # Add this column directly to the result list
      result_cols_list[[original_col_names[i]]] <- df[[i]]
    }
  }
  
  # Construct the new dataframe from the collected columns
  res_df <- as.data.frame(result_cols_list)
  
  # --- Build and append the final flag column (renamed for clarity) ---
  if (ncol(flag_matrix) > 0) {
    flag_column_values <- apply(flag_matrix, 1, function(row) {
      flagged <- names(row)[as.logical(row)]
      if (length(flagged) == 0) return(NA_character_)
      paste(flagged, collapse = ";")
    })
    res_df$flag_discrepancy <- flag_column_values # Renamed to reflect its purpose
    # Remove flag column if all its values are NA
    if (all(is.na(res_df$flag_discrepancy))) {
      res_df$flag_discrepancy <- NULL
    }
  }
  
  return(res_df)
}


#' Check Referential Integrity Between Parent and Child Data Frames
#'
#' This function checks if referential integrity holds for specified common columns
#' between a parent data frame and a child data frame. Referential integrity means
#' that all values in the specified columns of the child data frame exist in the
#' corresponding columns of the parent data frame.
#'
#' @param parent_df A data frame considered as the "parent" table. It should contain
#'                  the primary or unique keys that the child table references.
#' @param child_df A data frame considered as the "child" table. It should contain
#'                 foreign keys that reference the parent table.
#' @param cols A character vector specifying the names of the common columns (keys)
#'             to check for referential integrity. These columns must exist in both
#'             `parent_df` and `child_df`.
#'
#' @return A logical vector of the same length as `cols`. Each element is `TRUE` if
#'         all unique, non-NA values in the corresponding column of `child_df` are
#'         present in the unique, non-NA values of the same column in `parent_df`.
#'         Returns `logical(0)` if `cols` is empty.
#'
#' @details
#' The check is performed for each column specified in `cols` independently.
#' For a given column:
#'
#' * It extracts unique, non-NA values from that column in both `parent_df` and `child_df`.
#' * It then verifies if every unique value from the child's column is present within
#'   the unique values of the parent's column.
#' * This is a one-way check: it does not verify if all parent values are present in the child,
#'   nor does it check for orphaned parent records. It strictly checks if child records
#'   have a corresponding parent record.
#'
#' @examples
#' # Example 1: Basic referential integrity check
#' parent_data <- data.frame(
#'   CustomerID = c(101, 102, 103, 104),
#'   Name = c("Alice", "Bob", "Charlie", "David")
#' )
#'
#' child_data <- data.frame(
#'   OrderID = c(1, 2, 3, 4),
#'   CustomerID = c(101, 103, 101, 102),
#'   Amount = c(50, 75, 120, 30)
#' )
#'
#' # Check integrity for 'CustomerID'
#' check_ref_integrity(parent_data, child_data, "CustomerID") # Should be TRUE
#'

check_ref_integrity <- function(parent_df, child_df, cols) {
  if (length(cols) == 0) return(logical(0))
  sapply(cols, function(col) {
    pvals <- unique(na.omit(parent_df[[col]]))
    cvals <- unique(na.omit(child_df [[col]]))
    all(cvals %in% pvals)
  })
}


#' Merge Database Tables by Common Keys
#'
#' Merges a set of `start_tables` (e.g., child tables or tables to be integrated) into a largr collection of
#' `db_tables` (e.g., parent tables or the main database backbone) by their common columns.
#' The merging strategy (`type`) can be "referential" (based on foreign key integrity) or "loose" (based on common values).
#' It handles duplicate columns by fusing identical ones and flags inconsistencies.
#'
#' @param db_tables A named list of data frames representing the entire database or a collection of potential parent tables.
#'                  These tables are the targets into which `start_tables` will be merged.
#' @param start_tables A named list of data frames or a single data frame. These are the tables that will be iteratively
#'                     merged into `db_tables`.
#' @param type Character. The type of merging relationship to use.
#'             \code{"referential"} (default) ensures that child keys exist in the parent,
#'             while \code{"loose"} performs a merge if there's any overlap in common key values.
#' @param drop_keys Logical. If \code{TRUE}, the common join key columns will be dropped from the merged table after merging (default: \code{FALSE}).
#' @param suffixes Character vector of length 2. Suffixes to append to duplicate column names that are not identical (default: \code{c(".x", ".y")}).
#' @param exclude_keys Optional character vector of column names to exclude from being considered or used as join keys.
#'
#' @return A list containing:
#' \itemize{
#'   \item \code{results_df}: A named list of data frames, where each element corresponds to a table from `start_tables`
#'                          after it has been merged with relevant tables from `db_tables`.
#'   \item \code{tables_merged}: A character vector listing the names of all tables from `db_tables` that were successfully
#'                             merged into any of the `start_tables`.
#' }
#'
#' @details
#' The function operates as an iterative merging engine:
#'
#' * **Initialization:** It prepares the `db_tables` and `start_tables` for merging, sorting table names for consistency
#'     and pre-calculating primary keys.
#' * **Iterative Merging Loop:** For each table in `start_tables`, it enters a loop:
#'     * It identifies candidate tables from `db_tables` that have not yet been merged into the current `start_table`.
#'     * **Key Identification:** Based on the `type` parameter:
#'         * If `type = "referential"`, it looks for primary keys in the candidate table that are also present
#'         in the current `start_table` and verifies referential integrity (all child keys exist in parent).
#'         * If `type = "loose"`, it looks for any common columns where values overlap between the candidate table and `start_table`.
#'     * **Exclusion:** Specified `exclude_keys` are removed from the potential join keys.
#'     * **Merging:** If join keys are found, the candidate table is merged into the current `start_table` using `base::merge()`.
#'     * **Duplicate Column Fusion:** After merging, `fuse_duplicate_columns()` is called to handle columns
#'         with identical content but potentially different names (or suffixes from merging). This function also flags inconsistencies if columns with the same root name have different content.
#'     * **Key Dropping:** If `drop_keys` is `TRUE`, the join key columns are removed from the merged table.
#'     * The loop continues for the current `start_table` until no more candidates can be merged into it.
#'
#' This function relies on helper functions `get_pkeys()`, `check_ref_integrity()` (implicitly or explicitly depending on `type`), and `fuse_duplicate_columns()`.
#'

merge_db_tables <- function(db_tables,
                            start_tables,
                            type = c("referential", "loose"),
                            force_keys = NULL,
                            exclude_keys = NULL,
                            drop_keys = FALSE,
                            suffixes = c("-x", "-y")) {
  
  # db_tables = db[roots]
  # start_tables = db
  # type = "referential"
  # force_keys = force_keys
  # exclude_keys = exclude_keys
  # drop_keys = drop_keys
  
  type <- match.arg(type)
  
  db_tables <- db_tables[order(names(db_tables))]
  tbl_names <- names(db_tables)
  all_cols <- lapply(db_tables, colnames)
  all_pkeys <- lapply(db_tables, function(df) get_pkeys(df, alternates = FALSE))
  
  if (!is.list(start_tables)) start_tables <- list(start_tables)
  results <- list()
  all_merged <- character(0)
  start_names <- names(start_tables)
  
  if (is.null(start_names)) {
    start_names <- paste0("start_", seq_along(start_tables))
  }
  
  for (i in seq_along(start_tables)) {

    #print(names(start_tables)[i])
    result_df <- start_tables[[i]]
    joined <- character(0)
    
    repeat {
      candidates <- setdiff(tbl_names, c(joined, start_names[[i]]))
      joined_this_round <- FALSE
      
      for (dim in candidates) {

        jkeys <- character(0)
        
        if (type == "referential") {
          poss <- intersect(all_pkeys[[dim]], names(result_df))
          ok <- check_ref_integrity(db_tables[[dim]], result_df, poss)
          jkeys <- poss[ok]
        } else {
          poss <- intersect(all_cols[[dim]], names(result_df))
          jkeys <- Filter(function(col) {
            v1 <- unique(na.omit(result_df[[col]]))
            v2 <- unique(na.omit(db_tables[[dim]][[col]]))
            all(v1 %in% v2) || all(v2 %in% v1)
          }, poss)
        }
        
        if (!is.null(force_keys) &&
            all(force_keys %in% intersect(names(result_df), names(db_tables[[dim]])))) {
          jkeys <- c(force_keys, jkeys)
        }
        
        if (!is.null(exclude_keys)) {
          jkeys <- setdiff(jkeys, exclude_keys)
        }
        
        if (length(jkeys)) {
          result_df <- merge(result_df,
                             db_tables[[dim]],
                             by = jkeys,
                             all.x = TRUE,
                             all.y = FALSE,
                             suffixes = suffixes)
          
          result_df <- fuse_duplicate_columns(result_df)
          # Drop join keys unless set to be preserved
          if (drop_keys) { result_df[jkeys] <- NULL }
          
          joined <- c(joined, dim)
          all_merged <- union(all_merged, dim)
          joined_this_round <- TRUE
        }
      }
      
      if (!joined_this_round) break
    }
    
    results[[start_names[[i]]]] <- result_df
  }
  
  return(list(results_df = results, tables_merged = all_merged))
}
