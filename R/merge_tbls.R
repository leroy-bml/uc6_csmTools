#' Normalize a character vector by trimming whitespace
#'
#' Converts input to character and removes leading and trailing whitespace.
#'
#' @param x A vector to normalize.
#' 
#' @return A character vector with whitespace trimmed.
#' 
#' @examples
#' normalize("  hello world  ")
#' normalize(c("  a", "b  ", "  c  "))
#' 

normalize <- function(x) trimws(as.character(x))


#' Test if Two Vectors are Identical (with NA Handling and Normalization)
#'
#' Compares two vectors to determine if they are identical, allowing for NA values and normalizing by trimming whitespace and coercing to character.
#'
#' @param a First vector to compare.
#' @param b Second vector to compare.
#'
#' @return Logical. \code{TRUE} if the vectors are identical (including NA positions), \code{FALSE} otherwise.
#'
#' @details
#' The function:
#' \itemize{
#'   \item Returns \code{FALSE} if the vectors are of different lengths.
#'   \item Returns \code{TRUE} if both vectors are entirely \code{NA}.
#'   \item Trims whitespace and coerces both vectors to character.
#'   \item Compares elements, treating \code{NA} in the same position as equal.
#' }
#'
#' @examples
#' are_identical_cols(c("A", "B", NA), c("A", "B", NA)) # TRUE
#' are_identical_cols(c("A", "B", NA), c("A", "C", NA)) # FALSE
#' are_identical_cols(c(NA, NA), c(NA, NA))             # TRUE
#' are_identical_cols(c(" A", "B"), c("A", "B"))        # TRUE
#'
#' @export

are_identical_cols <- function(a, b) {
  
  # Return FALSE if vectors are of different lengths
  if (length(a) != length(b)) return(FALSE)
  # Return TRUE if both vectors are entirely NA
  if (all(is.na(a)) && all(is.na(b))) return(TRUE)
  
  a <- normalize(a)
  b <- normalize(b)
  # Compare elements, treating NA in the same position as equal
  same <- (a == b) | (is.na(a) & is.na(b))
  if (any(!same, na.rm = TRUE)) return(FALSE)
  return(TRUE)
}


#' Fuse Identical Columns in a Data Frame
#'
#' Identifies groups of columns in a data frame that share a common base name (differentiated by a separator and suffix)
#' and fuses (merges) columns that are identical, keeping only one representative per group. 
#' Adds a flag column indicating rows where columns with the same base name are not identical.
#'
#' @param df A data frame in which to fuse identical columns.
#' @param sep A character string used as the separator between the base name and suffix in column names (default: "-").
#'
#' @return A data frame with identical columns fused and a \code{flag} column indicating rows with non-identical values among columns with the same base name. If all rows are unflagged, the \code{flag} column is removed.
#'
#' @details
#' The function:
#' \itemize{
#'   \item Groups columns by their base name (before the separator).
#'   \item For each group, flags rows where the values are not identical across columns.
#'   \item Within each group, keeps only one representative column for each set of identical columns.
#'   \item Renames the kept column to the base name if possible.
#'   \item Adds a \code{flag} column listing base names with non-identical values in each row.
#'   \item Removes the \code{flag} column if all values are \code{NA}.
#' }
#'
#' @examples
#' df <- data.frame(
#'   A = c(1, 2, 3),
#'   A-x = c(1, 2, 3),
#'   A-y = c(1, 2, 4),
#'   B = c("a", "b", "c"),
#'   B-x = c("a", "b", "c")
#' )
#' fuse_identical_columns(df)
#'
#' @export
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
      vals_for_flagging[] <- lapply(vals_for_flagging, normalize) # Use your normalize function here
      
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

#'
#'
#'

check_ref_integrity <- function(parent_df, child_df, cols) {
  if (length(cols) == 0) return(logical(0))
  sapply(cols, function(col) {
    pvals <- unique(na.omit(parent_df[[col]]))
    cvals <- unique(na.omit(child_df [[col]]))
    all(cvals %in% pvals)
  })
}


#' Merge Parent and Child Tables by Common Keys
#'
#' Merges lists of parent and child data frames by their common columns (primary keys or otherwise), according to the specified relationship type.
#' Handles duplicate columns by fusing identical columns and flags inconsistencies. Returns the merged tables and a record of merged child tables.
#'
#' @param parent_list A named list of parent data frames.
#' @param child_list A named list of child data frames.
#' @param type Character. The type of relationship to use for merging: \code{"parent-child"}, \code{"child-parent"}, \code{"bidirectional"}, or \code{"any-common"} (default: all).
#' @param drop_keys Logical. If \code{TRUE}, drops the join key columns after merging (default: \code{TRUE}).
#' @param suffixes Character vector of length 2. Suffixes to append to duplicate column names (default: \code{c("-x", "-y")}).
#' @param exclude_cols Optional character vector of column names to exclude from being used as join keys.
#'
#' @return A named list of merged parent data frames, each with an attribute \code{join_tbls} listing the child tables merged into it.
#'
#' @details
#' The function:
#' \itemize{
#'   \item Identifies common columns between parent and child tables according to the specified relationship type.
#'   \item Skips merging if there are no common columns between a parent and child table.
#'   \item Merges tables by these columns, optionally excluding specified columns.
#'   \item Fuses identical columns and flags inconsistencies using \code{\link{fuse_identical_columns}}.
#'   \item Optionally drops join key columns after merging.
#'   \item Records which child tables were merged into each parent.
#' }
#'
#' @examples
#' # Example usage (assuming get_pkeys and fuse_identical_columns are defined):
#' # parent_list <- list(
#' #   parent = data.frame(ID = 1:3, Value = c("A", "B", "C"))
#' # )
#' # child_list <- list(
#' #   child = data.frame(ID = 1:3, Data = c("X", "Y", "Z"))
#' # )
#' # merge_tbls(parent_list, child_list, type = "parent-child")
#'
#' @importFrom magrittr %>%
#'
#' @export

merge_db_tables <- function(db_tables,
                            start_tables,
                            type = c("referential", "loose"),
                            drop_keys = FALSE,
                            suffixes = c("-x", "-y"),
                            exclude_cols = NULL) {
  
  
  type <- match.arg(type)
  
  db_tables <- db_tables[order(names(db_tables))]
  tbl_names <- names(db_tables)
  all_cols  <- lapply(db_tables, colnames)
  all_pkeys <- lapply(db_tables, function(df) get_pkeys(df, alternates = FALSE))
  
  check_ref <- function(parent_df, child_df, cols) {
    if (length(cols) == 0) return(logical(0))
    sapply(cols, function(col) {
      p <- unique(na.omit(parent_df[[col]]))
      c <- unique(na.omit(child_df[[col]]))
      all(c %in% p)
    })
  }
  
  if (!is.list(start_tables)) start_tables <- list(start_tables)
  results     <- list()
  all_merged  <- character(0)
  start_names <- names(start_tables)
  
  if (is.null(start_names)) {
    start_names <- paste0("start_", seq_along(start_tables))
  }
  
  for (i in seq_along(start_tables)) {
    
    result_df <- start_tables[[i]]
    joined    <- character(0)
    
    repeat {
      candidates       <- setdiff(tbl_names, c(joined, start_names[[i]]))
      joined_this_round <- FALSE
      
      for (dim in candidates) {
        
        jkeys <- character(0)
        
        if (type == "referential") {
          poss <- intersect(all_pkeys[[dim]], names(result_df))
          ok   <- check_ref(db_tables[[dim]], result_df, poss)
          jkeys <- poss[ok]
        } else {
          poss <- intersect(all_cols[[dim]], names(result_df))
          jkeys <- Filter(function(col) {
            v1 <- unique(na.omit(result_df[[col]]))
            v2 <- unique(na.omit(db_tables[[dim]][[col]]))
            all(v1 %in% v2) || all(v2 %in% v1)
          }, poss)
        }
        
        if (!is.null(exclude_cols)) {
          jkeys <- setdiff(jkeys, exclude_cols)
        }
        
        if (length(jkeys)) {
          result_df <- merge(result_df,
                             db_tables[[dim]],
                             by       = jkeys,
                             all.x    = TRUE,
                             all.y    = FALSE,
                             suffixes = suffixes)
          
          result_df <- fuse_duplicate_columns(result_df)
          # Drop join keys unless set to be preserved
          if (drop_keys) { result_df[jkeys] <- NULL }
          
          joined          <- c(joined, dim)
          all_merged      <- union(all_merged, dim)
          joined_this_round <- TRUE
        }
      }
      
      if (!joined_this_round) break
    }
    
    results[[start_names[[i]]]] <- result_df
  }
  
  return(list(results_df = results, tables_merged = all_merged))
}
