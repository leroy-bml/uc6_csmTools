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

fuse_identical_columns <- function(df, sep = "-") {
  # Extract base names from column names (before the separator)
  base_names <- gsub(paste0("(", sep, "[^-]+)+$"), "", names(df))
  flag_matrix <- matrix(FALSE, nrow = nrow(df), ncol = 0)
  colnames(flag_matrix) <- character(0)
  
  for (bn in unique(base_names)) {
    cols <- which(base_names == bn)
    if (length(cols) > 1) {
      vals <- df[cols]
      vals[] <- lapply(vals, normalize)
      # Flag: TRUE if not all non-NA values in the row are identical
      flag <- apply(vals, 1, function(row) {
        non_na <- row[!is.na(row)]
        if (length(non_na) <= 1) return(FALSE)
        length(unique(non_na)) > 1
      })
      flag_matrix <- cbind(flag_matrix, setNames(data.frame(flag), bn))
      
      # Find sets of identical columns
      # We'll use a grouping approach: for each column, assign it to a group of identical columns
      n <- length(cols)
      groups <- rep(NA_integer_, n)
      group_id <- 1
      for (i in seq_len(n)) {
        if (!is.na(groups[i])) next
        groups[i] <- group_id
        for (j in seq_len(n)) {
          if (i == j) next
          if (is.na(groups[j]) && are_identical_cols(vals[[i]], vals[[j]])) {
            groups[j] <- group_id
          }
        }
        group_id <- group_id + 1
      }
      # For each group, keep only the first column
      keep <- logical(n)
      for (g in unique(groups)) {
        idx <- which(groups == g)
        keep[idx[1]] <- TRUE
      }
      # Remove duplicate columns
      df <- df[ , c(setdiff(seq_along(df), cols[!keep])), drop = FALSE]
      # Rename the kept columns to the base name if any of them is the no-suffix column
      kept_cols <- cols[keep]
      kept_names <- names(df)[kept_cols]
      no_suffix_idx <- which(kept_names == bn)
      if (length(no_suffix_idx) == 0) {
        # If no no-suffix column, just keep the first as base name
        names(df)[kept_cols[1]] <- bn
      } else {
        names(df)[kept_cols[no_suffix_idx]] <- bn
      }
      # Update base_names for next iteration
      base_names <- gsub(paste0("(", sep, "[^-]+)+$"), "", names(df))
    }
  }
  
  # Build the flag column
  if (ncol(flag_matrix) > 0) {
    flag <- apply(flag_matrix, 1, function(row) {
      flagged <- names(row)[as.logical(row)]
      if (length(flagged) == 0) return(NA_character_)
      paste(flagged, collapse = ";")
    })
    df$flag <- flag
    # Move flag to last column if it exists
    if ("flag" %in% names(df)) {
      df <- df[ c(setdiff(names(df), "flag"), "flag") ]
    }
    # Remove flag if only NA
    if (all(is.na(df$flag))) {
      df$flag <- NULL
    }
  }
  return(df)
}


#' Merge Parent and Child Tables by Common Keys
#'
#' Merges lists of parent and child data frames by their common columns (primary keys or otherwise), according to the specified relationship type.
#' Handles duplicate columns by fusing identical columns and flags inconsistencies. Returns the merged tables and a record of merged child tables.
#'
#' @param parent_list A named list of parent data frames.
#' @param child_list A named list of child data frames.
#' @param type Character. The type of relationship to use for merging: \code{"parent-child"}, \code{"child-parent"}, or \code{"bidirectional"} (default: all).
#' @param drop_keys Logical. If \code{TRUE}, drops the join key columns after merging (default: \code{TRUE}).
#' @param suffixes Character vector of length 2. Suffixes to append to duplicate column names (default: \code{c("-x", "-y")}).
#' @param exclude_cols Optional character vector of column names to exclude from being used as join keys.
#'
#' @return A list with two elements:
#'   \item{db}{A named list of merged parent data frames, each with an attribute \code{join_tbls} listing the child tables merged into it.}
#'   \item{merged_leaves}{A character vector of names of child tables that were merged.}
#'
#' @details
#' The function:
#' \itemize{
#'   \item Identifies common columns between parent and child tables according to the specified relationship type.
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
#' 

merge_tbls <- function(parent_list, child_list,
                       type = c("parent-child","child-parent","bidirectional"),
                       drop_keys = TRUE,
                       suffixes = c("-x", "-y"),
                       exclude_cols = NULL) {
  
  # Sort parent and child lists by name for reproducibility
  parent_list <- parent_list[order(names(parent_list))]
  parents_cols <- lapply(parent_list, colnames)
  parent_pkeys <- lapply(parent_list, function(df) get_pkeys(df, alternates = FALSE))
  
  child_list <- child_list[order(names(child_list))]
  child_cols <- lapply(child_list, colnames)
  child_pkeys <- lapply(child_list, function(df) get_pkeys(df, alternates = FALSE))
  
  # Prepare output lists
  out_list <- list()
  join_tbls_list <- vector("list", length(parent_list))
  names(join_tbls_list) <- names(parent_list)
  merged_leaves <- character(0)
  
  # Loop over parent tables
  for (i in seq_along(parent_list)) {
    parent_df <- parent_list[[i]]
    # Loop over child tables
    for (j in seq_along(child_list)) {
      # Determine common columns for merging based on relationship type
      if(type == "parent-child"){
        common_cols <- intersect(parent_pkeys[[i]], child_cols[[j]])
      } else if(type == "child-parent"){
        common_cols <- intersect(parents_cols[[i]], child_pkeys[[j]])
      } else if(type == "bidirectional"){
        common_cols <- unique(
          c(intersect(parent_pkeys[[i]], child_cols[[j]]),
            intersect(parents_cols[[i]], child_pkeys[[j]]) )
        )
      }
      # Exclude specified join columns
      if (!is.null(exclude_cols)) {
        common_cols <- setdiff(common_cols, exclude_cols)
      }      
      # If there are common columns, perform the merge
      if (length(common_cols) > 0) {
        parent_df <- merge(parent_df, child_list[[j]],
                           by = common_cols, all.x = TRUE, all.y = FALSE,
                           suffixes = suffixes)
        # Fuse identical columns and flag inconsistencies
        parent_df <- fuse_identical_columns(parent_df)
        # Record which child tables were merged
        join_tbls_list[[i]] <- c(join_tbls_list[[i]], names(child_list)[j])
        merged_leaves <- union(merged_leaves, names(child_list)[j])
        # Optionally drop join key columns
        if(drop_keys == TRUE) { parent_df[common_cols] <- NULL }
      }
    }
    out_list[[i]] <- parent_df
  }
  # Attach join_tbls attribute to each parent table
  for (i in seq_along(out_list)) {
    attr(out_list[[i]], "join_tbls") <- join_tbls_list[[i]]
  }
  names(out_list) <- names(parent_list)
  # Return merged tables and merged child table names
  return(list(db = out_list, merged_leaves = merged_leaves))
}
