#' Identify the primary key(s) in a table
#' 
#' 
#' @param df a data frame
#' @param col_name character string; name of the "year" column
#' @param type character string; specifies the data type for primary keys: "int" for integer, "all" for any type
#' 
#' @return a logical stating whether the input variable is a unique identifier
#' 
#' @export
#'

is_unique_id <- function(df, col_name, type = c("int","all")) {
  switch(
    type,
    "int" = {nrow(df) == length(unique(df[[col_name]])) & is.integer(df[[col_name]]) },
    "all" = {nrow(df) == length(unique(df[[col_name]])) }
  )
}


#' Identify the primary key(s) in a table
#' 
#' 
#' @param df a data frame
#' @param alternates logical; whether to include alternate primary keys in case when multiple column uniquely identfy the rows
#' 
#' @return a character vectir containing the primary key(s) column name(s)
#' 
#' @export
#'

get_pkeys <- function(df, alternates = FALSE, ignore_na = FALSE) {
  
  is_unique <- sapply(names(df), function(col_name) { 
    col <- df[[col_name]]
    if (ignore_na) {
      col <- col[!is.na(col)]
      # Check if all values are unique among non-NAs
      length(unique(col)) == length(col)
    } else {
      nrow(df) == length(unique(col))
    }
  })
  
  is_pk <- is_unique[is_unique]
  
  if(alternates == FALSE && length(is_pk) > 1){ 
    pk <- names(is_pk)[1]
  } else {
    pk <- names(is_pk)
  }
  
  return(pk)
}


#' Find Nested Join Keys Between Two Data Frames
#'
#' Identifies columns with the same name in two data frames where the set of unique, non-missing values in one column is a subset of the values in the corresponding column of the other data frame (in either direction).
#' Such columns are suitable as join keys for merging or joining the data frames, as all values in one data frame are guaranteed to be present in the other.
#'
#' @param df1 A data frame. The first data frame to compare.
#' @param df2 A data frame. The second data frame to compare.
#'
#' @details
#' The function operates as follows:
#' \itemize{
#'   \item It first identifies all columns with the same name in both data frames.
#'   \item For each such column, it extracts the unique, non-\code{NA} values from both data frames.
#'   \item If all values in one data frame's column are present in the other data frame's column (i.e., the values are nested in either direction), the column name is included in the result.
#'   \item The function does not require the sets of values to be identical, only that one is a subset of the other.
#' }
#'
#' @return
#' A character vector of column names that are suitable as join keys based on both name and nested content.
#'
#' @examples
#' df1 <- data.frame(id = c(1, 2, 3), group = c("A", "B", "C"))
#' df2 <- data.frame(id = c(1, 2, 3, 4), group = c("A", "B", "C", "D"))
#' find_nested_join_keys(df1, df2)
#' # Returns: "id" "group"
#'
#' df3 <- data.frame(id = c(1, 2), group = c("A", "B"))
#' find_nested_join_keys(df1, df3)
#' # Returns: "id" "group"
#'
#' df4 <- data.frame(id = c(4, 5), group = c("D", "E"))
#' find_nested_join_keys(df1, df4)
#' # Returns: character(0)
#'
#' @export

get_jkeys <- function(df1, df2) {
  
  common_cols <- intersect(names(df1), names(df2))
  nested_keys <- character(0)
  
  for (col in common_cols) {
    vals1 <- unique(na.omit(df1[[col]]))
    vals2 <- unique(na.omit(df2[[col]]))
    if (all(vals1 %in% vals2) || all(vals2 %in% vals1)) {
      nested_keys <- c(nested_keys, col)
    }
  }
  nested_keys
}


#' Retrieve the parent table(s) of a table
#' 
#' @export
#' 
#' @param tbl a data frame
#' @param tbl_list a list of data frames
#' 
#' @return the names of the parent tables of the focal table
#'

get_parents <- function(tbl, tbl_list) {
  
  cols <- colnames(tbl)
  pkeys <- lapply(tbl_list, function(df) get_pkeys(df, alternates = FALSE))
  
  parent_nms <- c()
  for (df_name in names(tbl_list)) {
    
    if (identical(tbl_list[[df_name]], tbl)) next
    
    common_cols <- intersect(cols, pkeys[[df_name]])
    
    if (length(common_cols) > 0) {
      parent_nms <- c(parent_nms, df_name)
    }
  }
  
  out <- tbl_list[parent_nms]
  return(out)
}


#' Categorize Data Frames in a Database List Based on Referential Relationships
#'
#' This function analyzes a named list of data frames (representing tables in a database)
#' and categorizes each table based on its referential relationships with other tables in the list.
#' It identifies whether a table is a "root," "branch," "leaf," or "standalone" table.
#'
#' @param db A named list of data frames, where each data frame represents a table in the database.
#'           Table names are expected as the names of the list elements.
#'
#' @return A named character vector where names are the data frame (table) names from `db`,
#'         and values are their assigned categories:
#'         * **"root"**: Tables that are referenced by other tables but do not themselves reference any other tables within `db`.
#'         * **"branch"**: Tables that both reference other tables and are referenced by other tables within `db`.
#'         * **"leaf"**: Tables that reference other tables but are not referenced by any other tables within `db`.
#'         * **"standalone"**: Tables that neither reference nor are referenced by any other tables within `db`.
#'         Returns `character(0)` if the input `db` is invalid or empty.
#'
#' @details
#' The categorization relies on two main criteria for each table:
#'
#' 1.  **Is it referencing another table in `db`?** This is determined by calling the `get_parents()` helper function
#'     on the current data frame against the entire `db` list. `get_parents()` is expected to identify if the current
#'     table contains foreign keys that point to primary keys in other tables within `db`.
#' 2.  **Is it referenced by another table in `db`?** This is determined by checking if the current table's name
#'     appears in the list of all tables that are identified as "parents" by any other table in `db`.
#'
#' Based on these two criteria, each table is assigned one of the four categories.
#' The function relies on the `get_parents()` helper function to correctly identify parent relationships.
#' 

categorize_tables <- function(db) {
  
  if (!is.list(db) || length(db) == 0) {
    warning("Input 'db' must be a non-empty list of dataframes.")
    return(character(0))
  }
  
  all_referenced <- unique(unlist(lapply(db, function(df_child) names(get_parents(df_child, db)))))
  
  categories <- character(length(db))
  names(categories) <- names(db)
  
  for (df_name in names(db)) {
    current_df <- db[[df_name]]
    
    is_referencing <- length(names(get_parents(current_df, db))) > 0
    is_referenced <- df_name %in% all_referenced
    
    if (!is_referencing && is_referenced) { # 0 Referencing, 1 Referenced
      categories[df_name] <- "root"
    } else if (is_referencing && is_referenced) { # 1 Referencing, 1 Referenced
      categories[df_name] <- "branch"
    } else if (is_referencing && !is_referenced) { # 1 Referencing, 0 Referenced
      categories[df_name] <- "leaf"
    } else if (!is_referencing && !is_referenced) { # 0 Referencing, 0 Referenced
      categories[df_name] <- "standalone"
    }
  }
  
  return(categories)
}
