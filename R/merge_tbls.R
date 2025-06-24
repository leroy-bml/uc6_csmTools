#' Merge data tables
#' 
#' Detects join keys between two related data tables and join tables accordingly
#' 
#' @export
#' 
#' @param parent_list a list containing the parent data frames
#' @param child_list a list containing the child data frames
#' @param type type of join between parent and child tables; "parent-child" (parent primary key is the join column);
#' "child-parent" (child primary key is the join column); "bidirectional" (looks for both parent-child and child-parent joins)
#' @param drop_keys logical; keep join keys in merged tables
#' @param suffixes character vector of length 2 specifying the suffixes to be added to overlapping column names in the result
#' (other than the join keys). Default is c(".x", ".y").
#' 
#' @return a list containing the parent tables with their joined attributes from the child tables
#'

merge_tbls <- function(parent_list, child_list,
                       type = c("parent-child","child-parent","bidirectional"),
                       drop_keys = TRUE,
                       suffixes = c(".x", ".y")) {

  # Identify all columns and primary keys
  # TODO: attributes letter sequences names if no names
  
  parent_list <- parent_list[order(names(parent_list))]
  parents_cols <- lapply(parent_list, colnames)
  parent_pkeys <- lapply(parent_list, function(df) get_pkeys(df, alternates = FALSE))
  
  child_list <- child_list[order(names(child_list))]
  child_cols <- lapply(child_list, colnames)
  child_pkeys <- lapply(child_list, function(df) get_pkeys(df, alternates = FALSE))
  
  # Merge by common columns according to the specified relationship type
  out_list <- list()
  
  join_tbls_list <- vector("list", length(parent_list))
  names(join_tbls_list) <- names(parent_list)
  
  for (i in seq_along(parent_list)) {
    
    parent_df <- parent_list[[i]]
    
    for (j in seq_along(child_list)) {
      
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
      print(common_cols)
      if (length(common_cols) > 0) {
        
        parent_df <- merge(parent_df, child_list[[j]],
                           by = common_cols, all.x = TRUE, all.y = FALSE,
                           suffixes = suffixes)
        
        join_tbls_list[[i]] <- c(join_tbls_list[[i]], names(child_list)[j])
        
        # Check for duplicate columns (redundant foreign keys)
        dupes <- grep("\\.x$", names(parent_df), value = TRUE)
        for (dupe in dupes) {
          
          dupe_y <- sub("\\.x$", ".y", dupe)
          
          if (all(parent_df[[dupe]] == parent_df[[dupe_y]], na.rm = TRUE)) {
            
            parent_df[[dupe]] <- NULL
            names(parent_df)[names(parent_df) == dupe_y] <- sub("\\.y$", "", dupe_y)
          }
        }
        
        if(drop_keys == TRUE) { parent_df[common_cols] <- NULL }
      }
    }
    out_list[[i]] <- parent_df
  }
  
  for (i in seq_along(out_list)) {
    
    attr(out_list[[i]], "join_tbls") <- join_tbls_list[[i]]
  }
  
  names(out_list) <- names(parent_list)
  
  return(out_list)
}
