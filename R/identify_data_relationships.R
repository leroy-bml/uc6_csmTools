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

get_pkeys <- function(df, alternates = FALSE){
  
  is_unique <- sapply(names(df), function(col_name) { 
    is_unique_id(df, col_name, type = "all")
  })
  
  is_pk <- is_unique[is_unique == TRUE]
  
  if(alternates == FALSE && length(is_pk) > 1){ 
    pk <- names(is_pk)[1]
  } else {
    pk <- names(is_pk)
  }
  
  return(pk)
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

#' Determine whether two data frames are linked by a common column
#' 
#' @param df1 the first data frame
#' @param df2 the second data frame
#' @param subset character vector containing the column names that should be considered in the evaluation
#' 
#' @return a logical stating whether the two data frames are dependent or not
#' 
#' @export
#'

has_link <- function(df1, df2, subset = NULL, exclude = NULL) {
  
  common_cols <- intersect(names(df1), names(df2))
  
  if(!is.null(subset)){
    common_cols <- common_cols[common_cols %in% subset]
  }
  
  if(!is.null(exclude)){
    common_cols <- common_cols[!common_cols %in% exclude]
  }
  
  return(length(common_cols) > 0)
}