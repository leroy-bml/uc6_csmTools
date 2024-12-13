#' ######
#' 
#' @param ##
#' 
#' @importFrom ###
#' 
#' @return 

set_class <- function(df, classes) {
  df <- df[names(df) %in% names(classes)]
  correct_class <- sapply(names(df), function(x) class(x)[1])
  df <- mapply(function(value, correct_class) {
    switch(correct_class,
           Date = as.Date(value),
           POSIXct = as.POSIXct(value),
           POSIXt = as.POSIXct(value),
           numeric = as.numeric(value),
           integer = as.integer(value),
           character = as.character(value),
           factor = as.factor(value),
           value)
  }, df, classes, SIMPLIFY = FALSE)
  
  return(as.data.frame(df))
}

#' ######
#' 
#' @param fdata
#' @param type
#' @param rate
#' @param FDATE
#' @param FNAME
#' @param FMCD
#' @param FACD
#' @param FDEP
#' @param FOCD
#' 
#' @importFrom dplyr "%>%" mutate
#' @importFrom tibble add_row
#' 
#' @return 

filex <- read_filex("C:/DSSAT48/Wheat/KSAS8101.WHX")
section <- "irrigation"
args <- list(EFIR = 1, IDATE = c("1981-05-26","1981-06-24"), IRVAL = c(50,50))
#args <- list(FEDATE = c("1981-05-26","1981-06-24"), FMCD = "FE041", FACD = "AP001", FAMN = , FAMP = , FAMK = , FAMC = , FDEP = ,)

add_management(filex = filex, section = "irrigation", args = args)

# TODO: handle composite section (initial_conditions, irrigation); class list for collapsed vars
add_management <- function(filex,
                           section = c("initial_conditions","planting","tillage","irrigation",
                                       "fertilizer","organic_amendment","chemicals","harvest"),
                           args = list()) {
  
  # Match management type to corresponding file X section
  input_nm <- toupper(substr(section, 1, 5))
  sections_pref <- sapply(names(filex), function(x) substr(x, 1, 5))
  matches <- sapply(sections_pref, function(x) identical(input_nm, x))
  
  if (!section_name %in% sections_pref) {
    stop(paste(section, "is not a valid file X section"))
  }
  
  # Set focal section and its elements
  sec_nm <- names(matches[matches])
  sec <- filex[[sec_nm]]  # select focal section
  cols <- names(sec)  # section columns
  classes <- sapply(sec, function(x) class(x)[1])  # classes for section columns

  # Format args as a new row for the specified section
  new_row <- setNames(as.list(rep(NA, length(cols))), cols)
  for (col in names(args)) {
    if (col %in% cols) {
      new_row[[col]] <- args[[col]]
    } else {
      warning(paste("Column", arg, "not found in the dataframe"))
    }
  }
  
  new_row <- set_class(df = new_row, classes = classes)
  new_row[[1]] <- max(sec[1]) + 1  # set new row primary key
  
  # If collapsible section:
  nested_cols <- names(classes[classes %in% "list"])
  
  if (length(nested_cols > 0)){
    classes_nested <- sapply(sec[nested_cols], function(x) sapply(x, function(x) class(x)[1]))
    new_row_lss <- set_class(df = new_row, classes = classes_nested)
    new_row <- cbind(
      new_row[, !(names(new_row) %in% intersect(names(new_row), nested_cols))],
      new_row_lss
    )
  }
  
  # Bind new row to section data
  if (section %in% c("initial_conditions","irrigation")){  # nested sections
    sec <- unnest(sec, cols = all_of(nested_cols))
    out <- bind_rows(sec, new_row) 
    out <- collapse_cols(new_row, names(new_row_lss))
  } else {
    out <- bind_rows(sec, new_row) 
  }

  return(out)
}


