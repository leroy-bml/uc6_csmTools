get_icasa <- function(){
  url <- "https://github.com/DSSAT/ICASA-Dictionary/raw/refs/heads/main/ICASA%20Data%20Dictionary.xlsx"  # official ICASA repo
  wb <- wb_load(file = url, data_only = TRUE)
  
  ls <- lapply(2:length(wb$worksheets), function(x) { 
    wb_to_df(wb, sheet = x)
  })
  names(ls) <- wb$sheet_names[2:length(wb$worksheets)]
  
  return(ls)
}


#'
#'

get_model_list <- function(){
  
  dssat_csm <- options()$DSSAT.CSM  # locate DSSAT.CSM executable
  dssat_dir <- dirname(dssat_csm)  # locate DSSAT directory
  
  lines <- readLines(file.path(dssat_dir, "SIMULATION.CDE"))
  
  sec_start <- grep("Crop Models", lines, fixed = TRUE)
  sec_end <- which(lines[(sec_start + 1):length(lines)] == "")[1] + sec_start  # find the next empty line
  sec_end <- ifelse(is.na(sec_end), length(lines) + 1, sec_end)  # set to end if no empty line is foun
  sec_lines <- lines[(sec_start + 1):(sec_end - 1)]
  
  sec_comps <- strsplit(sec_lines, " {2,}")
  
  sec_cols <- do.call(rbind, lapply(sec_comps, function(x) {
    # ensure each row has exactly 3 components by collapsing extra spaces
    if (length(x) > 3) {
      x <- c(x[1], x[2], paste(x[3:length(x)], collapse = " "))
    }
    x
  }))
  sec_df <- as.data.frame(sec_cols[-1,])
  colnames(sec_df) <- sec_cols[1,]
  
  return(sec_df)
}

#'
#'

get_crop_list <- function(){
  
  dssat_csm <- options()$DSSAT.CSM  # locate DSSAT.CSM executable
  dssat_dir <- dirname(dssat_csm)  # locate DSSAT directory
  
  lines <- readLines(file.path(dssat_dir, "DETAIL.CDE"))
  
  sec_start <- suppressWarnings(
    grep("Crop and Weed Species", lines, fixed = TRUE)
  )
  sec_end <- which(lines[(sec_start + 1):length(lines)] == "")[1] + sec_start  # find the next empty line
  sec_end <- ifelse(is.na(sec_end), length(lines) + 1, sec_end)  # set to end if no empty line is foun
  sec_lines <- lines[(sec_start + 1):(sec_end - 1)]
  
  sec_comps <- strsplit(sec_lines, " {2,}")
  
  sec_cols <- do.call(rbind, lapply(sec_comps, function(x) {
    # ensure each row has exactly 3 components by collapsing extra spaces
    if (length(x) > 3) {
      x <- c(x[1], x[2], paste(x[3:length(x)], collapse = " "))
    }
    x
  }))
  sec_df <- as.data.frame(sec_cols[-1,])
  colnames(sec_df) <- sec_cols[1,]
  
  return(sec_df)  
}
