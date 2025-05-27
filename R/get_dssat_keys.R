options(DSSAT.CSM = "C:\\DSSAT48\\DSCSM048.EXE")


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

lookup_keys <- function(){
  
  data.frame(
    item = c("crops","models","soil_analyses"),
    file = c("DETAIL.CDE","SIMULATION.CDE","SOIL.CDE"),
    header = c("Crop and Weed Species","Crop Models","")
  )
}

#'
#'

get_dssat_terms <- function(key = c("crops","models","soil_analyses")){
  
  dssat_csm <- options()$DSSAT.CSM  # locate DSSAT.CSM executable
  dssat_dir <- dirname(dssat_csm)  # locate DSSAT directory
  
  keys <- lookup_keys()
  key_location <- keys[which(keys$item == key),]
  
  lines <- readLines(file.path(dssat_dir, key_location$file))
  
  sec_start <- grep(key_location$header, lines, fixed = TRUE)
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
