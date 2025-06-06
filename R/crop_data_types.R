#' Identify the type of experimental data
#' 
#' Determines whether a data table contains management data (i.e., fixed experimental parameters), observed data
#' (i.e., parameters measured during the experiment) or other attributes (i.e., neither management nor observed data)
#' 
#' @export
#'
#' @param db a list of data frames
#' @param years_col character string; name of the "year" column
#' @param plots_col character string; name of the plot identifier column
#' @param plots_len integer; length of the plot identifier (i0.e., number of plots)
#' @param max_events integer; maximum number of management event of the same type (e.g., irrigation) within one year; default is 8
#' 
#' @return the input database tagged as "management", "observed" or "other" data category (as attr(., "category"))
#' 
#' @importFrom dplyr "%>%" group_by_at summarise n_distinct across mutate ungroup select pull left_join
#' @importFrom tidyr all_of
#' @importFrom tibble as_tibble
#' 

tag_data_type <- function(db, years_col, plots_col, plots_len, max_events = 8) {

  # db = DATA_tbls
  # years_col = "Year"
  # plots_col = "Plot_id"
  # plots_len = PLOTS_n
  # max_events = mean(TREATMENTS_n, na.rm = TRUE)
  
  # Tag tables independent of plots as "other" as both management and observed data are tied to the plots
  db_desc <- lapply(db, function(df){
    
    if(!plots_col %in% names(df) | !years_col %in% names(df) ){
      
      df$tag <- tag  <- "other"
      return(as_tibble(df))
    }
  })
  db_desc <- db_desc[!sapply(db_desc, is.null)]
  
  db <- db[!names(db) %in% names(db_desc)]

  db_tag <- lapply(db, function(df){

    pkey <- get_pkeys(df, alternates = FALSE)
    
    # Calculate mean number of modalities per year
    modalities <- df %>%
      # Drop unique identifiers to keep only unique measurement/management modalities
      select(-all_of(c(plots_col, pkey))) %>%
      distinct() %>%
      group_by_at(years_col) %>%
      # Calculate modalities per year and average across years
      summarise(n = n(), .groups = "drop") %>%
      summarise(mean_n = mean(n)) %>%
      pull(mean_n)

    tag <- ifelse(modalities <= max_events, "management", "observed")
    df$tag <- tag

    # Handle obs at the year-level rather than overall
    # Observed data can over between Summary and TimeSeries categories depending on how many measurements were
    # taken per year. This may change across years.
    if (tag == "observed") {
      
      plots_yr <- data.frame(
        Year = as.numeric(names(plots_len)),
        length = unname(plots_len)
      )
      
      obs_tag <- df %>%
        group_by_at(years_col) %>%
        summarise(n = n()) %>%
        left_join(plots_yr, by = "Year") %>%
        mutate(obs_cat = ifelse(n <= length, "observed_summary", "observed_timeseries")) %>%
        ungroup() %>%
        select(-n)
      
      df <- df %>%
        left_join(obs_tag, by = years_col) %>%
        mutate(tag = ifelse(tag == "management", "management", obs_cat)) %>%
        select(-obs_cat)   
    }
    
    return(as_tibble(df))
  })
  
  db_tagged <- append(db_tag, db_desc)
  
  # Split data frames by tag
  db_tagged <- lapply(db_tagged, function(df) {
    if(length(unique(df$tag)) == 1) {
      ls <- list(df)
      names(ls) <- unique(df$tag)
      return(ls)
    } else {
      split(df, df$tag)
    }
  })
  
  # Set all missing tag sublists as NULL to use the revert_list_str function
  nms <- unique(unlist(lapply(db_tagged, names), use.names = FALSE))
  
  db_out <- lapply(db_tagged, function(ls) {
    for (i in nms) {
      if (!i %in% names(ls)) {
        ls <- append(ls, setNames(list(NULL), i))
      }
    }
    return(ls)
  })

  db_out <- revert_list_str(db_out)
  
  # Drop NULLS
  db_out <- lapply(db_out, function(ls) Filter(Negate(is.null), ls))

  return(db_out)
}

#' Identify if a management type is an experimental treatment
#' 
#' Determines for each year whether a management type has different fixed value among plots within year (experimental treatment)
#' or is uniform among plots within year.
#' 
#' @export
#'
#' @param df a data frame containing management data
#' @param years_col character string; name of the "year" column
#' @param plots_col character string; name of the plot identifier column
#'  
#' @return a data frame containing the year column and a logical "is_trt"
#' 
#' @importFrom dplyr "%>%" group_by_at arrange mutate row_number across c_across rowwise select n_distinct summarise
#' @importFrom tidyr matches all_of spread
#' @importFrom rlang "!!" sym
#' 


is_treatment <- function(df, years_col, plots_col){
  
  if(is.null(plots_col)){
    df$plots_id = 1
    plots_col <- names(df)[ncol(df)]
  }
  
  mngt_id <- get_pkeys(df, alternates = FALSE)
  date_col = colnames(df[apply(df, 2, is_date)])[1] ### CHECK WHY DOES NOT WORK WITH SAPPLY
  # [1] is in case they are two dates, e.g., sowing and emergence (not great solution...)
  
  management_cols <- setdiff(names(df), c(mngt_id, years_col, plots_col))
  
  out <- df %>%
    # Count event within years (ordered by date to attribute the same rank to identical events)
    group_by_at(c(years_col, plots_col)) %>%
    arrange(!!sym(date_col)) %>%
    mutate(events_count = row_number()) %>%
    # Collapse all management features into one variable for wide format conversion
    mutate(across(matches(paste(management_cols, collapse = "|")), as.character)) %>%
    rowwise() %>%
    mutate(mngt_full = paste(c_across(all_of(management_cols)), collapse = "|")) %>%
    # Convert to wide format, i.e., year x plot matrix with management as values
    select(all_of(c(years_col, plots_col)), events_count, mngt_full) %>%
    spread(plots_col, mngt_full) %>%
    # Count the number of unique management events among plots
    rowwise() %>%
    mutate(unique_count = n_distinct(c_across(3:ncol(.)))) %>%
    # If there are more than one distinct management sequence among plots (within year), return TRUE
    group_by_at(years_col) %>%
    summarise(is_trt = any(ifelse(unique_count > 1, TRUE, FALSE))) %>%
    as.data.frame()
  
  return(out)
}