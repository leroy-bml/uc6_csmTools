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
#' @param max_mods integer; maximum number of management event of the same type (e.g., irrigation) within one year; default is 8
#' 
#' @return the input database tagged as "management", "observed" or "other" data category (as attr(., "category"))
#' 
#' @importFrom dplyr "%>%" group_by_at summarise n_distinct across mutate ungroup select pull left_join
#' @importFrom tidyr all_of
#' @importFrom tibble as_tibble
#' 

# db <- seehausen
# design_nm <- "VERSUCHSAUFBAU"
# exp_str <- identify_exp_design(db, design_tbl = db[[design_nm]])
# db <- reshape_exp_data(db, mother_tbl_name = design_nm)
# 
# n_plots <- exp_str[exp_str$comp == "plot", "n"]
# n_treatments <- exp_str[exp_str$comp == "treatment", "n"]
# 
# max_plots <- max(
#   as.numeric(
#     # Drop year ranges
#     sub("^\\s*(\\d+).*", "\\1",
#         # Split instances
#         unlist(strsplit(n_plots, ";")))
#   ), na.rm = TRUE
# )
# max_treatments <- max(
#   as.numeric(
#     # Drop year ranges
#     sub("^\\s*(\\d+).*", "\\1",
#         # Split instances
#         unlist(strsplit(n_treatments, ";")))
#   ), na.rm = TRUE
# )


tag_data_type <- function(db, exp_str, plots_len, max_mods = 8) {

  # Design information
  years_col <- exp_str[exp_str$comp == "year", "id_name"]
  plots_col <- exp_str[exp_str$comp == "plot", "id_name"]
  treatments_col <- exp_str[exp_str$comp == "treatment", "id_name"]
  
  # Tag tables independent of plots as "other" as both management and observed data are tied to the plots
  db_desc <- lapply(db, function(df){
    
    if(!plots_col %in% names(df) | !years_col %in% names(df) ){
      
      df$tag <- tag  <- "other"  # use attribute instead?
      return(as_tibble(df))
    }
  })
  db_desc <- db_desc[!sapply(db_desc, is.null)]
  
  #
  db <- db[!names(db) %in% names(db_desc)]

  db_tag <- unlist(
    lapply(db, function(df){
      
      pkey <- get_pkeys(df, alternates = FALSE)
      
      # Calculate mean number of modalities per year
      events <- df %>%
        # Drop unique identifiers to keep only unique measurement/management modalities
        select(-all_of(c(plots_col, pkey))) %>%
        distinct()
      
      events_yr <- events %>%
        group_by_at(years_col) %>%
        # Calculate modalities per year and average across years
        summarise(n = n(), .groups = "drop") %>%
        summarise(max_n = max(n, na.rm = TRUE)) %>%
        pull(max_n)
      
      return(events_yr)
    })
  )
  
  tag_mngt_obs <- function(x, mngt_thr, obs_thr) {
    # Calculate difference
    diffs1 <- abs(x - mngt_thr)
    diffs2 <- abs(x - obs_thr)
    # Assign table names to mngt and obs groups
    mngt <- names(x)[diffs1 <= diffs2]
    obs <- names(x)[diffs2 <= diffs1]
    list(
      management = mngt,  # closer to t1 (or equal distance)
      observed = obs   # closer to t2
    )
  }
  
  tag_mngt_obs(db_tag, max_treatments, max_plots)
  
  tag <- ifelse(modalities <= max_mods, "management", "observed")
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
  
  #return(as_tibble(df))

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

# db <- muencheberg
# design_nm <- "VERSUCHSAUFBAU"
# exp_str <- identify_exp_design(db, design_tbl = db[[design_nm]])
# db <- reshape_exp_data(db, mother_tbl_name = design_nm)
# 
# df <- db$PFLANZENSCHUTZ

# TODO: debug when date missing (seehausen BODENBIOLOGIE, muencheberg PFLANZENLABOR...)
# TODO: problem when multiple dates + phenology data = measured but few values...
# TODO: summary calculation (e.g., seehausen ENERGIEBILANZ)
# TODO: no years col (e.g., seehausen KLIMADATEN)
# EXTRA GROUP: noDate
# --> noDate+noPlot: BILANS (=/= TREATMENT, as treatment has date and plot)
# --> noDate+wPlot: OBS_SUMMARY (=/= TREATMENT, as treatment has plot)
# --> missingDate+wPlot: OBS_TIMESERIES (=/= SUMMARY, as TS>1 record per plotk)
# --> multipleDates: AUSSAAT or PHENOLOGY?

# exp_str
# years_col <- exp_str[1,3]
# plots_col <- exp_str[2,3]
# is_treatment(df, years_col, plots_col)

is_treatment <- function(df, years_col, plots_col){

  
  expand_design <- function(df) {
    
    # Find all matches of value [start-end]
    n_plots_str <- exp_str[exp_str$comp == "plot", "n"]
    n_treatments_str <- exp_str[exp_str$comp == "treatment", "n"]
    
    expand_n <- function(str) {
      
      # Extract n values
      n <- as.integer(str_extract_all(str, "\\d+(?= \\[)")[[1]])
      # Expand year sequences
      year_ranges <- str_extract_all(str, "\\[(\\d{4}-\\d{4})\\]")[[1]]
      year_ranges <- gsub("\\[|\\]", "", year_ranges)
      year_ranges <- strsplit(year_ranges, "-")
      # Map n to years in data frames
      n_df <- Map(function(n, years) {
        data.frame(
          year = seq(as.integer(years[1]), as.integer(years[2])),
          n = n
        )
      }, n, year_ranges)
      # Combine into one data frame
      n_df <- do.call(rbind, n_df)
    }
    
    plots_df <- expand_n(n_plots_str)
    names(plots_df)[2] <- "n_plots"
    
    treatments_df <- expand_n(n_treatments_str)
    names(treatments_df)[2] <- "n_treatments"

    out <- merge(plots_df, treatments_df)
    names(out)[1] <- exp_str$id_name[1]
    
    return(out)
  }
  

  exp_str_df <- expand_design(exp_str)
  
  
  mngt_id <- get_pkeys(df, alternates = FALSE)
  date_col = colnames(df[apply(df, 2, is_date)])[1] ### CHECK WHY DOES NOT WORK WITH SAPPLY
  # [1] is in case they are two dates, e.g., sowing and emergence (not great solution...)
  
  management_cols <- setdiff(names(df), c(mngt_id, years_col, plots_col))

  # Count number of events within year and plot
  df2 <- df %>%
    mutate(across(all_of(management_cols), as.character)) %>%
    group_by_at(c(years_col, plots_col)) %>%
    arrange(!!sym(date_col), .by_group = TRUE) %>%
    mutate(events_count = row_number()) %>%
    ungroup() %>%
    # Concatenate event attributes
    # TODO: for now contains some attributes that might need to be excluded
    # (e.g., comments, operator)
    mutate(mngt_full = do.call(paste, c(across(all_of(management_cols)), sep = "|")))
  
  # Count unique management events across plots for each year-event index
  unique_counts <- df2 %>%
    group_by_at(c(years_col, "events_count")) %>%
    summarise(n_plots = n(),
              unique_count = n_distinct(mngt_full, na.rm = TRUE), .groups = "drop") %>%
    left_join(exp_str_df, by = 1)
    mutate(is_trt = ifelse(unique_count > 1, TRUE, FALSE),
           plots_per_comb = n_plots/unique_count) %>%
    as.data.frame()
  
  # Output: is treatment per year
  out <- unique_counts %>%
    group_by_at(years_col) %>%
    summarise(is_trt = any(is_trt), .groups = "drop")
  
  return(unique_counts)
}
