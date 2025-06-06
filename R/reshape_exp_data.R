#' Reshape crop experiment data into a standard ICASA format
#' 
#' This function identifies the components of a crop experiment data set and re-arrange them into ICASA sections
#' (see ICASA standards: https://docs.google.com/spreadsheets/u/0/d/1MYx1ukUsCAM1pcixbVQSu49NU-LfXg-Dtt-ncLBzGAM/pub?output=html)
#' Note that the function does not handle variable mapping but only re-arranges the data structure.
#' 
#' @export
#' 
#' @param db a list of data frames composing a crop experiment dataset
#' @param metadata a metadata table described the dataset, as returned by read_metadata (BonaRes-LTFE format)
#' @param mother_tbl a data frame; the mother table of the dataset, i.e., describing the experimental design (years, plots_id, treatments_ids, replicates)
#' 
#' @return a list containing the reshaped crop experiment data
#' 
#' @importFrom lubridate as_date parse_date_time
#' @importFrom dplyr "%>%" select group_by group_by_at ungroup mutate relocate distinct left_join arrange across cur_group_id
#' @importFrom tidyr any_of all_of everything ends_with
#' @importFrom rlang "!!" ":="
#' @importFrom countrycode countrycode
#'

# tmp load data
db_files <- list.files(path = "./inst/extdata/lte_muencheberg/0_raw/", pattern = "\\.csv$")
db_paths <- sapply(db_files, function(x){ paste0("./inst/extdata/lte_muencheberg/0_raw/", x) })
db_list <- lapply(db_paths, function(x) { file <- read.csv(x, fileEncoding = "UTF-8") })
# TODO: check best encoding, iso-8859-1 vs. latin1 vs. UTF-8

# Simplify names
# names(db_list) <- str_extract(names(db_list), "(?<=_)[^_]+$") # cut names after version number
# names(db_list) <- gsub("_V[0-9]+_[0-9]+_", "", names(db_list)) # drop version number
find_common_prefix <- function(strings) {
  common_prefix <- strings[1]
  for (str in strings) {
    while (!startsWith(str, common_prefix) && nchar(common_prefix) > 0) {
      common_prefix <- substr(common_prefix, 1, nchar(common_prefix) - 1)
    }
  }
  return(common_prefix)
}
prefix <- find_common_prefix(names(db_list))
names(db_list) <- sub(paste0("^", prefix), "", names(db_list))
names(db_list) <- sub("\\..*$", "", names(db_list)) # drop file extension

##--

reshape_exp_data <- function(db, metadata, mother_tbl) {
  
  
  
  
  
  # Identify the components of the experimental design ----------------------
  
  mother_tbl <- db_list[["VERSUCHSAUFBAU"]]  #tmp
  db <- db_list  #tmp
  
  DESIGN_tbl <- mother_tbl
  DESIGN_tbl_nm <- get_df_name(db, DESIGN_tbl)
  
  # Find year column across the data
  # multiyear <- if(metadata$start_date != metadata$status) {
  #   TRUE
  # } else {
  #   FALSE
  # }

  # Identify years column name and number
  YEARS_nm <- "Versuchsjahr"
  YEARS_id <- unique(DESIGN_tbl[[YEARS_nm]])
  YEARS_n <- length(YEARS_id) 
  
  # YEARS_id <- get_years_col(DESIGN,
  #                           start = metadata$start_date,
  #                           end = metadata$status)
  # YEARS_n <- length(unique(DESIGN[[YEARS_nm]])) # number of years in data
  
  # Identify plots number, column names and data table
  # PLOTS_n <- if (is.na(metadata$number_plots)){  
  #   nrow(DESIGN) / YEARS_n
  # } else {
  #   as.numeric(metadata$number_plots)
  # }
  distinct_per_year <- function(col, df, yr = "Year") {
    tapply(df[[col]], df[[yr]], function(x) length(unique(x)))
  }
  
  PLOTS_nm <- "Parzelle_ID"
  PLOTS_id <- unique(DESIGN_tbl[[PLOTS_nm]])
  PLOTS_n <- distinct_per_year(PLOTS_nm, DESIGN_tbl, YEARS_nm) 
  PLOTS_tbl <- get_tbl(db, PLOTS_nm)

  # PLOTS_id <- apply(DESIGN, 2, function(x) length(unique(x)))
  # PLOTS_id <- names(PLOTS_id[PLOTS_id == PLOTS_n])
  # PLOTS_tbl <- get_tbl(db, PLOTS_id)
  
  # Identify repliates number and column name
  TREATMENTS_nm <- "Pruefglied_ID"
  TREATMENTS_id <- unique(DESIGN_tbl[[TREATMENTS_nm]])
  TREATMENTS_n <- distinct_per_year(TREATMENTS_nm, DESIGN_tbl, YEARS_nm) 
  TREATMENTS_tbl <- get_tbl(db, TREATMENTS_nm)
  # TREATMENTS_n <- PLOTS_n / REPS_n  # Can also exploit design metadata
  # TREATMENTS_id <- get_treatments_col(DESIGN, YEARS_id, PLOTS_id)[[1]]
  # TREATMENTS_tbl <- get_tbl(db, TREATMENTS_id)
  
  # DESIGN_tbl %>%
  #   group_by(Versuchsjahr) %>%
  #   summarise(across(everything(), n_distinct)) %>%
  #   mutate(across(-c(Versuchsjahr, Parzelle_ID), ~ Parzelle_ID / .))

  REPS_n <- PLOTS_n / TREATMENTS_n
  DESIGN_tbl <- DESIGN_tbl %>%
    group_by(Versuchsjahr, !!sym(TREATMENTS_nm)) %>%
    mutate(Rep_no = n())
  REPS_nm <- "Rep_no"
  
  # Update design table in dataset
  db[["VERSUCHSAUFBAU"]] <- DESIGN_tbl

  # Give standard name to design variables ----------------------------------
  
  
  raw_str_names <- c(YEARS_nm, PLOTS_nm, TREATMENTS_nm, REPS_nm)
  std_str_names <- c("Year", "Plot_id", "Treatment_id", "Rep_no")
  
  for (i in seq_along(db)) {
    for (j in seq_along(raw_str_names)) {
      if (raw_str_names[j] %in% colnames(db[[i]])) {
        colnames(db[[i]])[colnames(db[[i]]) == raw_str_names[j]] <- std_str_names[j]
      }
    }
  }
  
  
  
  # Format dates ------------------------------------------------------------
  
  
  # Identify dates and convert into a usable format (yyyy-mm-dd; vMapper default)
  db <- lapply(db, function(df){
    df <- as.data.frame(
      lapply(df, function(x){
        if(is_date(x, dssat_fmt = FALSE)){
          format(parse_date_time(x, orders = c("Ymd", "mdY", "dmy", "ymd", "mdy")), "%Y-%m-%d")
        } else {
          x
        }
      })
    )
  })
  
  
  # Format variable names ---------------------------------------------------
  
  
  all_cols <- unlist(lapply(db, colnames), use.names = FALSE)
  
  # Identify all date columns (can be identified as join keys, which is not desirable)
  date_cols <- lapply(db, function(df) unlist(lapply(df, function(x) is_date(x)))) %>% unlist()
  names(date_cols) <- gsub("^[^.]*\\.","", names(date_cols))
  date_cols <- names(date_cols[date_cols])
  
  # Find join keys that are not dates
  all_pkeys <- unlist(lapply(db, function(df) get_pkeys(df, alternates = FALSE)), use.names = FALSE)  # primary keys
  
  all_jkeys <- all_cols[all_cols %in% c(all_pkeys, std_str_names) & !all_cols %in% date_cols]  # exclude dates
  all_jkeys <- c(unique(all_jkeys[duplicated(all_jkeys)]), "Rep_no")  # all join keys + rep number (str variables)
  
  attr_cols <- all_cols[!all_cols %in% all_jkeys]  # non-join attributes
  
  # Bind original table name to column names to prevent duplicate attributes and help with tracking errors
  # along the data wrangling process
  db_ipt <- lapply(
    names(db), function(df_name) {
      df <- db[[df_name]]
      colnames(df) <- ifelse(colnames(df) %in% attr_cols,
                             paste(df_name, colnames(df), sep = "."),
                             colnames(df))
      return(df)
    }
  )
  names(db_ipt) <- names(db)
  
  # Update design elements affected by name change
  DESIGN_tbl <- db_ipt[[DESIGN_tbl_nm]]
  PLOTS_tbl <- get_tbl(db_ipt, "Plot_id")
  TREATMENTS_tbl <- get_tbl(db_ipt, "Treatment_id")
  
  # Assemble the structured experimental design table
  DESIGN_str <-
    merge_tbls(list(DESIGN = DESIGN_tbl),
               list(TREATMENTS = TREATMENTS_tbl, PLOTS = PLOTS_tbl),
               type = "child-parent",
               drop_keys = FALSE)[[1]] %>%
    select(any_of(std_str_names), matches("(?i)Faktor"))  #!!
  
  
  # Identify and join related tables ----------------------------------------
  
  
  # Identify non-design tables
  DESIGN_tbls_nms <- unlist(
    lapply(list(DESIGN_tbl, PLOTS_tbl, TREATMENTS_tbl), function(df) get_df_name(db_ipt, df))
  )
  
  other_tbls <- db_ipt[!names(db_ipt) %in% DESIGN_tbls_nms]
  other_tbls <- other_tbls[order(names(other_tbls))]
  
  # Identify data tables: tables with link to the design tabes
  has_design_link <- sapply(other_tbls, function(df){
    has_link(df, DESIGN_str, subset = c("Year", "Plot_id", "Treatment_id", "Faktor_id"))
  })
  DATA_tbls <- other_tbls[has_design_link]
  
  # Attribute tables: tables with no direct link to the design table (i.e., attributes of data tables)
  ATTR_tbls <- other_tbls[!has_design_link]
  
  
  
  # ==== 1) ATTR tables ------------------------------------------------------
  
  # Iterate to merge multi-level joins (i.e., attributes of attribute tables) ###
  repeat {
    # Identify parent tables of each table in the database
    ATTR_parents <- lapply(ATTR_tbls, function(df) get_parent(df, ATTR_tbls))
    
    # Identify leaf tables (i.e., terminal tables) and append to their parent tables
    ATTR_leaves <- ATTR_tbls[sapply(ATTR_parents, is.null)]
    ATTR_children <- ATTR_tbls[!names(ATTR_tbls) %in% names(ATTR_leaves)]
    
    # If there are no more leaves, break out of the loop
    if (length(ATTR_leaves) == 0) {
      break
    }
    
    # Perform the merge
    ATTR_merged <- merge_tbls(ATTR_children, ATTR_leaves, type = "child-parent", drop_keys = TRUE)
    
    # Update the original list
    indices <- which(names(ATTR_tbls) %in% names(ATTR_merged))
    ATTR_tbls[indices] <- ATTR_merged
    
    # Update the ATTR_children list after the merge
    ATTR_children <- ATTR_tbls[!names(ATTR_tbls) %in% names(ATTR_leaves)]
    
    # If there are no more parents to process, break out of the loop
    if (length(ATTR_children) == 0) {
      break
    }
  }
  
  
  
  # ==== 2) DATA tables ------------------------------------------------------
  
  # Join attributes to data tables
  DATA_tbls <- merge_tbls(DATA_tbls, ATTR_tbls, type = "bidirectional", drop_keys = TRUE)
  #TODO: function to identify relationship types
  
  
  
  # Identify management and observed data -----------------------------------
  
  
  # Distinguish management and observed data
  DATA_tbls_ident <- tag_data_type(db = DATA_tbls,
                                   years_col = "Year",
                                   plots_col = "Plot_id",
                                   plots_len = PLOTS_n,
                                   max_events = 8)
  # TODO: >2 date vars? 
  
  # Separate management and observed data
  data_cats <- sapply(DATA_tbls_ident, function(df) attr(df, "category"))
  
  MNGT_ipt <- DATA_tbls_ident[["management"]]
  DOBS_suma_ipt <- DATA_tbls_ident[["observed_summary"]]
  DOBS_tser_ipt <- DATA_tbls_ident[["observed_timeseries"]] ## some years summary, other timeseries
  ATTR_ipt <- DATA_tbls_ident[["other"]]
  
  
  MNGT_ipt <- DATA_tbls[names(DATA_tbls) %in%
                          c("AUSSAAT", "BEREGNUNG", "BODENBEARBEITUNG", "ERNTE", "PFLANZENSCHUTZ", "DUENGUNG")]
  
  
  # Format fields table -----------------------------------------------------
  
  
  # Define FIELDS ID
  PLOTS_all_ids <- get_pkeys(PLOTS_tbl, alternates = TRUE)
  FIELDS_cols <- setdiff(colnames(PLOTS_tbl), c(PLOTS_all_ids, std_str_names))
  
  # Make field table
  FIELDS_tbl <- PLOTS_tbl %>%
    group_by_at(FIELDS_cols) %>%
    mutate(FL_ID = cur_group_id()) %>% ungroup() %>%
    relocate(FL_ID, .before = everything()) 
  
  FIELDS <- FIELDS_tbl %>%
    select(FL_ID, all_of(FIELDS_cols)) %>%
    distinct()
  
  # Check if spatial data is in the table using bounding box data
  for (col_name in colnames(FIELDS)) { ##!! is not fool proof?
    
    if (all(FIELDS[[col_name]] == metadata$longitude)) {  #! currently does not handle multi-sites
      names(FIELDS)[names(FIELDS) == col_name] <- "FL_LON"
    }
    
    if (all(FIELDS[[col_name]] == metadata$latitude)) {
      names(FIELDS)[names(FIELDS) == col_name] <- "FL_LAT"
    }
  }
  
  # If not in table, replace by the mean of the bounding box x and y boundaries
  # Whether this is accurate depends on how the bouding box is defined, and only if there is one field
  if(!"FL_LON" %in% names(FIELDS)){
    FIELDS[["FL_LON"]] = metadata$longitude
  }
  
  if(!"FL_LAT" %in% names(FIELDS)){
    FIELDS[["FL_LAT"]] = metadata$latitude
  }
  
  
  
  # Format management tables ------------------------------------------------
  
  
  # Identify which management category correpond to the treatments
  MNGT_is_trt <- lapply(MNGT_ipt, function(df) is_treatment(df, "Year", "Plot_id"))
  # TODO: handle 0/1 treatments (so far not tagged as treatment)
  # TODO: create treatment name in the function
  
  # Merge management IDs with the treatments table
  MNGT_ipt <- mapply(function(df1, df2) left_join(df1, df2, by = "Year"), MNGT_ipt, MNGT_is_trt)
  
  # Add an ID per year for non-treatment management events
  MNGT_fmt <- lapply(names(MNGT_ipt), function(df_name){
    
    df <- MNGT_ipt[[df_name]]
    
    #
    mngt_id <- get_pkeys(df, alternates = FALSE)
    
    # Sepcify name of the ID for the reduced table
    ID_nm <- paste0(toupper(substr(df_name, 1, 2)), "_ID")
    # Create the ID variable
    df <- df %>%
      # TODO: modify to handle variable factor levels
      left_join(DESIGN_str, by = c("Year", "Plot_id")) %>%
      group_by_at(c("Year", "Faktor1_Stufe_ID")) %>%
      mutate(ID_trt_1 = cur_group_id()) %>% ungroup() %>%
      group_by_at(c("Year", "Faktor2_Stufe_ID")) %>%
      mutate(ID_trt_2 = cur_group_id()) %>% ungroup() %>%
      group_by_at("Year") %>%
      mutate(ID_fix = cur_group_id()) %>% ungroup() %>%
      mutate(!!ID_nm := ifelse(is_trt, paste(ID_trt_1, ID_trt_2, sep = "_"), ID_fix)) %>%
      select(-c(ID_trt_1, ID_trt_2, ID_fix, is_trt, Faktor1_Stufe_ID, Faktor2_Stufe_ID)) %>%
      relocate(!!ID_nm, .before = everything())
    
    # Drop primary key, treatment and crop keys
    df <- df[!names(df) %in% c(mngt_id, "Treatment_id")]
    df <- df %>% arrange(.data[[ID_nm]])
    
    return(df)
    
  })
  names(MNGT_fmt) <- names(MNGT_ipt)
  
  # List 1: reduced management tables
  # (identified by unique management event features rather than event x plot combinations)
  MNGT_out <- lapply(MNGT_fmt, function(df){ distinct(df[!names(df) == "Plot_id"]) })
  names(MNGT_out) <- names(MNGT_fmt)
  
  # List 2: IDs only (to update IDs in the treatment matrix)
  MNGT_ids <- lapply(MNGT_fmt, function(df){
    mngt_id <- colnames(df)[1]
    df[names(df) %in% c(mngt_id, "Year", "Plot_id")]
  })
  
  
  
  # Format treatments matrix ------------------------------------------------
  
  
  TREATMENTS_matrix <-
    # Append each management IDs to the correponding year x plot combination
    Reduce(function(x, y)
      merge(x, y, by = intersect(names(x), names(y)), all.x = TRUE),
      MNGT_ids, init = DESIGN_str) %>%
    select(-Rep_no) %>%  ##!! see if necessary
    distinct() %>%
    # Append FIELDS ids
    left_join(FIELDS_tbl %>%
                select(FL_ID, Plot_id),
              by = "Plot_id") %>%
    # Replace NA ids by 0 (= no management event)
    mutate(across(ends_with("_ID"), ~ifelse(is.na(.x), 0, .x))) %>%
    # Rename and recode treatment ID
    group_by(Treatment_id) %>%
    mutate(TRTNO = cur_group_id()) %>% ungroup() %>%
    #rename(REPNO = !!REPS_id) %>%
    relocate(TRTNO, .before = everything())
  
  
  
  # Format observed data ----------------------------------------------------
  
  
  # Drop identifiers and tag
  DOB_suma_ipt <- lapply(DOBS_suma_ipt, function(df){
    pkeys <- get_pkeys(df, alternates = FALSE)
    df[!names(df) %in% c(pkeys, "tag")]
  })
  
  # Merge all dataframes
  DOB_suma_out <- Reduce(function(x, y)
    merge(x, y, by = c("Year", "Plot_id"), all = TRUE), DOB_suma_ipt,
    init = DESIGN_str %>% select(Year, Plot_id, Treatment_id, Rep_no)) %>%
    # Rename and recode treatment and replicate IDs
    group_by(Treatment_id) %>%
    mutate(TRTNO = cur_group_id()) %>% ungroup() %>%
    relocate(TRTNO, .before = everything()) %>%
    arrange(Year, TRTNO) %>%
    distinct()
  
  # Drop identifiers and tag
  DOB_tser_ipt <- lapply(DOBS_tser_ipt, function(df){
    pkeys <- get_pkeys(df, alternates = FALSE)
    df[!names(df) %in% c(pkeys, "tag")]
  })
  
  # Merge all dataframes
  DOB_tser_out <- Reduce(function(x, y)
    merge(x, y, by = c("Year", "Plot_id"), all = TRUE), DOB_tser_ipt,
    init = DESIGN_str %>% select(Year, Plot_id, Treatment_id, Rep_no)) %>%
    # Use design structure as init to have all years covered (necessary for later estimation of missing variables)
    # e.g., phenology
    # Rename and recode treatment and replicate IDs
    group_by(Treatment_id) %>%
    mutate(TRTNO = cur_group_id()) %>% ungroup() %>%
    relocate(TRTNO, .before = everything()) %>%
    arrange(Year, TRTNO) %>%
    distinct()
  
  
  
  # Format metadata output elements -----------------------------------------
  
  
  # Format provenance and identification metadata
  EXP_NAME = metadata$name
  PERSONS = metadata$contact_name
  EMAIL = metadata$contact_email
  ADDRESS = metadata$trial_institution  #TODO: Add address elements if more are added to the metadata schema
  DOI = metadata$doi
  
  # Site and plots details
  SITE <- metadata$site
  COUNTRY <- metadata$country
  PLTA = metadata$size_plots
  
  NOTES <- c(paste0("Data files mapped on ", Sys.Date(), " with csmTools"), paste0("Source data DOI: ", DOI))
  GENERAL <- data.frame(PERSONS = PERSONS,
                        EMAIL = EMAIL,
                        ADDRESS = ADDRESS,
                        SITE = SITE,
                        COUNTRY = COUNTRY,
                        PLTA = PLTA,
                        DOI = DOI,
                        NOTES = NOTES)
  
  
  
  # Format output -----------------------------------------------------------
  
  
  # Data sections
  TREATMENTS_matrix <- TREATMENTS_matrix %>%
    select(-c(Plot_id, "Treatment_id")) %>%
    distinct()
  
  MANAGEMENT <- append(MNGT_out, list(GENERAL, FIELDS, TREATMENTS_matrix), after = 0)
  names(MANAGEMENT)[1:3] <- c("GENERAL", "FIELDS", "TREATMENTS")
  
  OBSERVED <- list(Summary = DOB_suma_out, Time_series = DOB_tser_out)
  
  OTHER <- ATTR_ipt
  names(OTHER) <- paste0("OTHER_", names(ATTR_ipt))
  
  DATA_out <-
    append(MANAGEMENT, c(list(OBSERVED_Summary = DOB_suma_out),
                         list(OBSERVED_TimeSeries = DOB_tser_out),
                         OTHER),
           after = length(MANAGEMENT))
  
  # Metadata attributes
  attr(DATA_out, "EXP_DETAILS") <- EXP_NAME
  attr(DATA_out, "SITE_CODE") <- toupper(
    paste0(
      substr(SITE, 1, 2),
      countrycode(COUNTRY, origin = "country.name", destination = "iso2c")  # if input language is English
    ))
  
  return(DATA_out)
}