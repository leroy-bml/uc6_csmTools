#' Iteratively Merge Leaf Tables into a Database Backbone
#'
#' Iteratively merges "leaf" tables (those without parents) into their parent tables in a database, 
#' using primary keys and the \code{\link{merge_tbls}} function. The process continues until no mergeable leaves remain.
#'
#' @param db A named list of data frames representing the database.
#' @param drop_keys Logical. If \code{TRUE}, drops the join key columns after merging (default: \code{FALSE}).
#' @param exclude_tbls Optional character vector of table names to exclude from merging as leaves.
#' @param exclude_cols Optional character vector of column names to exclude from being used as join keys.
#'
#' @return A list of data frames representing the melted (fully merged) database.
#'
#' @details
#' The function:
#' \itemize{
#'   \item Identifies leaf tables (those with no parent in the database).
#'   \item Merges mergeable leaves into their parent tables using \code{\link{merge_tbls}}.
#'   \item Repeats the process until no mergeable leaves remain.
#'   \item Optionally drops join key columns after merging.
#'   \item Removes the \code{flag} column from tables if all values are \code{NA}.
#' }
#'
#' @examples
#' # Example usage (assuming get_pkeys, get_parent, and merge_tbls are defined):
#' # db <- list(
#' #   parent = data.frame(ID = 1:3, Value = c("A", "B", "C")),
#' #   child = data.frame(ID = 1:3, Data = c("X", "Y", "Z"))
#' # )
#' # melt_dataset(db)
#'
#' @export
#' 

melt_dataset <- function(db, drop_keys = FALSE, exclude_tbls = NULL, exclude_cols = NULL) {
  iteration <- 0
  
  repeat {
    iteration <- iteration + 1
    cat(sprintf("Iteration %d\n", iteration))
    
    # Identify parent for each table
    all_parents <- lapply(db, function(df) get_parent(df, db))
    
    # Identify leaf tables (no parent)
    leaf_tbls <- db[sapply(all_parents, is.null)]
    leaf_tbls <- leaf_tbls[!names(leaf_tbls) %in% exclude_tbls]
    branch_tbls <- db[!names(db) %in% names(leaf_tbls)]
    
    if (length(leaf_tbls) > 0) {
      # Identify which leaves are mergeable (their primary key is present in a branch table)
      leaves_mergeable <- leaf_tbls[
        sapply(leaf_tbls, function(df1) {
          pk <- get_pkeys(df1)
          any(sapply(branch_tbls, function(df2) any(pk %in% names(df2))))
        })
      ]
      leaves_unmergeable <- leaf_tbls[!names(leaf_tbls) %in% names(leaves_mergeable)]
    } else {
      leaves_mergeable <- character(0)
    }
    
    n_left <- length(leaves_mergeable)
    
    if (n_left == 0) {
      cat("No mergeable leaves left. Stopping.\n")
      break
    }
    
    # Merge mergeable leaves into the database
    res <- merge_tbls(
      db, leaves_mergeable,
      type = "child-parent",
      drop_keys = drop_keys,
      exclude_cols = exclude_cols
    )
    db <- res$db  # Update db with merged tables
    
    # Remove merged leaves from db
    db <- db[!names(db) %in% names(leaves_mergeable)]
  }
  
  # Drop DQ flags if no rows are flagged (all NAs)
  db <- lapply(db, function(df) {
    if ("flag" %in% names(df)) {
      df <- df[ c(setdiff(names(df), "flag"), "flag") ]
      if (all(is.na(df$flag))) {
        df$flag <- NULL
      }
    }
    return(df)
  })
  return(db)
}

#' TODO: UPDATE DOCUMENTATION Reshape crop experiment data into a standard ICASA format
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
#' @importFrom magrittr %>%
#' @importFrom lubridate as_date parse_date_time
#' @importFrom dplyr select group_by group_by_at ungroup mutate relocate distinct left_join arrange across cur_group_id
#' @importFrom tidyr any_of all_of everything ends_with
#' @importFrom rlang !! :=
#' @importFrom countrycode countrycode
#'
#'

# seehausen <- read_exp_data(dir = "./inst/extdata/lte_seehausen/0_raw", extension = "csv")  #check
# muencheberg <- read_exp_data(dir = "./inst/extdata/lte_muencheberg/0_raw", extension = "csv")  #check
# grossbeeren <- read_exp_data(dir = "./inst/extdata/lte_grossbeeren/0_raw", extension = "csv")  #check
# duernast <- read_exp_data(dir = "./inst/extdata/lte_duernast/0_raw", extension = "csv")  #check
# westerfeld <- read_exp_data(dir = "./inst/extdata/lte_westerfeld/0_raw", extension = "csv")  #check
# rauischholzhausen <- read_exp_data(dir = "./inst/extdata/lte_rauischholzhausen/0_raw", extension = "csv")  #check
# rostock <- read_exp_data(dir = "./inst/extdata/lte_rostock/0_raw", extension = "csv")  #check
# darmstadt_a <- read_exp_data(dir = "./inst/extdata/lte_darmstadt_a/0_raw", extension = "csv")  #check
# darmstadt_e <- read_exp_data(dir = "./inst/extdata/lte_darmstadt_e/0_raw", extension = "csv")  #data problem: PRUEFGLIED table missing
# + dikopshof
# + bad lauchstadt


reshape_exp_data <- function(db, metadata, mother_tbl_name) {
  
  # Identify the components of the experimental design ----------------------
  
  # db <- duernast  #tmp
  # mother_tbl_name <- "VERSUCHSAUFBAU"  #tmp
  
  design_tbl <- db[[mother_tbl_name]]
  
  # Identify core design elements: years, plots, treatments
  exp_str <- identify_exp_design(db, design_tbl)
  exp_str_ids <- exp_str$id_name  # unique identifiers
  exp_str_tbl_nms <- c(exp_str$tbl_name[!is.na(exp_str$tbl_name)], mother_tbl_name)  # table names
  
  # Identify column properties (primary/foreign keys, data attributes)
  # > Labels data attributes with table name as prefix to make them unique
  db <- identify_exp_attributes(db, str_cols = exp_str_ids)
  nokeys <- do.call(c, attr(db, "attributes"))  # list of unique data attributes
  
  # Get design tables
  exp_str_tbls <- db[exp_str_tbl_nms]
  

  # Melt dataset ------------------------------------------------------------
  
  # Melt structural tables into one table
  design_str <- melt_dataset(exp_str_tbls, drop_keys = FALSE)
  
  # Calculate replicate number (inconsistent concept across datasets)
  # TODO: CHECK variety/crop as treatment levels? not consistently?
  design_str <- lapply(design_str, function(df) {
    df %>%
      group_by(!!sym(exp_str_ids[1]), !!sym(exp_str_ids[3])) %>%
      mutate(Rep_no = row_number())
  })
  
  # Replace structural tables by unique merged design table
  db <- db[!names(db) %in% exp_str_tbl_nms]
  db <- c(design_str, db)
  
  # Melt dataset into immediate children only (tables linked directly to the design table)
  # TODO: double check get_parent
  db <- melt_dataset(db, drop_keys = TRUE, exclude_tbls = exp_str_tbl_nms, exclude_cols = c(exp_str_ids, nokeys))
  

  # # Give standard name to design variables ----------------------------------
  # 
  # 
  # raw_str_names <- c(YEARS_nm, PLOTS_nm, TREATMENTS_nm, REPS_nm)
  # std_str_names <- c("Year", "Plot_id", "Treatment_id", "Rep_no")
  # 
  # for (i in seq_along(db)) {
  #   for (j in seq_along(raw_str_names)) {
  #     if (raw_str_names[j] %in% colnames(db[[i]])) {
  #       colnames(db[[i]])[colnames(db[[i]]) == raw_str_names[j]] <- std_str_names[j]
  #     }
  #   }
  # }
  
  
  # Format dates ------------------------------------------------------------
  
  # Identify dates and convert into a usable format (yyyy-mm-dd; vMapper default)
  db <- lapply(db, function(df) {
    as.data.frame(
      lapply(df, function(x) {
        if (is_date(x)) {
          standardize_date(x, output_format = "Date")
        } else {
          x
        }
      }),
      stringsAsFactors = FALSE
    )
  })
  
  return(db)  #tmp
}


  
  # Identify management and observed data -----------------------------------
  
  # # Distinguish management and observed data
  # DATA_tbls_ident <- tag_data_type(db = db,
  #                                  years_col = exp_str$id_name[1],
  #                                  plots_col = exp_str$id_name[2],
  #                                  plots_len = 240,
  #                                  max_mods = mean(4, na.rm = TRUE))
  # # TODO: >2 date vars?
  # 
  # # Separate management and observed data
  # data_cats <- sapply(DATA_tbls_ident, function(df) attr(df, "category"))
  # 
  # MNGT_ipt <- DATA_tbls_ident[["management"]]
  # DOBS_suma_ipt <- DATA_tbls_ident[["observed_summary"]]
  # DOBS_tser_ipt <- DATA_tbls_ident[["observed_timeseries"]] ## some years summary, other timeseries
  # ATTR_ipt <- DATA_tbls_ident[["other"]]
  
  
  # # Extract metadata --------------------------------------------------------
  # 
  # # Extract field coordinates
  # 
  # path <- "./inst/extdata/lte_duernast/0_raw/7e526e38-4bf1-492b-b903-d8dbcfd36b6d.xml"
  # metadata <- read_metadata(path, repo = "bnr")
  # 
  # #NOTES <- c(paste0("Data files mapped on ", Sys.Date(), " with csmTools"), paste0("Source data DOI: ", DOI))
  # 
  # 
  # 
  # 
  # # Format management tables ------------------------------------------------
  # 
  # 
  # # Identify which management category correpond to the treatments
  # MNGT_is_trt <- lapply(MNGT_ipt, function(df) is_treatment(df, "Year", "Plot_id"))
  # # TODO: handle 0/1 treatments (so far not tagged as treatment)
  # # TODO: create treatment name in the function
  # 
  # # Merge management IDs with the treatments table
  # MNGT_ipt <- mapply(function(df1, df2) left_join(df1, df2, by = "Year"), MNGT_ipt, MNGT_is_trt)
  # 
  # # Add an ID per year for non-treatment management events
  # MNGT_fmt <- lapply(names(MNGT_ipt), function(df_name){
  #   
  #   df <- MNGT_ipt[[df_name]]
  #   
  #   #
  #   mngt_id <- get_pkeys(df, alternates = FALSE)
  #   
  #   # Sepcify name of the ID for the reduced table
  #   ID_nm <- paste0(toupper(substr(df_name, 1, 2)), "_ID")
  #   # Create the ID variable
  #   df <- df %>%
  #     # TODO: modify to handle variable factor levels
  #     left_join(DESIGN_str, by = c("Year", "Plot_id")) %>%
  #     group_by_at(c("Year", "Faktor1_Stufe_ID")) %>%
  #     mutate(ID_trt_1 = cur_group_id()) %>% ungroup() %>%
  #     group_by_at(c("Year", "Faktor2_Stufe_ID")) %>%
  #     mutate(ID_trt_2 = cur_group_id()) %>% ungroup() %>%
  #     group_by_at("Year") %>%
  #     mutate(ID_fix = cur_group_id()) %>% ungroup() %>%
  #     mutate(!!ID_nm := ifelse(is_trt, paste(ID_trt_1, ID_trt_2, sep = "_"), ID_fix)) %>%
  #     select(-c(ID_trt_1, ID_trt_2, ID_fix, is_trt, Faktor1_Stufe_ID, Faktor2_Stufe_ID)) %>%
  #     relocate(!!ID_nm, .before = everything())
  #   
  #   # Drop primary key, treatment and crop keys
  #   df <- df[!names(df) %in% c(mngt_id, "Treatment_id")]
  #   df <- df %>% arrange(.data[[ID_nm]])
  #   
  #   return(df)
  #   
  # })
  # names(MNGT_fmt) <- names(MNGT_ipt)
  # 
  # # List 1: reduced management tables
  # # (identified by unique management event features rather than event x plot combinations)
  # MNGT_out <- lapply(MNGT_fmt, function(df){ distinct(df[!names(df) == "Plot_id"]) })
  # names(MNGT_out) <- names(MNGT_fmt)
  # 
  # # List 2: IDs only (to update IDs in the treatment matrix)
  # MNGT_ids <- lapply(MNGT_fmt, function(df){
  #   mngt_id <- colnames(df)[1]
  #   df[names(df) %in% c(mngt_id, "Year", "Plot_id")]
  # })
  # 
  # 
  # 
  # # Format treatments matrix ------------------------------------------------
  # 
  # 
  # TREATMENTS_matrix <-
  #   # Append each management IDs to the correponding year x plot combination
  #   Reduce(function(x, y)
  #     merge(x, y, by = intersect(names(x), names(y)), all.x = TRUE),
  #     MNGT_ids, init = DESIGN_str) %>%
  #   select(-Rep_no) %>%  ##!! see if necessary
  #   distinct() %>%
  #   # Append FIELDS ids
  #   left_join(FIELDS_tbl %>%
  #               select(FL_ID, Plot_id),
  #             by = "Plot_id") %>%
  #   # Replace NA ids by 0 (= no management event)
  #   mutate(across(ends_with("_ID"), ~ifelse(is.na(.x), 0, .x))) %>%
  #   # Rename and recode treatment ID
  #   group_by(Treatment_id) %>%
  #   mutate(TRTNO = cur_group_id()) %>% ungroup() %>%
  #   #rename(REPNO = !!REPS_id) %>%
  #   relocate(TRTNO, .before = everything())
  # 
  # 
  # 
  # # Format observed data ----------------------------------------------------
  # 
  # 
  # # Drop identifiers and tag
  # DOB_suma_ipt <- lapply(DOBS_suma_ipt, function(df){
  #   pkeys <- get_pkeys(df, alternates = FALSE)
  #   df[!names(df) %in% c(pkeys, "tag")]
  # })
  # 
  # # Merge all dataframes
  # DOB_suma_out <- Reduce(function(x, y)
  #   merge(x, y, by = c("Year", "Plot_id"), all = TRUE), DOB_suma_ipt,
  #   init = DESIGN_str %>% select(Year, Plot_id, Treatment_id, Rep_no)) %>%
  #   # Rename and recode treatment and replicate IDs
  #   group_by(Treatment_id) %>%
  #   mutate(TRTNO = cur_group_id()) %>% ungroup() %>%
  #   relocate(TRTNO, .before = everything()) %>%
  #   arrange(Year, TRTNO) %>%
  #   distinct()
  # 
  # # Drop identifiers and tag
  # DOB_tser_ipt <- lapply(DOBS_tser_ipt, function(df){
  #   pkeys <- get_pkeys(df, alternates = FALSE)
  #   df[!names(df) %in% c(pkeys, "tag")]
  # })
  # 
  # # Merge all dataframes
  # DOB_tser_out <- Reduce(function(x, y)
  #   merge(x, y, by = c("Year", "Plot_id"), all = TRUE), DOB_tser_ipt,
  #   init = DESIGN_str %>% select(Year, Plot_id, Treatment_id, Rep_no)) %>%
  #   # Use design structure as init to have all years covered (necessary for later estimation of missing variables)
  #   # e.g., phenology
  #   # Rename and recode treatment and replicate IDs
  #   group_by(Treatment_id) %>%
  #   mutate(TRTNO = cur_group_id()) %>% ungroup() %>%
  #   relocate(TRTNO, .before = everything()) %>%
  #   arrange(Year, TRTNO) %>%
  #   distinct()
  # 
  # 
  # 
  # # Format metadata output elements -----------------------------------------
  # 
  # 
  # 
  # 
  # 
  # # Format output -----------------------------------------------------------
  # 
  # 
  # # Data sections
  # TREATMENTS_matrix <- TREATMENTS_matrix %>%
  #   select(-c(Plot_id, "Treatment_id")) %>%
  #   distinct()
  # 
  # MANAGEMENT <- append(MNGT_out, list(GENERAL, FIELDS, TREATMENTS_matrix), after = 0)
  # names(MANAGEMENT)[1:3] <- c("GENERAL", "FIELDS", "TREATMENTS")
  # 
  # OBSERVED <- list(Summary = DOB_suma_out, Time_series = DOB_tser_out)
  # 
  # OTHER <- ATTR_ipt
  # names(OTHER) <- paste0("OTHER_", names(ATTR_ipt))
  # 
  # DATA_out <-
  #   append(MANAGEMENT, c(list(OBSERVED_Summary = DOB_suma_out),
  #                        list(OBSERVED_TimeSeries = DOB_tser_out),
  #                        OTHER),
  #          after = length(MANAGEMENT))
  # 
  # # Metadata attributes
  # attr(DATA_out, "EXP_DETAILS") <- EXP_NAME
  # attr(DATA_out, "SITE_CODE") <- toupper(
  #   paste0(
  #     substr(SITE, 1, 2),
  #     countrycode(COUNTRY, origin = "country.name", destination = "iso2c")  # if input language is English
  #   ))
  # 
  # return(DATA_out)

