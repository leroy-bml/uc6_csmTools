#' Iteratively Merge Leaf Tables into a Database Backbone
#'
#' Iteratively merges "leaf" tables (those without children and potentially standalone tables) into their parent tables in a database,
#' using primary keys and a referential merging strategy. The process continues until no mergeable tables remain.
#' This function aims to "flatten" a relational database structure into a more consolidated format.
#'
#' @param db A named list of data frames representing the database. Each data frame corresponds to a table.
#' @param drop_keys Logical. If \code{TRUE}, drops the join key columns after successful merges (default: \code{FALSE}).
#' @param exclude_cols Optional character vector of column names to exclude from being considered or used as join keys during the merge process.
#'
#' @return A list of data frames representing the melted (fully merged) database. The number of tables in the output
#'         will typically be less than or equal to the input, with related data consolidated.
#'
#' @details
#' The function operates in an iterative loop, attempting to merge tables based on their relationships:
#'
#' * **Initial Root Merge:** It first identifies and merges tables categorized as "root" (tables that are referenced by other tables but do not themselves reference any other tables within the database backbone). This initial step helps to consolidate base data.
#' * **Iterative Branch and Leaf Merging:** In a `repeat` loop, the function continuously categorizes tables as "branch" (tables that reference other tables and are referenced by others) or "leaf"/"standalone" (tables that reference other tables but are not referenced by any).
#'     * It identifies "leaf" and "standalone" tables that can be merged into existing "branch" tables based on referential integrity.
#'     * These identified mergeable tables are then merged into the `db` using `merge_db_tables` with a "referential" type.
#'     * The loop continues until no more "leaf" or "standalone" tables can be merged, indicating that the database structure has been consolidated as much as possible through this method.
#' * **Flag Column Cleanup:** Finally, after all merges are complete, it checks for a `flag` column in each resulting data frame. If the `flag` column contains only `NA` values, it is removed, otherwise it is retained and moved to the end of the data frame.
#'
#' This function relies on several internal helper functions: `categorize_tables()`, `merge_db_tables()`, `get_pkeys()`, and `check_ref_integrity()`.
#'
#' @export
#'

recursive_merge <- function(db, drop_keys = FALSE, exclude_cols = NULL) {

  cat("Merging dataset recursively....\n")
  
  # Sort tables based on referencing
  cats <- categorize_tables(db)
  roots <- names(cats[cats %in% "root"])
  
  # Merge all roots to prevent downstream redundancies
  res <- merge_db_tables(
    db_tables = db[roots],
    start_table = db,
    type = "referential",
    drop_keys = drop_keys,
    exclude_cols = exclude_cols
  )
  db <- res$results_df
    
  iteration <- 0
  
  repeat {
    iteration <- iteration + 1
    cat(sprintf("Iteration %d\n", iteration))
    
    # Sort tables based on referencing
    cats <- categorize_tables(db)
    branches <- names(cats[cats %in% "branch"])
    branch_tbls <- db[branches]
    unrefs <- names(cats[cats %in% c("leaf", "standalone")])
    unref_tbls <- db[unrefs]
    
    if (length(unref_tbls) > 0) {
      # Identify which leaves reference a branch table
      # NB: include standalone tables so they are not dropped from the output
      unref_mergeable <- unref_tbls[
        sapply(unref_tbls, function(df1) {
          pk <- get_pkeys(df1)
          any(sapply(branch_tbls, function(df2) check_ref_integrity(df1, df2, pk)))
        })
      ]
    } else {
      unref_mergeable <- character(0)
    }
    
    n_left <- length(unref_mergeable)
    
    if (n_left == 0) {
      cat("No mergeable leaves left. Stopping.\n")
      break
    }
    
    # Merge mergeable roots into the database
    res <- merge_db_tables(
      db_tables = db,
      start_table = unref_mergeable,
      type = "referential",
      drop_keys = drop_keys,
      exclude_cols = exclude_cols
    )
    db <- res$results_df
  }
  
  # Drop DQ flags if no rows are flagged (all NAs)
  db <- lapply(db, function(df) {
    if ("flag" %in% names(df)) {
      df <- df[ c(setdiff(names(df), "flag"), "flag") ]
      if (all(is.na(df$flag))) {
        df$flag <- NULL
      }
    }
    df
  })
  return(db)
}


#' Split a Data Frame into Multiple Sub-Data Frames by Group Mapping
#'
#' This function splits a data frame into a list of sub-data frames based on groupings defined in a mapping data frame.
#' Each sub-data frame contains columns associated with a specific group, as well as any columns not assigned to any group (i.e., with \code{NA} as group).
#'
#' @param df A data frame to be split. Columns in this data frame should correspond to the \code{icasa_header_short} values in \code{map}.
#' @param map A data frame containing at least the columns \code{icasa_group} and \code{icasa_header_short}. This mapping defines which columns in \code{df} belong to which group.
#'
#' @details
#' The function works as follows:
#'
#' * It filters the `map` data frame to include only those rows where `icasa_header_short` matches a column in `df`.
#' * It identifies all unique, non-`NA` group names from `icasa_group`.
#' * Columns with `NA` in `icasa_group` are considered "no-group" columns and are included in every sub-data frame.
#' * For each identified group, a sub-data frame is created containing all columns mapped to that group, plus all "no-group" columns.
#' * The result is a named list of sub-data frames, with names corresponding to the group names.
#'
#' @return
#' A named list of data frames. Each element corresponds to a group defined in `map$icasa_group`, containing the relevant columns from `df`.
#'
#' @importFrom magrittr %>%
#' @importFrom dplyr filter select distinct
#'

project_dataframe <- function(df, map) {
  
  groups_df <- map %>%
    filter(icasa_header_short %in% colnames(df)) %>%
    select(icasa_group, icasa_header_short) %>%
    distinct()
  
  group_names <- na.omit(unique(groups_df$icasa_group))
  
  # Get columns with no group (NA; to include in all splits)
  na_cols <- groups_df$icasa_header_short[is.na(groups_df$icasa_group)]
  
  # Split df by group (keep no-group cols in each sub-df)
  split_dfs <- lapply(group_names, function(grp) {
    group_cols <- groups_df$icasa_header_short[groups_df$icasa_group == grp]
    cols <- unique(c(na_cols, group_cols))
    cols <- cols[cols %in% colnames(df)]
    df[, cols, drop = FALSE]
  })
  names(split_dfs) <- group_names
  
  return(split_dfs)
}


#' Split and Merge a List of Data Frames by Group Mapping
#'
#' This function takes a named list of data frames (typically representing sections of a dataset, already mapped to ICASA headers)
#' and further structures them into distinct ICASA groups (sections) by splitting and then merging fragments.
#'
#' @param db A named list of data frames, where each data frame represents a section of a dataset
#'           (e.g., "PLOT_DETAILS", "SOIL_MEASUREMENTS"), with column names already mapped to ICASA headers.
#' @param map A data frame containing the mapping information, typically loaded via `load_map()`.
#'            It must contain at least `section`, `header`, `icasa_header_short`, and `icasa_group` columns,
#'            which define how columns in input data frames should be grouped into output sections.
#'
#' @details
#' The function processes the input `db` list to consolidate data into final ICASA sections.
#'
#' **Processing Steps:**
#'
#' * **Metadata Handling:** The "METADATA" section (if present) is temporarily set aside and re-added at the end.
#' * **Data Projection:** For each remaining data frame in `db`, the `project_dataframe()` helper function is called.
#'     This function is expected to split the input data frame into multiple sub-data frames (fragments),
#'     each corresponding to a specific ICASA group (e.g., "SOIL_PROFILE", "WEATHER_DAILY") based on the `map`.
#' * **Flattening and Grouping:** All resulting sub-data frames from all input sections are combined into a single flat list.
#'     These fragments are then grouped by their target ICASA group names.
#' * **Merging Fragments:** For each unique ICASA group (e.g., "SOIL_PROFILE"), all data frame fragments belonging to that group
#'     are merged using a full outer join (`merge(..., all = TRUE)`) on their common columns.
#'     Duplicate rows resulting from the merge are then removed.
#' * **Data Cleaning and Key Filtering:**
#'     * For all merged sections *except* "TREATMENTS", columns that contain only `NA` values are removed using `remove_all_na_cols()`.
#'     * Rows with missing values in their primary key (as determined by `get_pkeys()`) are filtered out from these sections.
#' * **Final Assembly:** The initially extracted "METADATA" section, the cleaned data sections, and the "TREATMENTS" section
#'     (which is not subjected to primary key filtering by this function) are combined into the final output list.
#'
#' This function is crucial for transforming section-based data into the hierarchical ICASA group structure.
#'
#' @return
#' A named list of data frames, where each element represents a consolidated ICASA group (section).
#' For example, `output$SOIL_PROFILE` would contain all soil profile data gathered from various input sections.
#'
#'
#' @importFrom magrittr %>%
#' @importFrom dplyr filter
#' @importFrom rlang !! sym
#'

project_dataset <- function(db, map = load_map()) {

  # db <- db_icasa  #tmp
  # map <- load_map()
  
  # Exclude metadata from breakdown
  metadata <- db["METADATA"]
  db <- db[setdiff(names(db), "METADATA")]

  # Explode each section using the map
  projected_sec <- lapply(names(db), function(sec) {
    project_dataframe(db[[sec]], map)
  })
  
  # Flatten the list and assign names
  sec_tbls <- do.call(c, projected_sec)
  names(sec_tbls) <- unlist(lapply(projected_sec, names))
  sec_tbls <- split(sec_tbls, names(sec_tbls))
  
  db_out <- do.call(c, db_exploded)
  names(db_out) <- unlist(lapply(db_exploded, names))
  db_out <- split(db_out, names(db_out))
  
  # Merge fragments within each section (full join on common columns)
  merged_sec <- lapply(sec_tbls, function(tbl_list) {
    merged <- Reduce(function(x, y) {
      cmn <- intersect(names(x), names(y))
      merge(x, y, by = cmn, all = TRUE)
    }, tbl_list)
    unique(merged)
  })
  
  # Clean up: remove all-NA columns and handle primary keys
  data_tbls <- merged_sec[setdiff(names(merged_sec), "TREATMENTS")]
  data_tbls <- lapply(names(data_tbls), function(nm) {
    df <- data_tbls[[nm]]
    df <- remove_all_na_cols(df)
    pkey <- get_pkeys(df, alternates = FALSE, ignore_na = TRUE)
    if (length(pkey) > 0) {
      df <- df %>% filter(!is.na(!!sym(pkey)))
    } 
    df
  })
  names(data_tbls) <- setdiff(names(merged_sec), "TREATMENTS")
  
  out <- c(metadata, data_tbls, merged_sec["TREATMENTS"])
  
  return(out)
}


#' Transform BonaRes LTE dataset into the ICASA-format
#' 
#' This function identifies the components of a crop experiment data set, map terms into ICASA terminology 
#' and re-arranges the table according to the ICASA data model
#' (see ICASA standards: https://github.com/DSSAT/ICASA-Dictionary).
#'
#' @details
#' The `reshape_exp_data` function performs a series of steps to transform a raw crop experiment dataset,
#' typically provided as a list of data frames, into a structure compliant with the ICASA standards.
#'
#' **Workflow:**
#'
#' * **Metadata Extraction:** Reads metadata from a specified XML file (`mother_xml`) and appends it to the `db` list.
#' * **Experimental Design Identification:** Uses the `design_tbl_name` to identify the core components 
#'     of the experimental design, such as years, plots, and treatments. It also identifies primary and foreign keys and data attributes within the dataset.
#' * **Data Merging and Cleaning:** Recursively merges data frames in `db` based on identified keys
#'     and removes columns that contain only `NA` values.
#' * **Date Formatting:** Standardizes all date columns across the dataset to the 'yyyy-mm-dd' format.
#' * **ICASA Mapping:** Maps column names within each data frame to their corresponding ICASA headers
#'     based on the specified `data_model`. This step uses `load_map()` and `map_data()`.
#' * **Dataset Projection:** Further structures the data into distinct ICASA sections (e.g., 'PLOT_DETAILS', 'METADATA',
#'     'SUMMARY', 'TIME_SERIES', 'WEATHER_METADATA', 'WEATHER_DAILY', 'SOIL_METADATA', 'SOIL_LAYERS', and various 'MANAGEMENT' sections) using `project_dataset()`.
#' * **Management Data Formatting:** A crucial step is the reformatting of management data.
#'     This involves identifying unique event regimes within each experimental year and assigning unique IDs. It also creates a "TREATMENTS" matrix that links experimental years, plots, treatments, and their associated management regimes.
#'
#' The function aims to automate the structural transformation, assuming that variable content is consistent with the chosen `data_model`. Users may need to perform additional variable mapping or data cleaning outside this function if their data deviates significantly from the expected structure or if more granular control over variable transformation is required.
#'
#'
#' @param db a list of data frames composing a crop experiment dataset.
#' @param mother_xml a path or URL to the metadata XML file of the mother table.
#' @param design_tbl_name a character string specifying the name of the data frame within `db` that serves as the mother table (experimental design).
#' @param data_model a character string specifying the data model of the input dataset (e.g., "bonares-lte_de").
#'
#' @return a list containing the reshaped crop experiment data in ICASA format.
#'
#' @importFrom magrittr %>%
#' @importFrom tidyselect all_of
#' @importFrom dplyr filter mutate select arrange group_by ungroup distinct left_join
#' @importFrom rlang !! !!! := sym syms
#'
#' @export
#' 

reshape_exp_data <- function(db, mother_xml, design_tbl_name, data_model = "bonares-lte_de") {

  # Extract metadata --------------------------------------------------------
  
  xml <- read_metadata(mother_path = mother_xml, schema = "bonares")
  db <- c(db, list(METADATA = xml$metadata))  # append
  
  
  # Identify experimental design --------------------------------------------
  
  # Identify the components of the experimental design ----------------------
  
  design_tbl <- db[[design_tbl_name]]
  
  # Identify core design elements: years, plots, treatments
  exp_str <- identify_exp_design(db, design_tbl)
  exp_str_ids <- exp_str$id_name  # unique identifiers
  
  # Identify column properties (primary/foreign keys, data attributes)
  # > Labels data attributes with table name as prefix to make them unique
  db <- identify_exp_attributes(db, str_cols = exp_str_ids)
  nokeys <- do.call(c, attr(db, "attributes"))  # list of unique data attributes
  
  db <- recursive_merge(db, drop_keys = FALSE, exclude_cols = nokeys)
  db <- lapply(db, remove_all_na_cols)  # Remove only-NA artefact columns
  

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
  
  
  # Map data to ICASA -------------------------------------------------------

  db_icasa <- lapply(names(db), function(sec) {
    df <- db[[sec]]
    
    # TODO: prevent retaining unused section OBSERV_DATA_LINKS (why??) and GENERAL
    map_sub <- load_map() %>%
      filter(data_model == data_model) %>%
      filter(!is.na(icasa_header_short)) %>%
      filter(section %in% c(!!sec, "ACROSS") & header %in% colnames(df))
    
    # NB: tmp version before codes and units are properly formatted
    map_data(df,
             input_model = data_model,
             output_model = "icasa",
             map = map_sub,
             keep_unmapped = FALSE)  # TODO: TRUE does not work?
  })
  names(db_icasa) <- names(db)
  
  
  # Explode data frame into ICASA sections ----------------------------------
  
  db_icasa <- project_dataset(db_icasa, map = load_map())   # here we stand
  
  
  # Explode data frame into ICASA sections ----------------------------------
  
  format_management <- function(ls) {
    
    # Format management tables and matrix as ICASA
    # >> Tables: unique event regimes within year
    # >> Matrix: link between year, plot, and treatment and management regimes
    #ls <- management
    management_fmt <- list()
    management_matrix <- ls[["TREATMENTS"]]
    management <- ls[setdiff(names(ls), "TREATMENTS")]
    
    for (i in 1:length(management)) {
      df <- management[[i]]
      
      pkey <- setdiff(intersect(colnames(df), colnames(management_matrix)), c("EXP_YEAR","TRTNO","PLOT_ID"))
      # Handle special case when 2 keys are present (GE and another)
      if (length(pkey) > 1) pkey <- get_pkeys(df)
      
      # Exclude comments from event columns
      comment_cols <- colnames(df[grepl("NOTES|COMMENT", colnames(df))])
      grp_cols <- setdiff(colnames(df), c("EXP_YEAR", "TRTNO", "PLOT_ID", pkey, comment_cols))
      is_date_col <- apply(df, 2, is_date)
      date_cols <- names(is_date_col[is_date_col])
      
      # Go next if tables have no attributes besides structural cols and pkeys
      # TODO: not correct
      if (length(grp_cols) == 0) {
        management_fmt[[i]] <- df
        next 
      }
      
      # Create a signature for each event (row) ---
      df <- df %>%
        mutate(event_signature = do.call(paste, c(.[grp_cols], sep = "_")))
      
      # For each plot-EXP_YEAR, concatenate all event_signatures (ordered by PDATE or row order) ---
      df <- df %>%
        arrange(EXP_YEAR, PLOT_ID, !!!syms(date_cols)) %>%  # TODO: arrange by date only when applicable
        group_by(EXP_YEAR, PLOT_ID) %>%
        mutate(event_regime_signature = paste(event_signature, collapse = "|")) %>%
        ungroup()
      
      # Assign event_regime_IDs per EXP_YEAR (identical regimes get same ID within EXP_YEAR) ---
      regime_ids <- df %>%
        select(EXP_YEAR, event_regime_signature) %>%
        distinct() %>%
        group_by(EXP_YEAR) %>%
        mutate(!!sym(pkey) := as.integer(factor(event_regime_signature))) %>%
        ungroup()
      
      # Join event_regime_ID back to main table ---
      df <- df %>%
        select(-!!sym(pkey)) %>%
        left_join(regime_ids, by = c("EXP_YEAR", "event_regime_signature"))
      
      # Treatment-to-event_regime mapping (one row per plot per EXP_YEAR)
      mngt_event_regime <- df %>%
        select(EXP_YEAR, TRTNO, PLOT_ID, !!sym(pkey)) %>%
        distinct()
      management_matrix <- management_matrix %>%
        select(-!!sym(pkey)) %>%
        left_join(mngt_event_regime, by = c("EXP_YEAR", "TRTNO", "PLOT_ID"))
      
      # B. Event details table (one row per event, grouped by event_regime_ID within EXP_YEAR)
      management_fmt[[i]] <- df %>%
        select(EXP_YEAR, !!sym(pkey), all_of(grp_cols)) %>%
        distinct()
    }
    names(management_fmt) <- names(management)
    
    management_matrix <- management_matrix %>% distinct()
    
    list(management = management_fmt, management_matrix = management_matrix)
  }
  
  metadata <- db_icasa[intersect(c("PLOT_DETAILS", "METADATA"), names(db_icasa))]
  measured <- db_icasa[intersect(c("SUMMARY", "TIME_SERIES"), names(db_icasa))]
  tofix <- db_icasa[intersect(c("GENERAL", "OBSERV_DATA_LINKS"), names(db_icasa))]
  #TODO: prevent mapping
  weather <- db_icasa[intersect(c("WEATHER_METADATA", "WEATHER_DAILY"), names(db_icasa))]
  soil <- db_icasa[intersect(c("SOIL_METADATA", "SOIL_LAYERS"), names(db_icasa))]
  #TODO: handle soil data: unpractical profile/analysis/measured split in ICASA
  management <- db_icasa[setdiff(names(db_icasa),
                                 unlist(sapply(list(metadata, measured, weather, soil, tofix), names)))]
  
  management_fmt <- format_management(management)
  management <- management_fmt$management
  management_matrix <- management_fmt$management_matrix
  
  out <- list(metadata, management, list(TREATMENTS = management_matrix), weather, soil, measured)
  out <- do.call(c, out)
  
  return(out)
}


##---- TMP DEV

# # BonaRes specific adjustments
# if (grepl("bonares-lte", input_model)) {
#   
#   if ("FERTILIZERS" %in% names(management_fmt)) {
#     management_fmt[["FERTILIZERS"]] <- management_fmt[["FERTILIZERS"]] %>% filter(MIN == 1)
#   }
#   if ("ORGANIC_MATERIALS" %in% names(management_fmt)) {
#     management_fmt[["ORGANIC_MATERIALS"]] <- management_fmt[["ORGANIC_MATERIALS"]] %>% filter(ORG == 1)
#   }
# }

