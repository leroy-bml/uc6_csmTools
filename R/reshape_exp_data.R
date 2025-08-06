#' Iteratively Merge Leaf Tables into a Database Backbone
#'
#' Iteratively merges "leaf" tables (those without children and potentially standalone tables) into their parent tables in a database,
#' using primary keys and a referential merging strategy. The process continues until no mergeable tables remain.
#' This function aims to "flatten" a relational database structure into a more consolidated format.
#'
#' @param db A named list of data frames representing the database. Each data frame corresponds to a table.
#' @param drop_keys Logical. If \code{TRUE}, drops the join key columns after successful merges (default: \code{FALSE}).
#' @param exclude_keys Optional character vector of column names to exclude from being considered or used as join keys during the merge process.
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

recursive_merge <- function(db, force_keys = NULL, exclude_keys = NULL, drop_keys = FALSE) {
  
  # Sort tables based on referencing
  cats <- categorize_tables(db)
  roots <- names(cats[cats %in% "root"])
  
  # Merge all roots to prevent downstream redundancies
  res <- merge_db_tables(
    db_tables = db[roots],
    start_tables = db,
    type = "referential",
    force_keys = force_keys,
    exclude_keys = exclude_keys,
    drop_keys = drop_keys
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
      force_keys = force_keys,
      exclude_keys = exclude_keys,
      drop_keys = drop_keys
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

reshape_exp_data <- function(db, mother_xml, design_tbl_name, data_model = "bonares-lte_de", header_type = "long") {

  # Extract metadata --------------------------------------------------------
  cat("\033[34mDownload metadata...\033[0m\n")
  
  xml <- read_metadata(mother_path = mother_xml, schema = "bonares")
  db <- c(db, list(METADATA = xml$metadata$METADATA,
                   PERSONS = xml$metadata$PERSONS,
                   INSTITUTIONS = xml$metadata$INSTITUTIONS))  # append
  
  
  # Identify experimental design --------------------------------------------
  cat("\033[34mIdentify experimental design...\033[0m\n")
  
  design_tbl <- db[[design_tbl_name]]
  
  # Identify core design elements: years, plots, treatments
  exp_str <- identify_exp_design(db, design_tbl)
  exp_str_ids <- exp_str$id_name  # unique identifiers
  
  # Identify column properties (primary/foreign keys, data attributes)
  # > Labels data attributes with table name as prefix to make them unique
  db <- identify_exp_attributes(db, str_cols = exp_str_ids)
  nokeys <- do.call(c, attr(db, "attributes"))  # list of unique data attributes
  
  # Recursive merge ---------------------------------------------------------
  cat("\033[34mMerge dataset recursively...\033[0m\n")
  
  db <- recursive_merge(db, force_keys = exp_str_ids, exclude_keys = nokeys, drop_keys = FALSE)
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
  
  # Map data and apply rowwise transformations ------------------------------
  cat("\033[34mMap variables...\033[0m\n")

  db_icasa <- map_data2(dataset = db,
                        input_model = data_model,
                        output_model = "icasa",
                        header_type = header_type)
  #return(db_icasa)

  # Apply summary transformations -------------------------------------------
  cat("\033[34mTransform variables...\033[0m\n")
  out <- format_to_model(dataset = db_icasa,
                         data_model = "icasa",
                         header_type = header_type)
  
  return(out)
}

#' Reformat and Aggregate Experimental Data for a Target Model
#'
#' This function processes a list of raw experimental data tables and transforms
#' them into a structured format suitable for simulation models, specifically
#' the ICASA standard. It splits data into logical sections, aggregates key
#' tables (fields, weather), and normalizes complex management schedules by
#' converting event sequences into distinct management regimes.
#'
#' @details
#' The core transformation performed by this function is the aggregation of
#' management events into "regimes". For each management type (e.g.,
#' fertilization, tillage), it identifies unique sequences of operations
#' applied to plots or treatments. These unique sequences (regimes) are
#' assigned a numeric level. The main `TREATMENTS` table is then updated to
#' map each experimental plot to its corresponding management regime levels.
#' This simplifies the data structure by abstracting detailed event schedules
#' into a cross-referenced matrix.
#'
#' The function also performs the following pre-processing steps:
#' - Aggregates spatial data in the `FIELDS` table by year.
#' - Extracts year information from date columns in the `WEATHER_METADATA` table.
#'
#' @section TODO:
#' A list of known issues and areas for future improvement:
#' - **General:** The aggregation logic currently hardcoded could be moved to a
#'   more flexible `yaml` configuration file.
#' - **Fields:** Implement a function to handle different coordinate systems and
#'   group field coordinates more robustly.
#' - **Soil:** The soil data section (`SOIL_METADATA`, `SOIL_LAYERS`) is not yet
#'   processed. The unpractical profile/analysis/measured split in the ICASA
#'   standard needs to be handled.
#'
#' @param dataset A list of dataframes containing experimental data. Each
#'   dataframe in the list should correspond to a table from the ICASA suite
#'   (e.g., `TREATMENTS`, `FIELDS`, `FERTILIZERS`, etc.).
#' @param data_model A character string specifying the target data model.
#'   Currently a placeholder for `"icasa"`.
#' @param header_type A character string, either `"long"` or `"short"`, that
#'   specifies the column naming convention to use for key identifiers (e.g.,
#'   `"experiment_year"` vs. `"EXP_YEAR"`).
#'
#' @return A list of dataframes conforming to the target data model structure.
#'   The list includes the original data sections, but with the management
#'   tables redefined as regime definitions and an updated `TREATMENTS` matrix
#'   that links plots to these regimes.
#'
#' @importFrom magrittr %>%
#' @importFrom rlang !! !!! := syms sym
#' @importFrom dplyr group_by across all_of summarise n mutate select distinct arrange ungroup any_of left_join
#' @importFrom lubridate year
#' @export


format_to_model <- function(dataset, data_model = "icasa", header_type) {
  
  # HACK: Refresh design variables
  # TODO: Replace by aggregate phase in yaml
  if (header_type == "long") {
    year_col <- "experiment_year"
    treatment_col <- "treatment_number"
    plot_col <- "plot_id"
    field_id <- "field_level"
    field_lat <- "field_latitude"
    field_lon <- "field_longitude"
    field_elev <- "field_elevation"
  } else {
    year_col <- "EXP_YEAR"
    treatment_col <- "TRTNO"
    plot_col <- "PLTID"
    field_id <- "FL"
    field_lat <- "FL_LAT"
    field_lon <- "FL_LONG"
    field_elev <- "FLELE"
  }
  
  # Split data into major ICASA sections
  metadata <- dataset[intersect(c("GENERAL", "PERSONS", "INSTITUTIONS", "DOCUMENTS", "PLOT_DETAILS"), names(dataset))]
  measured <- dataset[intersect(c("SUMMARY", "TIME_SERIES"), names(dataset))]
  weather <- dataset[intersect(c("WEATHER_METADATA", "WEATHER_DAILY"), names(dataset))]
  soil <- dataset[intersect(c("SOIL_METADATA", "SOIL_LAYERS"), names(dataset))]
  #TODO: handle soil data: unpractical profile/analysis/measured split in ICASA
  management_sec <- c("GENOTYPES", "FIELDS", "SOIL_ANALYSES", "INITIAL_CONDITIONS", "PLANTINGS",
                      "IRRIGATIONS", "FERTILIZERS", "ORGANIC_MATERIALS", "MULCHES", "CHEMICALS",
                      "TILLAGE", "ENVIRON_MODIFICATIONS", "HARVESTS", "SIMULATION_CONTROLS","TREATMENTS")
  management <- dataset[intersect(management_sec, names(dataset))]
  
  # Post-processing FIELDS table
  management[["FIELDS"]] <- management[["FIELDS"]] %>%
    # TODO: Add here function to split coordinates into distinct groups (regardless of coord system)
    group_by(across(all_of(year_col))) %>%
    summarise(
      !!field_lat := mean(latitude, na.rm = TRUE),
      !!field_lon := mean(longitude, na.rm = TRUE),
      # More direct calculation for the mean of midpoint elevations
      !!field_elev := mean((elevation_min + elevation_max) / 2, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    group_by(across(everything())) %>%
    summarise(!!field_id := n(), .groups = "drop")
  
  # Aggregate WEATHER_METADATA table
  is_date_col <- apply(weather[["WEATHER_METADATA"]], 2, is_date)
  wth_date_col <- names(is_date_col[is_date_col])
  weather[["WEATHER_METADATA"]] <- weather[["WEATHER_METADATA"]] %>%
    mutate(!!year_col := year(!!sym(wth_date_col))) %>%
    select(-all_of(wth_date_col)) %>%
    distinct()
  
  # TODO: add SOIL_LAYER/SOIL_METADATA 
  
  # Aggregate management table
  format_management <- function(ls, year_col, treatment_col, plot_col) {
    
    str_cols <- c(year_col, treatment_col, plot_col)
    
    management_fmt <- list()
    management_matrix <- ls[["TREATMENTS"]]
    management <- ls[setdiff(names(ls), "TREATMENTS")]
    
    # Standardize join key types in the main matrix to character to prevent join errors.
    management_matrix <- management_matrix %>% mutate(across(any_of(str_cols), as.character))
    
    for (i in 1:length(management)) {
      df <- management[[i]]
      
      levels <- c("genotype_level", "field_level", "soil_analysis_level", "initial_conditions_level", "planting_level",
                  "irrigation_level", "fertilizer_level", "org_materials_applic_lev", "mulch_level", "chemical_applic_level",
                  "tillage_level", "environmental_modif_lev", "harvest_operations_level", "simulation_control_level")
      pkey <- intersect(colnames(df), levels)
      
      # Skip tables that don't have a primary key for management levels
      if (length(pkey) == 0) next
      
      comment_cols <- colnames(df[grepl("NOTE|COMMENT", colnames(df), ignore.case = TRUE)])
      str_cols_valid <- intersect(str_cols, names(df))
      
      # Standardize join key types in the event table to match the matrix.
      df <- df %>% mutate(across(any_of(str_cols_valid), as.character))
      
      grp_cols <- setdiff(colnames(df), c(str_cols, pkey, comment_cols))
      date_cols <- names(which(sapply(df, is.Date)))
      
      # FIX for "TODO: not correct"
      # If no grouping columns exist, the table is a simple map between plot/treatment and a management level.
      # Use it to update the matrix directly.
      if (length(grp_cols) == 0) {
        map_table <- df %>% select(any_of(c(str_cols_valid, pkey))) %>% distinct()
        management_matrix <- management_matrix %>%
          select(-any_of(pkey)) %>%
          left_join(map_table, by = str_cols_valid)
        management_fmt[[i]] <- df %>% select(any_of(c(year_col, pkey))) %>% distinct()
        next
      }
      
      df <- df %>%
        mutate(event_signature = do.call(paste, c(.[grp_cols], sep = "_")))
      
      df <- df %>%
        # FIX for "TODO: arrange by date only when applicable"
        # Use arrange(across(...)) which safely handles cases where date_cols is empty.
        arrange(across(all_of(c(str_cols_valid, date_cols)))) %>%
        group_by(!!!syms(str_cols_valid)) %>%
        mutate(event_regime_signature = paste(event_signature, collapse = "|")) %>%
        ungroup()
      
      regime_ids <- df %>%
        select(all_of(year_col), event_regime_signature) %>%
        distinct() %>%
        group_by(!!sym(year_col)) %>%
        mutate(!!sym(pkey) := as.integer(factor(event_regime_signature))) %>%
        ungroup()
      
      df <- df %>%
        select(-!!sym(pkey)) %>%
        # FIX for "TODO: force format on join keys" - join keys are pre-formatted to character
        left_join(regime_ids, by = c(year_col, "event_regime_signature"))
      
      mngt_event_regime <- df %>%
        select(any_of(str_cols_valid), !!sym(pkey)) %>%
        distinct()
      
      management_matrix <- management_matrix %>%
        select(-any_of(pkey)) %>%
        left_join(mngt_event_regime, by = str_cols_valid) %>%
        # NA levels are true 0s (no management)
        mutate(across(
          any_of(levels),
          ~ replace(., is.na(.), 0)
        ))
      
      management_fmt[[i]] <- df %>%
        select(!!sym(year_col), !!sym(pkey), all_of(grp_cols)) %>%
        # Use of `if` statement correctly handles cases with no date columns
        arrange(!!sym(year_col), !!sym(pkey), if(length(date_cols) > 0) across(all_of(date_cols))) %>%
        distinct()
    }
    names(management_fmt) <- names(management)
    
    management_matrix <- management_matrix %>% distinct()
    
    list(management = management_fmt, management_matrix = management_matrix)
  }
  management_fmt <- format_management(management, year_col, treatment_col, plot_col)
  
  management <- management_fmt$management
  management_matrix <- management_fmt$management_matrix
  
  out <- list(metadata, management, list(TREATMENTS = management_matrix), weather, soil, measured)
  out <- do.call(c, out)
  
  return(out)
}