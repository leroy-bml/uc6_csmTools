#' Remove Artefact Columns and Rows from a Data Frame
#'
#' Cleans a data frame by removing columns with missing or unnamed headers and rows that are entirely NA.
#'
#' @param df A data frame to be cleaned.
#'
#' @details
#' The function first replaces any missing column names with "unnamed" and ensures all column names are unique (without sanitizing special characters). It then removes columns whose names start with "unnamed" and drops any rows where all values are NA.
#'
#' This is useful for cleaning up data frames imported from files that may contain artefact columns (e.g., from extra delimiters or empty columns) or rows.
#'
#' The function uses the \strong{dplyr} package for row filtering.
#'
#' @return A cleaned data frame with artefact columns and all-NA rows removed.
#'
#' @examples
#' library(dplyr)
#' df <- data.frame(a = c(1, NA), b = c(NA, NA), c = c(2, 3))
#' colnames(df)[2] <- NA
#' drop_artefacts(df)
#' # Returns a data frame with only columns 'a' and 'c', and only rows with at least one non-NA value
#'
#' @importFrom magrittr %>%
#' @importFrom dplyr filter
#' 

drop_artefacts <- function(df) {

  # Identify artefact columns
  colnames(df) <- ifelse(is.na(colnames(df)), "unnamed", colnames(df))
  
  # Ensure uniqueness without sanitizing special characters (in some ICASA names; alternative to make.names)
  colnames(df) <- make.unique(colnames(df))
  
  # Remove unnammed and full NA columns
  df <- df[ , !grepl("^unnamed", colnames(df))]
  df <- df %>% filter(rowSums(is.na(.)) != ncol(.))
  
  return(df)
}


#' Extract and Organize Code Lists from a the ICASA Template Workbook
#'
#' Reads code lists from the "DropDown" sheet of the ICASA template workbook and organizes them into a named list of data frames.
#'
#' @param wb A workbook object, as accepted by \code{wb_to_df}.
#'
#' @details
#' The function reads the "DropDown" sheet from the provided workbook, starting from the second row. It removes columns ending with \code{\"_sort\"} (used for data entry helpers), then splits the remaining columns into separate data frames for each code list, based on column name prefixes. Each resulting data frame contains at least a "desc" (description) and "code" column, and is named according to its prefix. The "country" code list is excluded from the result.
#'
#' This is useful for extracting and organizing code lists from standardized data entry templates.
#'
#' The function uses the \strong{dplyr} package for data manipulation.
#'
#' @return A named list of data frames, each representing a code list from the template workbook.
#'
#' @examples
#' \dontrun{
#' wb <- openxlsx::loadWorkbook(\"template.xlsx\")
#' codes_ls <- get_template_codes(wb)
#' names(codes_ls)
#' # [1] \"crop\" \"soil\" \"site\" ...
#' }
#'
#' @importFrom magrittr %>%
#' @importFrom openxlsx2 wb_to_df
#' @importFrom dplyr select all_of filter ends_with
#' 

get_template_codes <- function(wb){
  
  codes <- wb_to_df(wb, sheet = "DropDown", startRow = 2)
  
  # Drop template helper columns (only for data entry)
  codes <- codes %>% select(-ends_with("_sort"))
  
  codes_prefixes <- unique(sub("_.*", "", names(codes)))
  
  # split into separate dataframe for each code list
  codes_ls <- lapply(codes_prefixes, function(prefix) {
    
    cols <- grep(paste0("^", prefix, "(_|$)"), names(codes), value = TRUE)
    df <- codes %>% select(all_of(cols))
    
    if (ncol(df) >= 2) {
      names(df)[1:2] <- c("desc", "code")
      df <- df %>% filter(!is.na(.[[1]]))
    } else if (ncol(df) == 1) {
      #names(df)[1] <- "desc"
      df <- data.frame(desc = character(), codes = character())
    }
    
    return(df)
  })
  names(codes_ls) <- codes_prefixes

  codes_ls <- codes_ls[!names(codes_ls) %in% "country"]
  #codes_ls$country <- codes_df$country %>%
  #  mutate(code = countrycode(desc, origin = "country.name", destination = "genc2c"))
  
  return(codes_ls)
}


#' Extract and Process Data from a the ICASA Template Workbook
#'
#' Loads, cleans, and processes data from the ICASA template Excel workbook, mapping descriptions to ICASA codes and formatting according to the ICASA data model.
#'
#' @param path Character. Path to the template Excel workbook.
#' @param exp_id Character vector or NA. Experiment ID(s) to extract. If NA (default), all experiments are returned.
#' @param headers Character. Which column header format to use in the output: \code{"short"} (default) or \code{"long"}.
#'
#' @details
#' The function loads the workbook, extracts all relevant (capitalized) sheets, and cleans each data frame by removing artefact columns and rows. It maps template dropdown list descriptions to ICASA codes, deletes empty data frames, and formats treatment and event tables. Metadata is structured according to the ICASA data model. If \code{headers} is \code{"short"}, column names are mapped to their short format using the template dictionary.
#'
#' The function returns a list of experiments, each as a list of processed data frames.
#'
#' The function uses several helper functions, including \code{wb_load}, \code{wb_get_sheet_names}, \code{wb_to_df}, \code{drop_artefacts}, \code{get_template_codes}, \code{desc_to_codes}, \code{format_envmod_tbl}, \code{format_treatment_str}, \code{format_events}, \code{structure_metadata}, \code{icasa_long_to_short}, and \code{split_experiments}.
#'
#' @return A named list of experiments, each containing a list of processed data frames for each section of the template.
#'
#' @examples
#' \dontrun{
#' out <- extract_template("template.xlsx")
#' names(out)
#' # [1] "EXP001" "EXP002" ...
#' }
#'
#' @importFrom magrittr %>%
#' @importFrom dplyr mutate filter
#' @importFrom openxlsx2 wb_load wb_get_sheet_names wb_to_df
#' 
#' @export
#' 

extract_template <- function(path = NULL, exp_id = NA_character_, headers = c("short", "long")){
  
  # Fetch template
  if (is.null(path)) {
    tpl <- fetch_template()
    if (tpl$created) {
      stop("Newly crearted template contains no data! Please fill it in and rerun.")
    }
    path <- tpl$path
  } else {
    user_template <- file.path(Sys.getenv("HOME"), ".csmTools/template_icasa_vba.xlsm")
    warning(
      "You have supplied a custom template file path. ",
      "Be aware that this file will not be updated automatically with package updates. ",
      "For the latest supported version, use the default user template at: ", user_template 
    )
  }
  
  # Load workbook and set name criterion for sheets to be loaded (capitalized sheet names)
  wb <- suppressWarnings(wb_load(path))
  nms <- wb_get_sheet_names(wb)
  nms_data <- nms[grepl("^[A-Z_]+$", nms)]
  
  # Extract all sections and section names
  dfs <- list()
  for (i in 1:length(nms_data)){
    df <- suppressWarnings(
      wb_to_df(wb, sheet = nms_data[i], startRow = 4, detect_dates = TRUE)
    )
    #attr(df, "section") <- nms_data[i]
    df <- drop_artefacts(df)
    
    dfs[[i]] <- df
    names(dfs)[i] <- nms_data[i]
  }

  # Map template dropdown list descriptions into ICASA codes
  codes <- suppressWarnings(
    get_template_codes(wb)
  )
  dfs <- lapply(dfs, function(df){ desc_to_codes(df, codes) })
  
  # Delete empty dataframes
  # TODO: quality check if mandatory data is input
  #dfs <- dfs[!sapply(dfs, function(df) all(is.null(df) || dim(df) == c(0, 0)) || all(is.na(df)))]
  dfs <- Filter(function(df) !is.null(df) || !all(dim(df) == c(0, 0)) || !all(is.na(df)), dfs)
  
  # Replace treatment names by treatment number in the treatment matrix
  dfs <- format_envmod_tbl(dfs)
  dfs <- format_treatment_str(dfs)
  
  # Replace null events (e.g., "rainfed" irrigation input)
  dfs <- format_events(dfs, "FERTILIZERS", "fertilizer_level", "fert_applied", "FERTILIZER_APPLICS")  #check warnings
  dfs <- format_events(dfs, "IRRIGATIONS", "irrigation_level", "irrig_applied", "IRRIGATION_APPLICATIONS")  #TODO: check warnings
  
  # Format metadata
  dfs <- structure_metadata(dfs, data_model = "icasa")
  
  # Map column headers to short format if applicable
  # TODO: extensive testing (e.g., no provenance sheets or empty)
  dict <- wb_to_df(wb, sheet = "Dictionary", startRow = 1) %>%
    # Change provenance section to experiment (now that provenance info has been incorporated to SOIL and WEATHER metadata)
    mutate(Sheet = ifelse(Sheet %in% c("PERSONS","INSTITUTIONS","DOCUMENTS"), "EXP_METADATA", Sheet)) %>%
    filter(!var_order_custom == "-99" | is.na(var_order_custom))  # tmp: preserve NAs until measured data all sorted in template
  if(headers == "short") {
    dfs <- mapply(FUN = icasa_long_to_short,
                  df = dfs,
                  section = names(dfs),
                  MoreArgs = list(dict = dict, keep_unmapped = FALSE)
    )
  }

  # Split experiments
  out <- split_experiments(dfs)
  if(!all(is.na(exp_id))){
    out <- out[exp_id %in% names(out)]
  } 

  return(out)
}


#' Map Long Column Names to ICASA Short Codes for a Template Section
#'
#' Renames columns in a data frame from long descriptive names to ICASA short codes, using a dictionary for a specific template section.
#'
#' @param df A data frame with columns to be renamed.
#' @param section Character. The name of the template section (sheet) to use for mapping.
#' @param dict A data frame containing the mapping dictionary, with columns \code{Sheet}, \code{var_name}, and \code{Code_Query}.
#' @param keep_unmapped Logical. If \code{FALSE}, only columns with a mapped ICASA code are retained. If \code{TRUE} (default), unmapped columns are kept with their original names.
#'
#' @details
#' The function generates a mapping vector from the dictionary for the specified section, then renames columns in \code{df} accordingly. If \code{keep_unmapped} is \code{FALSE}, only columns with a mapped ICASA code are retained in the output.
#'
#' This is useful for standardizing column names in data extracted from template workbooks to the ICASA data model.
#'
#' The function uses the \strong{dplyr} package for column renaming.
#'
#' @return A data frame with columns renamed to ICASA short codes, and optionally filtered to only mapped columns.
#'
#' @examples
#' library(dplyr)
#' df <- data.frame(LongName1 = 1:3, LongName2 = 4:6)
#' dict <- data.frame(
#'   Sheet = "SECTION",
#'   var_name = c("LongName1", "LongName2"),
#'   Code_Query = c("LN1", "LN2")
#' )
#' icasa_long_to_short(df, section = "SECTION", dict = dict)
#' # Returns a data frame with columns "LN1" and "LN2"
#'
#' @importFrom magrittr %>%
#' @importFrom dplyr rename_with
#' 

icasa_long_to_short <- function(df, section, dict, keep_unmapped = TRUE){

  # Generate mapping vector for the focal section
  subset <- dict[dict$Sheet == section,]
  rename_vector <- setNames(subset$Code_Query, subset$var_name)
  rename_vector <- rename_vector[!is.na(rename_vector)]  # remove additional terms with no ICASA code (added to structure the template)
  rename_vector <- rename_vector[names(rename_vector) %in% names(df)]  # keep only columns present in the dataframe
  
  # Perform the mapping
  df <- df %>% rename_with(~ rename_vector[.x], .cols = names(rename_vector))
  
  if (keep_unmapped == FALSE) df <- df[colnames(df) %in% rename_vector]
  
  return(df)
}


#' Replace categorical variable Values with corresponding ICASA Codes
#'
#' Recodes categorical columns in a data frame by replacing descriptive values with corresponding codes from a provided list of code data frames.
#'
#' @param df A data frame whose categorical columns (character or factor) may contain descriptive values to be recoded.
#' @param codes A named list of data frames, each with at least \code{desc} (description) and \code{code} columns, as produced by \code{get_template_codes}.
#'
#' @details
#' For each character or factor column in \code{df}, the function checks if any values match the \code{desc} column in any of the code data frames in \code{codes}. If a match is found, the descriptive value is replaced with the corresponding code. If no match is found, the original value is retained.
#'
#' This is useful for standardizing data entry by mapping human-readable descriptions to standardized codes.
#'
#' @return A data frame with descriptive values replaced by codes where applicable.
#'
#' @examples
#' df <- data.frame(
#'   crop = c(\"Maize\", \"Wheat\", \"Rice\"),
#'   stringsAsFactors = FALSE
#' )
#' codes <- list(
#'   crop = data.frame(desc = c(\"Maize\", \"Wheat\", \"Rice\"), code = c(\"MZ\", \"WH\", \"RC\"))
#' )
#' desc_to_codes(df, codes)
#' # Returns a data frame with crop codes: \"MZ\", \"WH\", \"RC\"
#'

desc_to_codes <- function(df, codes) {
  
  # TODO: homogenize with existing recoding function
  # Iterate through each column of the data df
  for (col in names(df)) {
    # Check if the column is categorical (character or factor)
    if (is.character(df[[col]]) || is.factor(df[[col]])) {
      # Iterate through each dataframe in the code list
      for (code_df in codes) {
        # Check if the column contains any of the variable names in the code dataframe
        if (any(df[[col]] %in% code_df$desc)) {
          # Replace the descriptive names with the corresponding codes
          
          if (length(df[[col]]) == 0) {
            message("Column ", col, " is empty.")
          } else {
            df[[col]] <- vapply(df[[col]], function(x) {
              match_index <- match(x, code_df$desc)
              if (!is.na(match_index)) {
                as.character(code_df$code[match_index])
              } else {
                as.character(x)
              }
            }, FUN.VALUE = character(1))
          }
        }
      }
    }
  }
  return(df)
}


#' Format ICASA Treatment Matrix and Related Tables in a List
#'
#' Cleans and standardizes the treatment matrix and related measured data tables within a list of data frames, ensuring numeric columns and removing extra string content.
#'
#' @param ls A named list of data frames, typically representing sections of an experimental template, including a "TREATMENTS" matrix and measured data tables.
#'
#' @details
#' The function processes the "TREATMENTS" data frame by:
#' \itemize{
#'   \item Removing any content after a pipe (\code{|}) in character columns.
#'   \item Trimming leading whitespace from all columns.
#'   \item Replacing \code{NA} values in level columns (\code{genotype_level_number} to \code{mulch_level}) with 0.
#'   \item Converting relevant columns to numeric type.
#'   \item Ensuring \code{simulation_control_level} is numeric and defaults to 1 if missing.
#' }
#' It also processes measured data tables (those with names matching \code{TS_}, \code{SM_}, or \code{OBSERV_}) by cleaning and converting the \code{treatment_number} column.
#'
#' The cleaned data frames replace the originals in the input list.
#'
#' The function uses the \strong{dplyr} package for data manipulation.
#'
#' @return The input list with formatted "TREATMENTS" and measured data tables.
#'
#' @examples
#' \dontrun{
#' ls <- format_treatment_str(ls)
#' }
#'
#' @importFrom magrittr %>%
#' @importFrom tidyselect everything
#' @importFrom dplyr mutate across where
#' 

format_treatment_str <- function(ls){

  # Format treatment matrix
  if("TREATMENTS" %in% names(ls)){
    df <- ls[["TREATMENTS"]]
  } else {
    error("Treatment matrix not found.")
  }
  
  df <- df %>%
    mutate(across(where(is.character), ~ sub("\\|.*", "", .)),
           across(everything(), ~ trimws(., which = "left")),
           across(genotype_level_number:mulch_level, ~ifelse(is.na(.x), 0, .x)),
           across(c(treatment_number, genotype_level_number:mulch_level), ~as.numeric(.))) %>%
    mutate(simulation_control_level = as.numeric(
      ifelse(is.na(simulation_control_level), 1, simulation_control_level))
    )
  
  ls[["TREATMENTS"]] <- df  # Replace original matrix
  
  # Format measured data
  obs <- ls[which(grepl("TS_|SM_|OBSERV_", names(ls)))]
  obs <- lapply(obs, function(df){
    df <- df %>%
      mutate(treatment_number = sub("\\|.*", "", treatment_number),
             treatment_number = as.numeric(treatment_number))
  })
  ls[names(obs)] <- obs  # Replace original tables
  
  return(ls)
}


#' Format the ICASA Environmental Modification Levels Table in a List
#'
#' Transforms the "ENVIRON_MODIF_LEVELS" data frame in a list to a wide format, renaming columns for ICASA compatibility.
#'
#' @param ls A named list of data frames, typically representing sections of an experimental template, including "ENVIRON_MODIF_LEVELS".
#'
#' @details
#' If the list contains a data frame named "ENVIRON_MODIF_LEVELS", this function:
#' \itemize{
#'   \item Removes the \code{environ_parameter_unit} column.
#'   \item Pivots the table to a wide format, with columns for each environmental parameter's modification code and value.
#'   \item Renames columns to use the \code{environ_modif_code} and \code{environ_modif} prefixes for ICASA compatibility.
#' }
#' The transformed data frame replaces the original in the input list.
#'
#' The function uses the \strong{dplyr} and \strong{tidyr} packages for data manipulation.
#'
#' @return The input list with the "ENVIRON_MODIF_LEVELS" table formatted in wide format and renamed columns.
#'
#' @examples
#' \dontrun{
#' ls <- format_envmod_tbl(ls)
#' }
#'
#' @importFrom magrittr %>%
#' @importFrom dplyr select
#' @importFrom tidyr pivot_wider
#'

format_envmod_tbl <- function(ls){
  
  if("ENVIRON_MODIF_LEVELS" %in% names(ls)){

    df <- ls[["ENVIRON_MODIF_LEVELS"]]
    
    df <- df %>%
      select(-environ_parameter_unit) %>%
      pivot_wider(names_from = environ_parameter, values_from = c(modif_code, environ_parameter_value))
    
    colnames(df) <- gsub(pattern = "modif_code", replacement = "environ_modif_code", colnames(df))
    colnames(df) <- gsub(pattern = "environ_parameter_value", replacement = "environ_modif", colnames(df))

    ls[["ENVIRON_MODIF_LEVELS"]] <- df
  }
  
  return(ls)
}


#' Format Event Tables by Handling Non-Applied Levels in Experimental Data
#'
#' Updates event header, application, and treatment tables in a list to handle cases where certain event levels (e.g., fertilizer or irrigation) are not applied.
#'
#' @param ls A named list of data frames, typically representing sections of an experimental template, including event headers, application tables, and the treatment matrix.
#' @param type Character. The name of the event header table (e.g., \code{"FERTILIZERS"}, \code{"IRRIGATIONS"}).
#' @param head_key Character. The name of the column in the event header table that identifies the event level (e.g., \code{"fertilizer_level"}, \code{"irrigation_level"}).
#' @param applied_key Character. The name of the column in the event header table indicating whether the event was applied (e.g., \code{"fert_applied"}, \code{"irrig_applied"}).
#' @param applics_key Character. The name of the application events table (e.g., \code{"FERTILIZER_APPLICS"}, \code{"IRRIGATION_APPLICATIONS"}).
#'
#' @details
#' For the specified event type, the function:
#' \itemize{
#'   \item Identifies event levels that were not applied (where \code{applied_key} == "N").
#'   \item Updates the event header table, setting the event level to 0 for non-applied levels and re-indexing levels.
#'   \item Updates the application events and treatment matrix tables to reflect the new event level indices.
#'   \item Removes non-applied levels from the event header and application tables.
#' }
#' The cleaned data frames replace the originals in the input list.
#'
#' The function uses the \strong{dplyr} package for data manipulation.
#'
#' @return The input list with updated event header, application, and treatment tables.
#'
#' @examples
#' \dontrun{
#' ls <- format_events(ls, "FERTILIZERS", "fertilizer_level", "fert_applied", "FERTILIZER_APPLICS")
#' }
#'
#' @importFrom magrittr %>%
#' @importFrom rlang !! := sym
#' @importFrom dplyr mutate arrange group_by ungroup left_join select filter coalesce all_of
#' 
  
format_events <- function(ls, type, head_key, applied_key, applics_key) {
  
  if (type %in% names(ls)) {
    head <- ls[[type]]
    applics <- ls[[applics_key]]
    treatments <- ls[["TREATMENTS"]]
    
    no_app_id <- head[head[[applied_key]] == "N", head_key]
    
    if (length(no_app_id) > 0) {
      
      # Update header levels
      head <- head %>%
        mutate(old_level = !!sym(head_key)) %>%
        mutate(!!sym(head_key) := ifelse(!!sym(head_key) == no_app_id, 0, !!sym(head_key))) %>%
        arrange(!!sym(head_key)) %>%
        group_by(old_level) %>%
        mutate(!!sym(head_key) := cur_group_id() - 1) %>%
        ungroup()
      
      # Update application events
      if (!is.null(applics)) {
        applics <- applics %>%
          left_join(head %>% select(all_of(head_key), old_level), by = setNames("old_level", head_key)) %>%
          mutate(!!sym(head_key) := coalesce(.data[[paste0(head_key, ".y")]], .data[[head_key]])) %>%
          select(-all_of(paste0(head_key, ".y"))) %>%
          filter(.data[[head_key]] > 0)
      }
      
      # Update treatment matrix
      treatments <- treatments %>%
        left_join(head %>% select(all_of(head_key), old_level), by = setNames("old_level", head_key)) %>%
        mutate(!!sym(head_key) := coalesce(.data[[paste0(head_key, ".y")]], .data[[head_key]])) %>%
        select(-all_of(paste0(head_key, ".y")))
      
      # Clean header
      head <- head %>%
        filter(.data[[head_key]] > 0) %>%
        select(-old_level)
      
      ls[[type]] <- head
      ls[[applics_key]] <- applics
      ls[["TREATMENTS"]] <- treatments
    }
  }
  
  return(ls)
}


#' Concatenate Non-Constant Columns by Group in a Data Frame
#'
#' Groups a data frame by columns with constant values and concatenates the remaining columns within each group.
#'
#' @param df A data frame to process.
#'
#' @details
#' The function identifies columns in \code{df} that have the same value in every row (constant columns). It then groups the data frame by these constant columns and, for each group, concatenates the values of the remaining columns into comma-separated strings.
#'
#' This is useful for summarizing data where some columns are invariant within groups, and you want to aggregate the other columns as concatenated lists.
#'
#' The function uses the \strong{dplyr} package for data manipulation.
#'
#' @return A data frame grouped by the constant columns, with other columns concatenated as comma-separated strings.
#'
#' @examples
#' library(dplyr)
#' df <- data.frame(
#'   id = c(1, 1, 2, 2),
#'   value1 = c('a', 'b', 'c', 'd'),
#'   value2 = c('x', 'y', 'z', 'w')
#' )
#' concatenate_per_group(df)
#' # Returns a data frame with one row per id, and value1/value2 as comma-separated strings
#'
#' @importFrom magrittr %>%
#' @importFrom tidyselect everything
#' @importFrom dplyr across n_distinct select_if group_by summarise all_of
#' 

# TODO: add_property_mapping <- function(name, unit, icasa = list(), agg_funs = list())
concatenate_per_group <- function(df) {
  
  constant_cols <- df %>%
    summarise(across(everything(), ~ n_distinct(.) == 1)) %>% 
    select_if(~ .) %>%
    colnames()
  
  other_cols <- base::setdiff(colnames(df), constant_cols)
  
  out <- df %>% 
    group_by(across(all_of(constant_cols))) %>%
    summarise(across(all_of(other_cols), ~ paste(., collapse = ", ")), .groups = 'drop')
  
  return(out)
}


#' Strictly Abbreviate a String to a Minimum Length
#'
#' Produces a strict abbreviation of a string, ensuring the result does not exceed the specified minimum length.
#'
#' @param string Character vector. The string(s) to abbreviate.
#' @param minlength Integer. The minimum (and maximum) length of the abbreviation. Default is 2.
#'
#' @details
#' The function uses \code{abbreviate} with \code{strict = TRUE} to generate abbreviations. If the resulting abbreviation exceeds \code{minlength}, it is truncated to exactly \code{minlength} characters.
#'
#' This is useful for generating short, fixed-length codes or labels from longer strings.
#'
#' @return A character vector of abbreviated strings, each of length \code{minlength}.
#'
#' @examples
#' strict_abbreviate("Wheat", minlength = 3)
#' # Returns "Whe"
#' strict_abbreviate(c("Maize", "Rice"), minlength = 2)
#' # Returns c("Ma", "Ri")
#'

strict_abbreviate <- function(string, minlength = 2) {
  abbr <- abbreviate(string, minlength = minlength, strict = TRUE)
  if (nchar(abbr) > minlength) {
    abbr <- substr(abbr, 1, minlength)
  }
  return(abbr)
}


#' Format and Merge Observed Data Sections in a List
#'
#' Merges related observed data tables (e.g., summary and time series) in a list of data frames, producing unified "SUMMARY" and "TIME_SERIES" sections.
#'
#' @param ls A named list of data frames, typically representing sections of an experimental template, including observed data tables.
#'
#' @details
#' The function searches for tables in the list whose names match "SM_" (summary) or "TS_" (time series), as well as "OBSERV_DATA_LINKS". It merges these tables by their common columns using \code{left_join}, producing unified "SUMMARY" and "TIME_SERIES" data frames. If the merged table has the same columns as "OBSERV_DATA_LINKS", it is omitted (returns \code{NULL}).
#'
#' After merging, the function removes all original observed data tables (those matching "SM_", "TS_", or "OBS") from the list, leaving only the merged "SUMMARY" and "TIME_SERIES" sections.
#'
#' The function uses the \strong{dplyr} package for joining data frames.
#'
#' @return The input list with merged "SUMMARY" and "TIME_SERIES" data frames, and observed data tables removed.
#'
#' @examples
#' \dontrun{
#' ls <- format_observed_data(ls)
#' }
#'
#' @importFrom dplyr left_join
#'

format_observed_data <- function(ls){
  
  merge_obs <- function(section_nm) {
    
    pattern <- paste0(section_nm, "|OBSERV_DATA_LINKS")
    section <- ls[grepl(pattern, names(ls))]
    
    section <- Reduce(function(x, y)
      left_join(x, y, by = intersect(names(x), names(y))),
      c(section[1], section[-1]))
    
    if (identical(colnames(section), colnames(ls$OBSERV_DATA_LINKS))) { 
      return(NULL) 
    } else { 
      return(section) 
    }
  }
    
    ls$SUMMARY <- merge_obs("SM_")
    ls$TIME_SERIES <- merge_obs("TS_")
    
    ls <- ls[!grepl("SM_|TS_|OBS", names(ls))]
    
    return(ls)
}


#' TEMP reshape function from icasa to dssat approach;
#' later integrate to a generic melt-explode-map sequence
#' 
#' 
#' @importFrom magrittr %>%
#' @importFrom dplyr filter select distinct

reshape_to_model_dssat <- function(vector, map_path, input_model = "icasa", output_model = "dssat") {
  
  # NB: temporary approach; later integrate to a generic melt-explode-map sequence
  
  map <- load_map(map_path) %>%
    filter(dataModel == output_model) %>%
    select(template_section, section) %>%
    distinct()
  
  # Create a named vector for easy lookup
  rename_map <- setNames(map$section, map$template_section)
  
  # Rename elements of the input vector using the lookup table
  renamed_vector <- sapply(vector, function(x) {
    if (x %in% names(rename_map)) {
      rename_map[x]
    } else {
      x
    }
  })
  
  names(renamed_vector) <- NULL
  return(renamed_vector)
}


#' Combine List Elements with Duplicate Names by Merging Data Frames
#'
#' Merges data frames in a list that share the same name, joining them by their common columns.
#'
#' @param ls A named list of data frames, possibly containing multiple data frames with the same name.
#'
#' @details
#' The function identifies all unique names in the list. For each name, it collects all data frames with that name, determines their common columns, and merges them using \code{merge} with \code{all = TRUE} (full outer join) on the common columns. The result is a new list with one merged data frame per unique name.
#'
#' This is useful for consolidating data from dual-tier or multi-section templates where sections may be split across multiple data frames with the same name.
#'
#' @return A named list of merged data frames, one for each unique name in the input list.
#'
#' @examples
#' df1 <- data.frame(a = 1:2, b = c(\"x\", \"y\"))
#' df2 <- data.frame(a = 3:4, b = c(\"z\", \"w\"))
#' ls <- list(SECTION = df1, SECTION = df2)
#' combine_dual_tier(ls)
#' # Returns a list with one element \"SECTION\", containing the merged data frame
#'

combine_dual_tier <- function(ls) {
  
  unique_names <- unique(names(ls))  # find unique names
  
  out <- list()
  for (name in unique_names) {
    
    section_dfs <- ls[names(ls) == name]  # filter dataframes with the same name
    common_cols <- Reduce(intersect, lapply(section_dfs, colnames))
    
    joined_dfs <- Reduce(function(x, y) merge(x, y, by = common_cols, all = TRUE), section_dfs)  # merged sections
    out[[name]] <- joined_dfs
  }
  return(out)
}


#' Remap and Reshape Dataset Between Data Models Using a Mapping File
#'
#' Transforms a dataset from one data model (e.g., ICASA) to another (e.g., DSSAT) using a mapping file, reshaping and renaming sections as needed.
#'
#' @param dataset A named list of data frames representing the dataset to be remapped.
#' @param map_path Character. Path to the mapping file used for variable and section translation.
#' @param input_model Character. The name of the input data model (default: \code{"icasa"}).
#' @param output_model Character. The name of the output data model (default: \code{"dssat"}).
#'
#' @details
#' The function first formats observed data into "SUMMARY" and "TIME_SERIES" tables, then renames sections to match the output model. For each section, it loads the mapping file and applies the mapping to translate variables and structure. Dual-tier sections (e.g., header + events/layers) are combined, and metadata is structured according to the output data model.
#'
#' The function uses several helper functions, including \code{format_observed_data}, \code{reshape_to_model_dssat}, \code{load_map}, \code{map_data}, \code{combine_dual_tier}, and \code{structure_metadata}.
#'
#' @return A named list of data frames, structured and mapped according to the output data model.
#'
#' @examples
#' \dontrun{
#' remapped <- remap(dataset, map_path = "mapping.csv", input_model = "icasa", output_model = "dssat")
#' }
#'
#' @importFrom magrittr %>%
#' @importFrom dplyr filter
#' 
#' @export
#' 

remap <- function(dataset, map_path, input_model = "icasa", output_model = "dssat"){
  
  # # Map data within target section
  # if (is.data.frame(dataset)) {
  #   
  #   map <- load_map(map_path)
  #   fdata <- map_data(dataset, input_model, output_model, map = map,
  #                     keep_unmapped = FALSE,  col_exempt = c("INST_NAME", "WST_NAME"))
  #       
  # }
  
  # Reshape to data model (ICASA -> DSSAT)
  rdata <- format_observed_data(dataset)  # All measured data into SUMMARY and TIME_SERIES tables
  names(rdata) <- reshape_to_model_dssat(vector = names(rdata))  # Rename sections #TMP
  
  mdata <- list()
  for (i in 1:length(rdata)){
    
    sec <- names(rdata)[i]
    map <- load_map(map_path) 
    sub <- map %>% filter(section %in% sec)
    
    # TODO: solve problem, bug when SUMMARY only contains TRNO
    mdata[[i]] <- map_data(rdata[[i]], input_model, output_model, map = sub,
                           # Keep institute name for DSSAT file metadata
                           keep_unmapped = FALSE, col_exempt = c("INST_NAME", "WST_NAME"))  #TODO: add FLL1;2;3 and revert mapping?
    names(mdata)[i] <- sec
  }
  
  # Combine dual tier sections (header + events/layers)
  fdata <- combine_dual_tier(mdata)
  
  # Structure metadata as DSSAT files
  fdata <- structure_metadata(fdata, data_model = "dssat")
  
  return(fdata)
}


#' Split a List of Data Frames into Experiments with Associated Soil and Weather Data
#'
#' Splits a list of data frames into separate experiments, attaching relevant soil and weather data to each experiment based on identifier columns.
#'
#' @param ls A named list of data frames, typically representing sections of an experimental template, including experiment, soil, and weather data.
#'
#' @details
#' The function determines the appropriate identifier columns for experiments, soil, and weather data based on the column names in the first data frame. It splits experiment data frames by experiment ID, then attaches soil and weather data frames that match the relevant profile or station IDs for each experiment. All-NA columns in measured data sections are removed.
#'
#' This is useful for organizing a combined dataset into a list of experiments, each with its associated environmental data, for further analysis or export.
#'
#' The function uses the \strong{dplyr} package for data manipulation and relies on the helper function \code{revert_list_str}.
#'
#' @return A list of lists, each representing an experiment with its associated data frames.
#'
#' @examples
#' \dontrun{
#' experiments <- split_experiments(ls)
#' }
#'
#' @importFrom magrittr %>%
#' @importFrom dplyr filter
#' 

# NB: fails when applied to the mapped dataset with unmapped variables kept, due to duplicate name in multiple table (ELEV.x, ELEV.y)
split_experiments <- function(ls) {

  # Set the splitting factor (IDs) for each data type (experiment, soil, weather)
  if(all(grepl("^[A-Z_]+$", colnames(ls[[1]])))){
    fcts <- c(exp = "EID", sol = "SOIL_SUBSET", wth = "WTH_SUBSET")
  } else {
    fcts <- c(exp = "experiment_ID", sol = "soil_identifier", wth = "weather_sta_identifier")
  }
  
  #1- split EID - nonEDI
  exp_dfs <- Filter(function(df) fcts[["exp"]] %in% names(df), ls)
  env_dfs <- Filter(function(df) !fcts[["exp"]] %in% names(df), ls)
  
  exp_dfs_split <- lapply(exp_dfs, function(df) split(df, f = df[[fcts[["exp"]]]]))
  exp_dfs_split <- revert_list_str(exp_dfs_split)

  # Initialize lists to store lookup values/dataframes
  sol_split <- wth_split <- c()
  
  for (i in 1:length(exp_dfs_split)){
    
    # Identify profile and station IDs for each experiment
    profile_id <- exp_dfs_split[[i]]$FIELDS[[fcts[["sol"]]]]
    station_id <- exp_dfs_split[[i]]$FIELDS[[fcts[["wth"]]]]
    
    # Keep only soil data records containing the profile ID
    sol_split[[i]] <- lapply(env_dfs, function(df) {
      
      matched_col <- names(which(sapply(df, function(col) any(col %in% profile_id))))
      
      if (all(!is.na(profile_id)) && length(matched_col) > 0) {
        df %>% filter(.data[[matched_col[1]]] %in% profile_id)
        # NB: [1] in case soil/weather data names are identical to IDs (wrong practice)
      } else {
        NULL
      }
    })
    
    # Remove empty dataframes
    sol_split[[i]] <- Filter(Negate(is.null), sol_split[[i]])
    
    # Keep only weather data records containing the station ID
    wth_split[[i]] <- lapply(env_dfs, function(df) {
      
      matched_col <- names(which(sapply(df, function(col) any(col %in% station_id))))
      
      if (all(!is.na(station_id)) && length(matched_col) > 0) {
        df %>% filter(.data[[matched_col[1]]] %in% station_id)
        # NB: [1] in case soil/weather data names are identical to IDs (wrong practice)
      } else {
        NULL
      }
    })
    
    # Remove empty dataframes
    wth_split[[i]] <- Filter(Negate(is.null), wth_split[[i]])
    
    # Append data when it exists
    if (length(sol_split[[i]]) > 0 || length(wth_split[[i]]) > 0) {
      exp_dfs_split[[i]] <- c(exp_dfs_split[[i]], sol_split[[i]], wth_split[[i]])
    }
    
    # Remove empty data frames
    exp_dfs_split[[i]] <- Filter(function(df) !is.null(df) || 
                                   !all(dim(df) == c(0, 0)) || 
                                   !all(is.na(df)), 
                                 exp_dfs_split[[i]])
    
    # Remove NA columns generated by splitting in measured data
    nms <- names(exp_dfs_split[[i]])
    exp_dfs_split[[i]] <- lapply(names(exp_dfs_split[[i]]), function(name) {
      
      ls <- exp_dfs_split[[i]]
      df <- ls[[name]]
      
      if (grepl("^SM_|TS_", name)) {
        df <- df[, colSums(!is.na(df)) > 0, drop = FALSE]  # Remove all-NA columns
      }
      
      return(df)
    })
    names(exp_dfs_split[[i]]) <- nms  # Restore names
  }
  
  return(exp_dfs_split)
}
