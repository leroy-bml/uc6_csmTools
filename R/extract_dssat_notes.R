#'
#'
#'

extract_dssat_notes <- function(data) {
  
  # Get names of first 2 columns for grouping
  group_cols <- names(data)[1:2]
  
  # --- ROBUSTNESS CHECK ---
  # Check if any notes columns actually exist in this dataframe
  notes_cols_exist <- any(grepl("_NOTES|_COMMENTS", names(data)))
  
  if (!notes_cols_exist) {
    # If no notes columns exist, return an empty tibble
    # with the *exact* final column structure.
    
    # Get an empty slice of the group_cols to preserve their types
    empty_df <- data[0, group_cols, drop = FALSE] 
    
    # Add the columns that pivot_longer would have created
    empty_df$Notes_Column  <- character(0)
    empty_df$Notes_Content <- character(0)
    
    return(empty_df)
  }

  # If we get here, notes columns DO exist. Proceed as normal.
  data %>%
    group_by(across(all_of(group_cols))) %>%
    summarise(
      across(
        matches("_NOTES|_COMMENTS"),
        ~ paste(unique(na.omit(.x[.x != ""])), collapse = "; "),
        .names = "{col}"
      ),
      .groups = "drop"
    ) %>%
    pivot_longer(
      cols = matches("_NOTES|_COMMENTS"), # This is now safe
      names_to = "Column",
      values_to = "Content"
    ) %>%
    # Split nested comments
    mutate(
      Content_List = if_else(
        # Regex to fetch vector structures
        str_detect(trimws(Content), "^c\\(.*\\)$"), 
        # Extract all quoted string
        str_extract_all(Content, "\"(.*?)\""),
        list(Content)
      )
    ) %>%
    unnest(Content_List) %>%
    # Overwrite original
    mutate(
      # Remove leading/trailing quotes
      Content = str_replace_all(Content_List, "^\"|\"$", ""),
      # Clean up any escaped quotes that are now literal
      Content = str_replace_all(Content, "\\\\\"", "\"")
    ) %>%
    select(-Content_List) %>%
    filter(!is.na(Content) & Content != "")
}
