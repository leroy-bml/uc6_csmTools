#' Build a Composite Data Frame from a List
#'
#' @description
#' Combines a list of data frames and summarizes the data by specified groups, either by taking the first non-NA value (`coalesce`)
#'   or by averaging.
#'
#' @param df_list A list of data frames to combine.
#' @param groups A character vector of column names to group by for summarization.
#' @param action The summary method: `"coalesce"` (default) to take the first non-NA value per group, or `"average"` to compute
#'   the mean.
#'
#' @return A single, summarized data frame.
#'
#' @importFrom purrr reduce
#' @importFrom dplyr bind_rows group_by summarise across any_of
#' @importFrom tidyselect everything
#'
#' @examples
#' df1 <- data.frame(id = "A", val1 = 1, val2 = NA)
#' df2 <- data.frame(id = "A", val1 = NA, val2 = 10)
#' df3 <- data.frame(id = "B", val1 = 5, val2 = 20)
#' df_list <- list(df1, df2, df3)
#'
#' # Coalesce values by group (default action)
#' build_composite_data(df_list, groups = "id")
#'
#' # Average values by group
#' build_composite_data(df_list, groups = "id", action = "average")
#'
#' @export
#' 

build_composite_data <- function(df_list, groups, action = "coalesce") {
  
  combined_df <- df_list %>% reduce(bind_rows)
  composite_df <- switch(action,
                         "coalesce" = {
                           combined_df %>%
                             group_by(across(any_of(groups))) %>%
                             summarise(across(everything(), ~ first(na.omit(.x))), .groups = "drop")
                         },
                         "average" = {
                           combined_df %>%
                             group_by(across(any_of(groups))) %>%
                             summarise(across(everything(), ~ mean(na.omit(.x))), .groups = "drop")
                         }
  )
  return(composite_df)
}
