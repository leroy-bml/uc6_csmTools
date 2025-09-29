#' Check if a Vector is Year-Like
#'
#' Determines whether a vector is "year-like" based on several criteria:
#' - All values are 4-digit numbers
#' - All values are integers
#' - There is at least one repeated value
#' - All values fall within a plausible year range (default: 1800 to current year plus a future buffer)
#'
#' @param col A vector to be tested for year-likeness.
#' @param range A numeric vector of length 2 specifying the minimum and maximum plausible years (default: c(1800, current year)).
#' @param future_buffer An integer specifying how many years beyond the current year to allow as plausible (default: 5).
#'
#' @return Logical. \code{TRUE} if the vector meets all year-like criteria, \code{FALSE} otherwise.
#'
#' @examples
#' is_year_like(c(1999, 2000, 2001, 2001))
#' is_year_like(c("2010", "2011", "2012", "2012"))
#' is_year_like(c(1999, 2000, 2001, 2002)) # No repeats, returns FALSE
#' is_year_like(c(199, 2000, 2001, 2001))  # Not all 4 digits, returns FALSE
#'
#' 

is_year_like <- function(col, range = c(1800, as.numeric(format(Sys.Date(), "%Y"))), future_buffer = 5) {
  
  # Harmonize to character
  col_chr <- as.character(col)
  # Remove NAs
  non_na <- na.omit(col_chr)
  if (length(non_na) == 0) return(FALSE)
  
  # Set plausible range
  range_max <- range[2] + future_buffer
  range_min <- range[1]
  
  # Coerce to numeric safely
  col_num <- suppressWarnings(as.numeric(non_na))
  if (any(is.na(col_num))) return(FALSE)
  
  # Criteria
  crit_4digits <- all(nchar(non_na) == 4)  # has 4 digits
  crit_int  <- all(col_num == floor(col_num))  # is integer
  crit_repeat <- length(unique(col_num)) < length(col_num)  # repeats
  crit_range  <- all(col_num >= range_min & col_num <= range_max)  # is within range
  
  # Combine
  crit_4digits && crit_int && crit_repeat && crit_range
}


#' Find Year-Like Columns in a Data Frame
#'
#' Identifies columns in a data frame that are "year-like" according to the \code{\link{is_year_like}} function.
#' A year-like column is one where all values are 4-digit integers, within a plausible year range, and with at least one repeated value.
#'
#' @param df A data frame to search for year-like columns.
#' @param range A numeric vector of length 2 specifying the minimum and maximum plausible years (default: c(1800, current year)).
#' @param future_buffer An integer specifying how many years beyond the current year to allow as plausible (default: 5).
#'
#' @return A character vector of column names in \code{df} that are year-like.
#'
#' @seealso \code{\link{is_year_like}}
#'
#' @examples
#' df <- data.frame(
#'   year = c(2000, 2001, 2001, 2002),
#'   value = c(1, 2, 3, 4),
#'   code = c("A", "B", "C", "D")
#' )
#' find_year_col(df)
#'

find_year_col <- function(df, range = c(1800, as.numeric(format(Sys.Date(), "%Y"))), future_buffer = 5) {
  hits <- sapply(df, is_year_like, range = range, future_buffer = future_buffer)
  names(df[hits])
}


#' Check if a Column is Plot-Like
#'
#' Determines whether a column in a data frame is "plot-like" based on several criteria:
#' \enumerate{
#'   \item Plot IDs are uniquely represented within each year (no duplicates within a year).
#'   \item At least one plot ID is repeated across all years (i.e., there is at least one plot present in every year).
#'   \item The column has the highest number of distinct values (modalities) within each year, compared to other non-primary-key columns.
#' }
#' The function returns \code{TRUE} if the column satisfies either criterion 1 or 2, and also satisfies criterion 3.
#'
#' @param df A data frame containing the data to be checked.
#' @param col_name The name of the column to test for plot-likeness (as a string).
#' @param year_col_name The name of the year column (as a string).
#'
#' @return Logical. \code{TRUE} if the column is plot-like, \code{FALSE} otherwise.
#'
#' @details
#' The function uses \code{get_pkeys} to determine primary key columns, which are excluded from the modality check.
#'
#' @seealso \code{\link{get_pkeys}}
#'
#' @examples
#' # Example usage (assuming get_pkeys is defined):
#' # df <- data.frame(Plot = c(1,1,2,2), Year = c(2000,2001,2000,2001), Value = c(5,6,7,8))
#' # is_plot_like(df, "Plot", "Year")
#'

is_plot_like <- function(df, col_name, year_col_name) {
  
  by_year_col <- split(df[[col_name]], df[[year_col_name]])
  
  # Criterion #1: Plots are uniquely represented within year
  crit_nested <- all(sapply(by_year_col, function(x) length(unique(x)) == length(x)))
  # >> not consistent; plots split with multiple crops in some experiments
  
  # Criterion #2: Plot IDs are repeated (at least partially) across all years
  unique_sets <- lapply(by_year_col, unique)
  # overlaps <- outer(seq_along(unique_sets), seq_along(unique_sets), Vectorize(function(i, j) {
  #   length(intersect(unique_sets[[i]], unique_sets[[j]]))
  # }))
  # crit_repeated <- any(overlaps[upper.tri(overlaps)])
  # Find elements common to all sets
  common_elements <- Reduce(intersect, unique_sets)
  # Check if there is at least one element present in all sets
  crit_repeated <- length(common_elements) > 0
  
  isPlotLike <- (crit_nested || crit_repeated)
  
  # Criterion #3: Plot has the most distinct modalities within 
  pk <- get_pkeys(df)
  by_year <- split(df[, !(names(df) %in% pk)], df[[year_col_name]])  # drop primary key
  col_longest <- unique(
    sapply(by_year, function(df) {
      unique_counts <- sapply(df, function(x) length(unique(x)))
      names(unique_counts)[which.max(unique_counts)]
    })
  )
  
  # Is plot criterion = (C1 OR C2) & C3
  if (length(col_longest) == 1) {
    col_longest == col_name
  } else {
    FALSE
  }
}


#' Find Plot-Like Columns in a Data Frame
#'
#' Identifies columns in a data frame that are "plot-like" according to the \code{\link{is_plot_like}} function.
#' A plot-like column is one that uniquely identifies plots within years, is repeated across years, and has the highest number of distinct values per year (see \code{is_plot_like} for details).
#'
#' @param df A data frame to search for plot-like columns.
#' @param year_col_name The name of the year column (as a string).
#'
#' @return A character vector of column names in \code{df} that are plot-like.
#'
#' @seealso \code{\link{is_plot_like}}
#'
#' @examples
#' # Example usage (assuming is_plot_like and get_pkeys are defined):
#' # df <- data.frame(Plot = c(1,1,2,2), Year = c(2000,2001,2000,2001), Value = c(5,6,7,8))
#' # find_plot_col(df, "Year")
#'

find_plot_col <- function(df, year_col_name) {
  
  isPlot <- sapply(
    setdiff(names(df), year_col_name),
    function(col) is_plot_like(df, col, year_col_name)
  )
  plot_nm <- names(isPlot[isPlot])
  
  return(plot_nm)
}


#' Check Consistency of Plot-Treatment Associations in Consecutive Year Blocks
#'
#' Determines whether plot-treatment associations are conserved within blocks of consecutive years in a data frame.
#' A new block is started whenever there is a change in treatment allocation or a gap in the sequence of years.
#' The function returns \code{TRUE} only if all blocks have consistent plot-treatment associations and each block is at least \code{min_block_length} years long.
#'
#' @param df A data frame containing the data to be checked.
#' @param col_name The name of the treatment column (as a string).
#' @param plot_col The name of the plot column (as a string).
#' @param year_col The name of the year column (as a string).
#' @param min_block_length Integer. The minimum number of consecutive years required for a block to be considered valid (default: 2).
#'
#' @return Logical. \code{TRUE} if all blocks of consecutive years have consistent plot-treatment associations and meet the minimum block length, \code{FALSE} otherwise.
#'
#' @details
#' The function sorts the data by year, splits it into blocks of consecutive years with consistent plot-treatment associations, and checks both the consistency and the minimum length of each block.
#'
#' @examples
#' # Example usage:
#' # df <- data.frame(
#' #   Plot = c(1,1,2,2,1,1,2,2),
#' #   Year = c(2000,2001,2000,2001,2003,2004,2003,2004),
#' #   Treatment = c('A','A','B','B','C','C','D','D')
#' # )
#' # check_consecutive_treatment_blocks(df, col_name = "Treatment", plot_col = "Plot", year_col = "Year", min_block_length = 2)
#'
#' @export

check_consecutive_treatment_blocks <- function(df, col_name, plot_col, year_col, min_block_length = 2) {
  # Sort by year
  df <- df[order(df[[year_col]]), ]
  years <- unique(df[[year_col]])
  treatment_map <- split(df[, c(plot_col, col_name)], df[[year_col]])
  treatment_map <- lapply(treatment_map, unique)
  treatment_lookup <- lapply(treatment_map, function(x) {
    x <- na.omit(x)
    x[order(x[[plot_col]]), ]
  })
  
  # Get mapping for each year
  get_mapping <- function(x) {
    mapping <- x[, col_name]
    names(mapping) <- x[[plot_col]]
    mapping
  }
  mappings <- lapply(treatment_lookup, get_mapping)
  
  # Identify change points (treatment change or year gap)
  change_points <- logical(length(years))
  change_points[1] <- TRUE
  for (i in 2:length(years)) {
    treatment_changed <- !identical(mappings[[i]], mappings[[i-1]])
    year_gap <- (years[i] - years[i-1]) != 1
    change_points[i] <- treatment_changed || year_gap
  }
  
  # Assign block IDs
  block_id <- cumsum(change_points)
  
  # Check consistency and length within each block
  block_consistent <- tapply(seq_along(mappings), block_id, function(idx) {
    all(sapply(mappings[idx], function(m) identical(m, mappings[[idx[1]]])))
  })
  block_lengths <- as.numeric(table(block_id))
  
  # Return TRUE if all blocks are consistent and meet the minimum length
  all(block_consistent) && all(block_lengths >= min_block_length)
}


#' Check if a Column is Treatment-Like
#'
#' Determines whether a column in a data frame is "treatment-like" based on two criteria:
#' \enumerate{
#'   \item For each year, the column must have at least two unique non-NA values and at least one value must be reused across plots (i.e., not all values are unique within a year).
#'   \item plot-column associations must be consistent within blocks of consecutive years, as determined by \code{\link{check_consecutive_treatment_blocks}}.
#' }
#'
#' @param df A data frame containing the data to be checked.
#' @param col_name The name of the focal column (as a string).
#' @param plot_col The name of the plot column (as a string).
#' @param year_col The name of the year column (as a string).
#'
#' @return Logical. \code{TRUE} if the column is treatment-like, \code{FALSE} otherwise.
#'
#' @details
#' The function first checks that, for each year, the focal column has at least two unique non-NA values and that at least one value is reused across plots.
#' It then checks that plot-column associations are consistent within blocks of consecutive years using \code{\link{check_consecutive_treatment_blocks}}.
#'
#' @seealso \code{\link{check_consecutive_treatment_blocks}}
#'
#' @examples
#' # Example usage:
#' # df <- data.frame(
#' #   Plot = c(1,1,2,2,1,1,2,2),
#' #   Year = c(2000,2001,2000,2001,2003,2004,2003,2004),
#' #   Treatment = c('A','A','B','B','C','C','D','D')
#' # )
#' # is_treatment_like(df, col_name = "Treatment", plot_col = "Plot", year_col = "Year")
#'
#' @export

is_treatment_like <- function(df, col_name, plot_col, year_col) {

  years <- unique(df[[year_col]])
  
  # Criterion #1: Treatment must have ≥2 unique non-NA values per year and be reused across plots
  per_year_check <- all(sapply(years, function(yr) {
    subset <- df[df[[year_col]] == yr, ]
    vals <- unique(na.omit(subset[[col_name]]))
    length(vals) >= 2 && any(duplicated(subset[[col_name]]))
  }))
  if (!per_year_check) return(FALSE)
  
  # Criteria #2: Plot → treatment must be consistent across years
  # (allowing distinct blocks of consecutive years)
  is_conserved <- check_consecutive_treatment_blocks(df, col_name, plot_col, year_col)
  
  return(is_conserved)
}


#' Check if a Column is Crop-Like
#'
#' Determines whether a column in a data frame is "crop-like" based on two criteria:
#' \enumerate{
#'   \item For each year, the column must have at least two unique non-NA values and at least one value must be reused across plots (i.e., not all values are unique within a year).
#'   \item plot-column associations must change within blocks of consecutive years (crop rotation), as determined by \code{\link{check_consecutive_treatment_blocks}}.
#' }
#'
#' @param df A data frame containing the data to be checked.
#' @param col_name The name of the focal column (as a string).
#' @param plot_col The name of the plot column (as a string).
#' @param year_col The name of the year column (as a string).
#'
#' @return Logical. \code{TRUE} if the column is crop-like, \code{FALSE} otherwise.
#'
#' @details
#' The function first checks that, for each year, the focal column has at least two unique non-NA values and that at least one value is reused across plots.
#' It then checks that plot-column associations change between consecutive years using \code{\link{check_consecutive_treatment_blocks}}.
#'
#' @seealso \code{\link{check_consecutive_treatment_blocks}}
#'
#' @examples
#' # Example usage:
#' # df <- data.frame(
#' #   Plot = c(1,1,2,2,1,1,2,2),
#' #   Year = c(2000,2001,2000,2001,2003,2004,2003,2004),
#' #   Treatment = c('A','B','B','C','C','D','D','A')
#' # )
#' # is_crop_like(df, col_name = "Treatment", plot_col = "Plot", year_col = "Year")
#'
#' @export

is_crop_like <- function(df, col_name, plot_col, year_col) {

  plots <- unique(df[[plot_col]][!is.na(df[[col_name]])])
  years <- unique(df[[year_col]][!is.na(df[[col_name]])])  # drop NAs to account for potential "pause" years
  
  # Criterion #1: Crops must have ≥2 unique non-NA values per plot across years
  per_plot_check <- all(sapply(plots, function(plt) { ##yr plt
    subset <- df[df[[plot_col]] == plt, ]
    vals <- unique(na.omit(subset[[col_name]]))
    length(vals) >= 2
  }))
  if (!per_plot_check) return(FALSE)
  
  # Criterion #2: Crops must be used in multiple plots within years
  per_year_check <- all(sapply(years, function(yr) {
    subset <- df[df[[year_col]] == yr, ]
    # For each crop in this year, how many plots is it in?
    crops <- unique(na.omit(subset[[col_name]]))
    any(sapply(crops, function(crp) {
      sum(subset[[col_name]] == crp, na.rm = TRUE) >= 2
    }))
  }))
  if (!per_year_check) return(FALSE)
  
  # Criterion #2: Plot-value association must differ between consecutive years
  # (rotation: no crop used in consecutive year)
  is_conserved <- check_consecutive_treatment_blocks(df, col_name, plot_col, year_col)
  
  return(!is_conserved)
}


#' Find Treatment-Like Columns in a Data Frame
#'
#' Identifies columns in a data frame that are "treatment-like" according to the \code{\link{is_treatment_like}} function.
#' A treatment-like column is one that has at least two unique non-NA values per year, at least one value reused across plots within a year, and consistent plot-to-treatment associations within blocks of consecutive years.
#'
#' @param df A data frame to search for treatment-like columns.
#' @param plot_col The name of the plot column (as a string).
#' @param year_col The name of the year column (as a string).
#'
#' @return A character vector of column names in \code{df} that are treatment-like.
#'
#' @seealso \code{\link{is_treatment_like}}
#'
#' @examples
#' # Example usage (assuming is_treatment_like and check_consecutive_treatment_blocks are defined):
#' # df <- data.frame(
#' #   Plot = c(1,1,2,2,1,1,2,2),
#' #   Year = c(2000,2001,2000,2001,2003,2004,2003,2004),
#' #   Treatment = c('A','A','B','B','C','C','D','D')
#' # )
#' # find_treatment_col(df, plot_col = "Plot", year_col = "Year")
#'

find_treatment_col <- function(df, plot_col, year_col) {
  candidate_cols <- setdiff(names(df), c(plot_col, year_col))
  Filter(function(col) is_treatment_like(df, col, plot_col, year_col), candidate_cols)
}


#' Find crop-Like Columns in a Data Frame
#'
#' Identifies columns in a data frame that are "crop-like" according to the \code{\link{is_crop_like}} function.
#' A crop-like column is one that has at least two unique non-NA values per year, at least one value reused across plots within a year, and changing plot-column associations in consecutive years.
#'
#' @param df A data frame to search for crop-like columns.
#' @param plot_col The name of the plot column (as a string).
#' @param year_col The name of the year column (as a string).
#'
#' @return A character vector of column names in \code{df} that are crop-like.
#'
#' @seealso \code{\link{is_crop_like}}
#'
#' @examples
#' # Example usage (assuming is_crop_like and check_consecutive_crop_blocks are defined):
#' # df <- data.frame(
#' #   Plot = c(1,1,2,2,1,1,2,2),
#' #   Year = c(2000,2001,2000,2001,2003,2004,2003,2004),
#' #   crop = c('A','B','B','C','C','D','C','D')
#' # )
#' # find_crop_col(df, plot_col = "Plot", year_col = "Year")
#'

find_crop_col <- function(df, plot_col, year_col) {

  candidate_cols <- setdiff(names(df), c(plot_col, year_col))
  Filter(function(col) is_crop_like(df, col, plot_col, year_col), candidate_cols)
}


#' Retrieve a Table from a List by Primary Key
#'
#' Retrieves a data frame (table) from a list of data frames by matching its primary key.
#'
#' @param db A list of data frames.
#' @param pkey The name of the primary key to match (as a string).
#'
#' @return The data frame from \code{db} whose primary key matches \code{pkey}.
#'
#' @details
#' The function uses \code{\link{get_pkeys}} (with \code{alternates = FALSE}) to determine the primary key of each data frame in the list, and returns the first data frame whose primary key matches \code{pkey}.
#'
#' @seealso \code{\link{get_pkeys}}
#'
#' @examples
#' # Example usage (assuming get_pkeys is defined):
#' # db <- list(
#' #   df1 = data.frame(ID = 1:3, Value = c('A', 'B', 'C')),
#' #   df2 = data.frame(Key = 1:3, Data = c('X', 'Y', 'Z'))
#' # )
#' # get_tbl(db, pkey = "ID")
#'
#' @export
#' 

get_tbl <- function(db, pkey){
  
  pkeys <- lapply(db, function(df) get_pkeys(df, alternates = FALSE))
  tbl <- db[[which(pkeys == pkey)]]
  
  return(tbl)
} 


#' Count Unique Values per Year
#'
#' Counts the number of unique values in a specified column for each year in a data frame.
#'
#' @param col The name of the column to count unique values in (as a string).
#' @param df A data frame containing the data.
#' @param yr The name of the year column (as a string). Default is \code{"Year"}.
#'
#' @return A named integer vector giving the number of unique values in \code{col} for each year.
#'
#' @examples
#' df <- data.frame(
#'   Year = c(2000, 2000, 2001, 2001, 2001),
#'   Plot = c(1, 2, 1, 2, 3)
#' )
#' count_per_year("Plot", df)
#'
#' @export

count_per_year <- function(col, df, yr = "Year") {
  tapply(df[[col]], df[[yr]], function(x) length(unique(x)))
}


#' Identify Experimental Design Structure in a Database
#'
#' Identifies and summarizes the year, plot, and treatment structure of an experimental design from a database and a design table.
#' The function prints summaries of year, plot, and treatment attributes, including their ranges and associated tables, and returns a summary data frame (invisibly).
#'
#' @param db A list of data frames representing the database.
#' @param design_tbl A data frame containing the experimental design information.
#'
#' @return Invisibly returns a data frame summarizing the year, plot, and treatment components, their associated table names, ID column names, and counts/ranges.
#'
#' @details
#' The function:
#' \itemize{
#'   \item Identifies the year column and summarizes the range of years.
#'   \item Identifies the plot column, summarizes the number and range of plots per year, and prints the associated table.
#'   \item Identifies the treatment column, summarizes the number and range of treatments per year, and prints the associated table.
#' }
#' Helper functions such as \code{find_year_col}, \code{find_plot_col}, \code{find_treatment_col}, \code{count_per_year}, \code{get_tbl}, and \code{get_df_name} are used internally.
#'
#' @examples
#' # Example usage (assuming all helper functions are defined):
#' # db <- list(
#' #   plots = data.frame(Plot = 1:4, ...),
#' #   treatments = data.frame(Treatment = c("A", "B"), ...)
#' # )
#' # design_tbl <- data.frame(Year = rep(2000:2002, each = 4), Plot = rep(1:4, 3), Treatment = sample(c("A", "B"), 12, TRUE))
#' # identify_exp_design(db, design_tbl)
#'
#' @export
#'  

identify_exp_design <- function(db, design_tbl) {

  # Helper: summarize value ranges for a named vector
  get_value_ranges <- function(x) {

    years <- as.integer(names(x))
    vals <- as.vector(x)
    # Order by year
    ord <- order(years)
    years <- years[ord]
    vals <- vals[ord]

    # Find runs of consecutive years with the same value
    runs <- split(seq_along(vals), cumsum(c(TRUE, vals[-1] != vals[-length(vals)] | years[-1] != years[-length(years)] + 1)))
    result <- sapply(runs, function(idx) {
      val <- vals[idx[1]]
      yrs <- years[idx]
      if (length(yrs) == 1) {
        paste0(val, " [", yrs, "]")
      } else {
        paste0(val, " [", yrs[1], "-", yrs[length(yrs)], "]")
      }
    })
    result
  }
  
  #--- Year attribute
  
  # Find year column across the data
  # multiyear <- if(metadata$start_date != metadata$status) {
  #   TRUE
  # } else {
  #   FALSE
  # }
  
  year_nm <- find_year_col(design_tbl,
                           range = c(1900, as.numeric(format(Sys.Date(), "%Y"))),
                           future_buffer = 0)
  year_n <- length(unique(design_tbl[[year_nm]]))
  year_min <- min(design_tbl[[year_nm]], na.rm = TRUE)
  year_max <- max(design_tbl[[year_nm]], na.rm = TRUE)
  cat("\033[31m>> YEARS - ID: \"", year_nm, "\"; n = ", year_n, " [", year_min, "-", year_max, "]\033[0m\n", sep = "")
  
  #--- Plot attribute
  plot_nm <- find_plot_col(design_tbl, year_col_name = year_nm)
  plot_n <- count_per_year(plot_nm, design_tbl, year_nm)
  plot_ranges <- get_value_ranges(plot_n)
  plot_ranges <- paste(plot_ranges, collapse = ", ")
  plot_tbl <- get_tbl(db, plot_nm)
  plot_tbl_nm <- get_df_name(db, plot_tbl)
  cat("\033[31m>> PLOTS - Table \"", plot_tbl_nm, "\"; ID: \"", plot_nm, "\"; n = ", plot_ranges, "\033[0m\n", sep = "")

  #--- Treatment attribute
  treat_nm <- find_treatment_col(design_tbl, plot_col = plot_nm, year_col = year_nm)
  treat_n <- count_per_year(treat_nm, design_tbl, year_nm)
  treat_ranges <- get_value_ranges(treat_n)
  treat_ranges <- paste(treat_ranges, collapse = ", ")
  treat_tbl <- get_tbl(db, treat_nm)
  treat_tbl_nm <- get_df_name(db, treat_tbl)
  cat("\033[31m>> TREATMENTS - Table \"", treat_tbl_nm, "\"; ID: \"", treat_nm, "\"; n = ", treat_ranges, "\033[0m\n", sep = "")
  
  #--- Crop attribute
  crop_nm <- find_crop_col(design_tbl, plot_col = plot_nm, year_col = year_nm)
  # HACK: deal with instance where crop and cultivar are both in design table 
  # crop_nm returns > 1 chr
  if (length(crop_nm) > 1) {
    crop_nm <- crop_nm[1]
  }
  crop_n <- count_per_year(crop_nm, design_tbl, year_nm)
  crop_ranges <- get_value_ranges(crop_n)
  crop_ranges <- paste(crop_ranges, collapse = ", ")
  crop_tbl <- get_tbl(db, crop_nm)
  crop_tbl_nm <- get_df_name(db, crop_tbl)
  cat("\033[31m>> CROPS - Table \"", crop_tbl_nm, "\"; ID: \"", crop_nm, "\"; n = ", crop_ranges, "\033[0m\n", sep = "")
  
  #--- Output summary
  out <- data.frame(
    comp = c("year", "plot", "treatment", "crop"),
    tbl_name = c(NA_character_, plot_tbl_nm, treat_tbl_nm, crop_tbl_nm),
    id_name = c(year_nm, plot_nm, treat_nm, crop_nm),
    n = c(year_n, plot_ranges, treat_ranges, crop_ranges),
    stringsAsFactors = FALSE
  )
  invisible(out)
}


#' Identify Experimental Attribute Columns and Join Keys in a Database
#'
#' Identifies and annotates attribute columns and join keys across all tables in a database.
#' The function renames non-join attribute columns to include their table name as a prefix, and attaches lists of primary keys, join keys, and attribute columns as attributes to the output.
#'
#' @param db A list of data frames representing the database.
#' @param str_cols Optional character vector of additional columns to consider as structural/join keys.
#'
#' @return A list of data frames (the database) with non-join attribute columns renamed to include their table name as a prefix. The list has the following attributes:
#'   \item{primary_keys}{A list of primary key columns for each table.}
#'   \item{join_keys}{A list of join key columns for each table.}
#'   \item{attributes}{A list of non-join attribute columns for each table.}
#'
#' @details
#' The function:
#' \itemize{
#'   \item Gathers all column names across tables.
#'   \item Identifies date columns using \code{is_date}.
#'   \item Identifies primary keys using \code{get_pkeys}.
#'   \item Determines join keys as columns that are primary keys or in \code{str_cols}, but not date columns, and that appear in more than one table.
#'   \item Renames non-join attribute columns to include their table name as a prefix.
#'   \item Attaches lists of primary keys, join keys, and attribute columns as attributes to the output.
#' }
#'
#' @examples
#' # Example usage (assuming is_date and get_pkeys are defined):
#' # db <- list(
#' #   plots = data.frame(PlotID = 1:4, Area = c(10, 12, 11, 13)),
#' #   treatments = data.frame(TreatmentID = 1:2, Name = c("A", "B"))
#' # )
#' # identify_exp_attributes(db)
#'
#' @importFrom lubridate is.Date
#'
#' @export
#' 

identify_exp_attributes <- function(db, str_cols = NULL) {
  
  # Gather all column names across tables
  all_cols <- unlist(lapply(db, colnames), use.names = FALSE)
  # Identify columns containing dates
  date_cols <- unname(unlist(lapply(db, function(df) names(df)[sapply(df, is_date)])))
  # Identify all join keys (excluding date columns)
  pkeys <- unlist(lapply(db, get_pkeys, alternates = FALSE), use.names = FALSE)
  keys <- all_cols[all_cols %in% c(pkeys, str_cols) & !all_cols %in% date_cols]
  jkeys <- c(unique(keys[duplicated(keys)])) 
  # Identify non-join attribute columns
  attr_cols <- setdiff(all_cols, keys)
  
  # Annotate non-join attribute columns with table name
  db_out <- lapply(names(db), function(df_name) {
    df <- db[[df_name]]
    colnames(df) <- ifelse(colnames(df) %in% attr_cols,
                           paste(df_name, colnames(df), sep = "."),
                           colnames(df))
    df
  })
  names(db_out) <- names(db)
  
  # Attach join keys and attribute columns as attributes
  attr(db_out, "primary_keys") <- lapply(db_out, function(df) intersect(colnames(df), pkeys))
  attr(db_out, "join_keys") <- lapply(db_out, function(df) intersect(colnames(df), jkeys))
  attr_cols_out <- setdiff(unlist(lapply(db_out, colnames), use.names = FALSE), keys)
  attr(db_out, "attributes") <- lapply(db_out, function(df) intersect(colnames(df), attr_cols_out))
  
  return(db_out)
}


#' Identify Production Season from Management Dates
#'
#' @description
#' Finds the earliest and latest dates across a list of crop management data frames to define the production season.
#'
#' @param mngt_list A list of data frames containing date columns.
#' @param period Character string. Reserved for future use.
#' @param output Character string specifying return format: `"bounds"` (default) for start/end dates
#'   or `"date_sequence"` for the full daily sequence.
#'
#' @return A vector of `Date` objects, either the two boundary dates or the full sequence of dates between them.
#'
#' @importFrom purrr reduce
#' @importFrom dplyr bind_rows mutate coalesce select filter distinct pull
#'
#' @examples
#' # Helper function for the example to work
#' is_date <- function(x) inherits(x, 'Date')
#'
#' # Sample list of management data frames
#' mngt <- list(
#'   data.frame(planting_date = as.Date("2023-05-10")),
#'   data.frame(harvest_date = as.Date("2023-09-25"))
#' )
#'
#' # Get the start and end dates
#' identify_production_season(mngt)
#' 
#' @export
#'

identify_production_season <- function(mngt_list, period = "growing_season", output = "bounds") {
  
  output_handlers <- c("bounds", "date_sequence")
  output <- match.arg(output, output_handlers)
  period_handlers <- c("growing_season", "cultivation_season")
  period <- match.arg(period, period_handlers)
  
  date_list <- lapply(mngt_list, function(df) df[sapply(df, is_date)])
  
  # Merge all unique management dates in a single vector
  date_list <- date_list[sapply(date_list, function(df) ncol(df) != 0)]
  mngt_dates <- reduce(date_list, bind_rows) %>%
    mutate(mngt_dates = as.Date(coalesce(!!!.))) %>%
    select(mngt_dates) %>%
    filter(!is.na(mngt_dates)) %>%
    distinct() %>%
    pull(mngt_dates)
  gs_bounds <- c(min(mngt_dates), max(mngt_dates))
  
  if (output == "bounds") {
    return(gs_bounds)
  } else if (output == "date_sequence") {
    gs_dates <- seq(from = as.Date(gs_bounds[1]),
                    to = as.Date(gs_bounds[2]),
                    by = "day")
    return(gs_dates)
  }
}
