#' Helper to calculate TAV and AMP
#' @noRd

calculate_wth_stats <- function(wth_data) {
  
  if (nrow(wth_data) == 0 ||
      !all(c("DATE", "TMAX", "TMIN") %in% names(wth_data))) {
    out_null <- data.frame(
      YEAR = NA_character_,
      AMP = NA_real_,
      TAV = NA_real_
    )
    return(out_null)
  }
  
  wth_tstats <- wth_data |>
    dplyr::mutate(
      TAVG = (TMAX + TMIN) / 2,
      # Breakdown DSSAT dates to extract year and month
      yr_2digit = as.numeric(substr(DATE, 1, 2)),
      doy = as.numeric(substr(DATE, 3, 5)),
      FULL_YEAR = ifelse(yr_2digit > 50, 1900 + yr_2digit, 2000 + yr_2digit),
      MO = lubridate::month(as.Date(paste0(FULL_YEAR, "-01-01")) + lubridate::days(doy - 1))
    ) |>
    dplyr::group_by(across(any_of(c("EXP_ID", "INSI"))), FULL_YEAR, MO) |>
    dplyr::mutate(MO_TAV = mean(TAVG, na.rm = TRUE)) |>
    dplyr::ungroup() |>
    dplyr::group_by(across(any_of(c("EXP_ID", "INSI"))), YEAR) |>
    dplyr::summarise(
      AMP = (max(MO_TAV) - min(MO_TAV)) / 2,
      TAV = mean(TAVG, na.rm = TRUE), .groups = "drop"
    ) |>
    dplyr::distinct()
  
  return(wth_tstats)
}
