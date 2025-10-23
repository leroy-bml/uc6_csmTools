#' Apply simulation controls and parameters to a FileX
#'
#' This internal helper modifies the SIMULATION_CONTROLS and TREATMENTS sections of a formatted DSSAT FileX
#' list. It overwrites defaults with values from the `args` list and set a default SDATE if needed.
#'
#' @param filex A formatted 'MANAGEMENT' (FileX) list generate with a 'build_*' function.
#' @param args A named list of simulation control parameters.
#'
#' @return The modified `filex` list with custom controls.
#' 
#' @noRd
#' 

set_dssat_controls <- function(filex, args) {
  
  # --- SIMULATION_CONTROLS ---
  sim_controls <- filex$SIMULATION_CONTROLS
  # Find args that match names in SIMULATION_CONTROLS
  control_args <- args[names(args) %in% names(sim_controls)]
  # Overwrite defaults with provided args
  if (length(control_args) > 0) {
    sim_controls <- utils::modifyList(sim_controls, control_args)
  }
  filex$SIMULATION_CONTROLS <- sim_controls
  
  # --- TREATMENTS CONTROLS ---
  treatments <- filex$TREATMENTS
  # Set defaults
  treatments <- treatments |>
    dplyr::mutate(
      R = ifelse(is.na(R), 1, R),
      O = ifelse(is.na(O), 0, O),
      C = ifelse(is.na(C), 0, C)
    )
  # Find args that match TREATMENTS columns
  treat_args <- args[names(args) %in% c("R", "O", "C")]
  # Overwrite defaults with provided args
  if (length(treat_args) > 0) {
    for (name in names(treat_args)) {
      treatments[[name]] <- treat_args[[name]]
    }
  }
  filex$TREATMENTS <- treatments
  
  # --- SET DEFAULT STARTING DATE ---
  # Set default start date as first cultivation event
  if (is.na(filex$SIMULATION_CONTROLS$SDATE)) {
    filex$SIMULATION_CONTROLS$SDATE <- .find_min_management_date(filex)
  }
  
  return(filex)
}


#' Find the earliest date in a FileX list
#'
#' Scans all data frames in a FileX list for DATE columns (i.e., names containing "DAT"),
#' and returns the earliest date found (i.e., first cultivation event for the focal season).
#'
#' @param filex A formatted 'MANAGEMENT' (FileX) list generate with a 'build_*' function.
#'
#' @return A date string in DSSAT format ("%y%j").
#' 
#' @noRd
#' 

.find_min_management_date <- function(filex) {
  
  # Get all management subsection data.frames
  all_dfs <- filex[sapply(filex, is.data.frame)]
  
  # Find all DATE columns ("DAT" in colname)
  all_date_cols <- lapply(all_dfs, function(df) {
    df[grepl("DAT", colnames(df))]
  })
  
  # Make a clean date vector
  all_dates_chr <- unlist(all_date_cols, use.names = FALSE)
  all_dates <- suppressWarnings(as.Date(all_dates_chr, format = "%y%j"))
  all_dates <- all_dates[!is.na(all_dates)]
  
  # Return first cultivation event, i.e., earliest managementdate
  if (length(all_dates) == 0) {
    warning("No management dates found to set SDATE. SDATE remains NA.", call. = FALSE)
    return(NA_character_)
  }
  
  min_date <- min(all_dates)
  return(format(min_date, "%y%j"))
}