#'
#'
#'
#'
#' @importFrom openxlsx2 wb_load wb_read
#' 

get_template_version <- function(path) {
  
  wb <- wb_load(path)
  # Read the value in cell A1 from the "Menu" sheet
  version <- wb_read(wb, sheet = "Menu", cols = 1, rows = 1, colNames = FALSE)[[1,1]]
  return(version)
}

get_template_version(template_pkg)

#'
#'
#'
#'

#TODO: fix version check and make doc
check_template_version <- function() {
  user_template <- file.path(Sys.getenv("HOME"), ".csmTools/template_icasa_vba.xlsm")
  pkg_template <- system.file("extdata", "template_icasa_vba.xlsm", package = "csmTools")
  if (!file.exists(user_template)) return("User template missing.")
  user_ver <- get_template_version(user_template)
  pkg_ver <- get_template_version(pkg_template)
  # if (pkg_ver > user_ver) {
  #   message("A new template version is available. Run update_user_template() to update. Your data will be backed up.")
  # }
  return(pkg_ver > user_ver)
}

#'
#'
#'
#'

fetch_template <- function(force = FALSE) {
  # User package directory
  user_dir <- file.path(Sys.getenv("HOME"), ".csmTools")
  if (!dir.exists(user_dir)) dir.create(user_dir, recursive = TRUE)
  
  user_template <- file.path(user_dir, "template_icasa_vba.xlsm")  # User file
  pkg_template <- system.file("extdata", "template_icasa_vba.xlsm", package = "csmTools")  # Default file
  
  # If user template doesn't exist, just copy
  if (!file.exists(user_template)) {
    file.copy(pkg_template, user_template, overwrite = TRUE)
    message("User template created at: ", user_template)
    return(list(path = user_template, created = TRUE, updated = FALSE))
  } else {
    outdated <- check_template_version()
    if (!outdated) {
      return(list(path = user_template, created = FALSE, updated = FALSE))
    } else if (force) {
      backup <- file.path(
        user_dir,
        paste0("template_icasa_vba_backup_", format(Sys.Date(), "%y%m%d"), ".xlsm")
      )
      file.copy(user_template, backup)
      file.copy(pkg_template, user_template, overwrite = TRUE)
      message("A new template version is available. User template was updated and old version backed up as: ", backup)
      return(list(path = user_template, created = FALSE, updated = TRUE))
    } else {
      message("A new template version is available. Run fetch_template(force = TRUE) to update. Your data will be backed up.")
      return(list(path = user_template, created = FALSE, updated = FALSE))
    }
  }
}

#'
#'
#'
#'
#'