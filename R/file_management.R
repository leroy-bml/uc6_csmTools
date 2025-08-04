#' Retrieve Template Version from Excel File
#'
#' Loads the Excel template workbook and extracts the version number.
#'
#' @param path Character string. The file path to the Excel workbook from which to retrieve the template version.
#'
#' @return A string version identifier.
#'
#' @details
#' This function uses `wb_load` to load the workbook and `wb_read` to read the value in a specific location in the workbook.
#' It assumes that the version information is always stored in this specific location.
#'
#' @examples
#' # Example usage:
#' # version <- get_template_version("path/to/template.xlsm")
#'
#' @importFrom openxlsx2 wb_load wb_read
#'
#' @export

get_template_version <- function(path) {
  
  wb <- wb_load(path)
  # Read the value in cell A1 from the "Menu" sheet
  version <- wb_read(wb, sheet = "Menu", cols = 1, rows = 1, colNames = FALSE)[[1,1]]
  return(version)
}


#' Compare User and Package Template Versions
#'
#' Checks and compares the version of the user's template file with the package's template file for ICASA VBA.
#'
#' @details
#' This function locates the user's template at `~/.csmTools/template_icasa_vba.xlsm` and the package template within the installed package's `extdata` directory.
#' It retrieves the version from each template using `get_template_version()`, splits the version strings into major, minor, and patch components, and prints a comparison table.
#' If the user template does not exist, a message is displayed and the function exits silently.
#'
#' @return
#' Invisibly returns a data frame with columns: \code{file}, \code{major}, \code{minor}, and \code{patch}, showing the version breakdown for both user and package templates.
#'
#' @examples
#' # Compare template versions (will print a table if both files exist)
#' check_template_version()
#'
#' @export

check_template_version <- function() {
  
  user_template <- file.path(Sys.getenv("HOME"), ".csmTools/template_icasa_vba.xlsm")
  pkg_template <- system.file("extdata", "template_icasa_vba.xlsm", package = "csmTools")
  
  # Check if user template exists
  if (!file.exists(user_template)) {
    message("User template not found.")
    return(invisible(NULL))
  }
  
  # Get version strings
  user_ver <- get_template_version(user_template)
  pkg_ver  <- get_template_version(pkg_template)
  
  # Breakdown version strings
  vers <- lapply(list(user_ver, pkg_ver), function(x) strsplit(x, "\\.")[[1]])
  vers <- as.data.frame(do.call(rbind, vers), stringsAsFactors = FALSE)
  colnames(vers) <- c("major", "minor", "patch")
  vers[] <- lapply(vers, as.numeric)
  
  # Add file labels
  vers$file <- c("user", "package")
  vers <- vers[, c("file", "major", "minor", "patch")]
  
  print(vers)
  invisible(vers)
}


#' Ensure User Has the Latest ICASA VBA Template
#'
#' Copies the latest version of the ICASA VBA template to the user's directory, updating and backing up as needed.
#'
#' @param force Logical. If \code{TRUE}, forces an update of the user template even if an older version exists, backing up the previous file. Default is \code{FALSE}.
#'
#' @details
#' This function checks for the presence of the user template at \code{~/.csmTools/template_icasa_vba.xlsm}. If it does not exist, it is copied from the package's \code{extdata} directory.
#' If the user template exists, its version is compared to the package template using \code{check_template_version()}. If the package template is newer, the user is prompted to update.
#' If \code{force = TRUE}, the user template is backed up and replaced with the latest version from the package.
#'
#' @return
#' Invisibly returns a list with elements:
#' \describe{
#'   \item{path}{The normalized path to the user template.}
#'   \item{created}{Logical, \code{TRUE} if the template was newly created.}
#'   \item{updated}{Logical, \code{TRUE} if the template was updated.}
#' }
#'
#' @examples
#' # Ensure the user has the latest template (will prompt if update is available)
#' fetch_template()
#'
#' # Force update and backup the old template
#' fetch_template(force = TRUE)
#'
#' @export

fetch_template <- function(force = FALSE) {
  
  # Set user and package template paths
  user_dir <- file.path(Sys.getenv("HOME"), ".csmTools")
  if (!dir.exists(user_dir)) dir.create(user_dir, recursive = TRUE)
  
  user_template <- file.path(user_dir, "template_icasa_vba.xlsm")  # User file
  pkg_template <- system.file("extdata", "template_icasa_vba.xlsm", package = "csmTools")  # Default file
  
  # Check if package template exists
  if (!file.exists(pkg_template)) {
    stop("Package template not found: ", pkg_template)
  }
  
  # If user template does not exist, copy from package
  if (!file.exists(user_template)) {
    file.copy(pkg_template, user_template, overwrite = TRUE)
    message("User template created at: ", normalizePath(user_template))
    return(invisible(list(path = normalizePath(user_template), created = TRUE, updated = FALSE)))
  }
  
  # Compare versions
  vers <- check_template_version()
  ver_value <- vers$major * 1e6 + vers$minor * 1e3 + vers$patch
  idx <- which.max(ver_value)
  most_recent <- vers[idx, "file"]
  
  outdated <- most_recent != "user"
  
  if (!outdated) {
    message("User template is the most recent version.")
    return(invisible(list(path = normalizePath(user_template), created = FALSE, updated = FALSE)))
  }
  
  message("A new template version is available.")
  
  if (force) {
    backup <- file.path(
      user_dir,
      paste0("template_icasa_vba_backup_", format(Sys.Date(), "%y%m%d"), ".xlsm")
    )
    file.copy(user_template, backup, overwrite = TRUE)
    file.copy(pkg_template, user_template, overwrite = TRUE)
    message("User template was updated. Old version backed up as: ", normalizePath(backup))
    return(invisible(list(path = normalizePath(user_template), created = FALSE, updated = TRUE)))
  } else {
    message("Run fetch_template(force = TRUE) to update. Your data will be backed up.")
    return(invisible(list(path = normalizePath(user_template), created = FALSE, updated = FALSE)))
  }
}


#'
#'
#'
#'

fetch_icasa <- function(url) {
  
  # List of CSV filenames (you can expand this list or scrape it dynamically)
  csv_files <- c("Metadata.csv", "Management_info.csv", "Soils_data.csv", "Weather_data.csv", "Measured_data.csv")
  
  # Read all CSVs into a named list
  data_list <- lapply(csv_files, function(file) {
    url <- paste0(url, file)
    read.csv(url)
  })
  
  out <- do.call(rbind, data_list)
  out <- cbind(out["var_uid"], out[ , setdiff(names(out), "var_uid")])
  row.names(out) <- NULL
  
  return(out)
}




