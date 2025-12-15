#' Build, parameterize, and write a DSSAT dataset
#'
#' Prepares a DSSAT dataset for a model run:
#' 1. Formats all raw data sections as proper inputs for DSSAT parser.
#' 2. Writes provided simulation control parameters to MANAGEMENT file.
#' 3. Optionally writes all files to their correct locations in DSSAT subdirectories or a user-specified location.
#'
#' @param dataset (list) A named list of raw DSSAT data containing formatted DSSAT data such as:
#'   `"MANAGEMENT"`, `"SOIL"`, `"WEATHER"`, `"SUMMARY"`, and `"TIME_SERIES"` elements.
#' @param write (logical) If `TRUE`, formatted files are written to disk. Default: `FALSE`.
#' @param sol_append (logical) If `TRUE`, soil profile data is appended as a new record at writing, provided a DSSAT soil file
#'   with the same indentifier (institution) already exists at the write location
#' @param write_in_dssat_dir (logical) If `TRUE`, files are written to standard DSSAT subdirectories.
#'   If `FALSE`, all files are written to the `path` directory. Default: `TRUE`.
#' @param path (character) Target directory for writing files. **Only used if `write_in_dssat_dir = FALSE`**. Default: `getwd()`.
#' @param control_config (character) Path to a JSON/YAML configuration file specifying DSSAT simulation control parameters.
#'   The file should contain key-value pairs where keys are parameter names (e.g., `"NITROGEN"`, `"WATER"`, `"RSEED"`, `"SDATE"`)
#'   and values are the desired settings. These values will override DSSAT's default simulation controls.
#'   To view default values, run `data(dssat_default_simcontrols)`.
#'
#' @details
#' This function is a high-level wrapper that calls several internal helpers in sequence:
#' \enumerate{
#'   \item \code{build_dssat_dataset} formats all raw data sections.
#'   \item \code{set_dssat_controls} modifies the \code{MANAGEMENT} section with parameters from \code{control_args} config file.
#'   \item If \code{write = TRUE}, \code{prepare_dssat_paths} resolves all output file paths
#'  \item if \code{write_dssat_dataset = TRUE} DSSAT input files are written to the adequate location in the DSSAT directory;
#'     if \code{write_dssat_dataset = FALSE}, files are written to the user-specified \code{path}.
#' }
#'
#' @return A named list containing a simulation-ready, formatted, and parameterized DSSAT input data.
#'   The list names will be \code{"MANAGEMENT"}, \code{"SOIL"}, \code{"WEATHER"}, \code{"SUMMARY"}, and \code{"TIME_SERIES"}.
#'
#' @examples
#' \dontrun{
#' # Assuming 'my_raw_dataset' is a list of raw data
#'
#' # Just format and parameterize (here, using config file), without writing
#' formatted_data <- build_simulation_files(
#'   my_raw_dataset,
#'   control_config = "path/to/sim_controls.yaml"
#' )
#'
#' # Format, parameterize, and write to a custom local directory
#' build_simulation_files(
#'   my_raw_dataset,
#'   write = TRUE,
#'   write_in_dssat_dir = FALSE,
#'   path = "./model_run_1",
#'   sol_append = FALSE,
#'   control_config = "path/to/sim_controls.json"
#' )
#'
#' # Format, parameterize, and write to standard DSSAT directories
#' build_simulation_files(
#'   my_raw_dataset,
#'   control_config = "path/to/sim_controls.yaml",
#'   write = TRUE,
#'   write_in_dssat_dir = TRUE
#' )
#' }
#' @importFrom tools file_ext
#' @importFrom yaml read_yaml
#' @importFrom jsonlite fromJSON
#' 
#' @export
#'

build_simulation_files <- function(
    dataset,
    write = FALSE, sol_append = FALSE, write_in_dssat_dir = TRUE, path = getwd(),
    control_config = NULL
) {
  
  # Resolve dataset
  dataset <- resolve_input(dataset)
  
  # Load control parameters if config file is provided
  control_args <- list()
  if (!is.null(control_config)) {
    if (!file.exists(control_config)) {
      stop("Control configuration file not found at: ", control_config)
    }
    
    config_ext <- tolower(file_ext(control_config))
    if (config_ext == "yaml" || config_ext == "yml") {
      control_args <- read_yaml(control_config)
    } else if (config_ext == "json") {
      control_args <- fromJSON(txt = readLines(control_config))
    } else {
      stop("Unsupported configuration file format. Use YAML (.yaml/.yml) or JSON (.json)")
    }
  }
  
  # Build parser-ready format (DSSAT_tbl class)
  dataset_fmt <- build_dssat_dataset(dataset)
  # TODO: handle cultivar file
  
  # Parameterize the simulation (overwrite default with provided values)
  dataset_fmt$MANAGEMENT <- set_dssat_controls(
    filex = dataset_fmt$MANAGEMENT, 
    args = control_args
  )
  
  # Handle file writing
  if (write) {
    # Set destination path for each file
    dataset_fmt_nms <- prepare_dssat_paths(
      dataset = dataset_fmt,
      write_in_dssat_dir = write_in_dssat_dir,
      path = path
    )
    # Write the files
    write_dssat_dataset(dataset_fmt_nms, sol_append = sol_append)
  }
  
  return(dataset_fmt)
}
