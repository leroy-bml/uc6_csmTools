#' Compile, parameterize, and write a DSSAT dataset
#'
#' Prepares a DSSAT dataset for a model run:
#' 1. Formats all raw data sections (MANAGEMENT, SOIL, WEATHER, etc.).
#' 2. Applies simulation control parameters.
#' 3. Optionally writes all files to their correct locations.
#'
#' @param dataset A named list of raw DSSAT data. Expected names correspond to DSSAT input files:
#'   "MANAGEMENT", "SOIL", "WEATHER", "SUMMARY", "TIME_SERIES".
#' @param write Logical. If \code{TRUE}, the formatted files are written to disk. Default is \code{FALSE}.
#' @param sol_append Logical. If \code{TRUE}, the formatted soil data is appended to the target
#'   `*.SOL` file. If \code{FALSE}, it overwrites the target `*.SOL` file. Default is \code{TRUE}.
#' @param write_in_dssat_dir Logical. If \code{TRUE}, each fileis  written to the adequate standard DSSAT
#'   subdirectories. If \code{FALSE}, all files are written to the \code{path} directory.
#'   Default is \code{TRUE}.
#' @param path Character. The target directory for writing files.
#'   **Only used if \code{write_in_dssat_dir = FALSE}**. Default is \code{getwd()}.
#' @param control_args A named list of simulation parameters to overwrite defaults in
#'   'SIMULATION_CONTROLS' and 'TREATMENTS' (e.g., \code{list(SDATE = "23001", R = 1)}).
#'
#' @details
#' This function is a high-level wrapper that calls several internal helpers in sequence:
#' \enumerate{
#'   \item \code{build_dssat_dataset} formats all raw data sections.
#'   \item \code{set_dssat_controls} modifies the \code{MANAGEMENT} section
#'     with parameters from \code{control_args}.
#'   \item If \code{write = TRUE}, \code{prepare_dssat_paths} resolves all
#'     output file paths, and \code{write_dssat_dataset} writes the files to disk.
#' }
#'
#' @return A named list containing a simulation-ready, formatted, and parameterized DSSAT dataset.
#'   The list names will be \code{"MANAGEMENT"}, \code{"SOIL"}, 
#'   \code{"WEATHER"}, \code{"SUMMARY"}, and \code{"TIME_SERIES"}.
#'
#' @examples
#' \dontrun{
#' # Assuming 'my_raw_dataset' is a list of raw data
#'
#' # Just format and parameterize (here, set start date and random seed), without writing
#' formatted_data <- compile_dssat_dataset(
#'   my_raw_dataset,
#'   control_args = list(RSEED = 1234, SDATE = "23001")
#' )
#'
#' # Format, parameterize, and write to a custom local directory
#' compile_dssat_dataset(
#'   my_raw_dataset,
#'   write = TRUE,
#'   write_in_dssat_dir = FALSE,
#'   path = "./model_run_1",
#'   sol_append = FALSE
#' )
#'
#' # Format, parameterize, and write to standard DSSAT directories
#' compile_dssat_dataset(
#'   my_raw_dataset,
#'   control_args = list(RSEED = 1234, SDATE = "23001"),
#'   write = TRUE,
#'   write_in_dssat_dir = TRUE
#' )
#' }
#'
#' @export
#'

compile_dssat_dataset <- function(
    dataset,
    write = FALSE, sol_append = TRUE, write_in_dssat_dir = TRUE, path = getwd(),
    control_args = list()
) {
  
  # Format all files to write-ready formats
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
    dataset_fmt <- prepare_dssat_paths(
      dataset = dataset_fmt,
      write_in_dssat_dir = write_in_dssat_dir,
      path = path
    )
    # Write the files
    write_dssat_dataset(dataset_fmt, sol_append = sol_append)
  }
  
  return(dataset_fmt)
}
