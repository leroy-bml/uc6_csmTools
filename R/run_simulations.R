#' Run DSSAT Simulations from a Dataset and Return Output
#'
#' Prepares input files, writes batch files, runs DSSAT simulations, and reads output for a given dataset.
#'
#' @param dataset A list containing DSSAT input tables, including at least \code{X} (management), \code{A} (observed summary), \code{T} (observed time series), \code{WTH} (weather), \code{SOL} (soil), and \code{CUL} (cultivar).
#' @param framework Character. Simulation framework to use (default: \code{"dssat"}).
#' @param dssat_dir Character or NULL. Path to the DSSAT installation directory. If NULL, uses environment variable or default.
#' @param sim_dir Character or NULL. Directory for simulation input/output files. If NULL, uses DSSAT directory or default.
#' @param args List. Additional simulation parameters (e.g., \code{TRTNO}, \code{RP}, \code{SQ}, \code{OP}, \code{CO}).
#'
#' @details
#' The function sets up the DSSAT environment, writes all required input files to the appropriate directories, prepares a batch file, and runs DSSAT in batch mode. It then reads the main output file (\code{PlantGro.OUT}) and returns it along with observed data. The function handles both Windows and Unix systems and uses several helper functions for file I/O and simulation control.
#'
#' @return A list containing:
#' \describe{
#'   \item{\code{plant_growth}}{A data frame of simulated plant growth output.}
#'   \item{\code{OBSERVED_Summary}}{Observed summary data (if provided).}
#'   \item{\code{OBSERVED_TimeSeries}}{Observed time series data (if provided).}
#' }
#'
#' @examples
#' \dontrun{
#' result <- run_simulations(dataset)
#' head(result$plant_growth)
#' }
#'
#' @importFrom utils modifyList
#' @importFrom DSSAT read_filex write_dssbatch run_dssat read_output
#' 
#' @export
#' 

run_simulations <- function(filex_path, treatments,
                            framework = "dssat", dssat_dir = NULL, sim_dir = NULL, args = list()) {
  
  # Read file X
  filex <- DSSAT::read_filex(filex_path)
  
  # Specify the location of the DSSAT CSM executable (can be passed as environment variable) and the location of input/output files
  if (is.null(dssat_dir)) {
    if (Sys.getenv("DSSAT_CSM") != "") {
      dssat_exe <- Sys.getenv("DSSAT_CSM")
      dssat_dir <- dirname(dssat_exe)
      sim_dir <- ifelse(is.null(sim_dir), dssat_dir, sim_dir)
      options(DSSAT.CSM = dssat_exe)
    } else {
      # use default windows dir
      options(DSSAT.CSM = "C:\\DSSAT48\\DSCSM048.EXE")
      sim_dir <- ifelse(is.null(sim_dir), "C:/DSSAT48", sim_dir)  # specify dir for simulations: input files, batch files and simulations all stored there
      dssat_dir <- "C:/DSSAT48"
    }
  } else {
    options(DSSAT.CSM = paste0(dssat_dir, "/DSCSM048.EXE"))
    sim_dir <- ifelse(is.null(sim_dir), "C:/DSSAT48", sim_dir)
  }
  
  # For Unix systems use home dir as default
  if(.Platform$OS.type == "unix"){
    options(DSSAT.CSM = paste0(Sys.getenv("HOME"), "/dssat/dscsm048")) # still hardcoded :(
    sim_dir <- paste0(Sys.getenv("HOME"), "/dssat/")
  }

  old_wd <- paste0(getwd(), "/") # trailing slash to avoid path issues
  setwd(sim_dir)

  # Default parameters
  default <- list(
    TRTNO = treatments,
    RP = rep(1, length(treatments)),
    SQ = rep(0, length(treatments)),
    OP = rep(0, length(treatments)),
    CO = rep(0, length(treatments))
  )
  params <- modifyList(default, args)  # set simulation parameters
  
  # Simulations
  batch_tbl <- data.frame(
    FILEX = rep(filex_path, length(params$TRTNO)),  # TODO: retrieve filename is not associated as attr (read through regular filex call)
    TRTNO = params$TRTNO,
    RP = params$RP,
    SQ = params$SQ,
    OP = params$OP,
    CO = params$CO
  )
  
  write_dssbatch(batch_tbl)  # write batch file
  run_dssat(run_mode = "B")  # run simulations
  
  # Read output
  setwd(old_wd)  # reset wd
  #out_files <- list.files(path = sim_dir, pattern = "\\.OUT$", full.names = TRUE)
  
  #output <- list()
  #for (nm in out_files){
  #  print(nm)
  #  output[[nm]] <- read_output(file_name = nm)
  #}
  #names(output) <- out_files
  output <- read_output(file_name = file.path(sim_dir, "PlantGro.OUT"))
  
  out <- list(plant_growth = as.data.frame(output),
              SUMMARY = filea,
              TIME_SERIES = filet)  # Append observed data

  return(out)
}


#' Plot Simulated and Observed Plant Growth Output
#'
#' Creates a plot comparing simulated and observed plant growth (e.g., yield) for different treatments.
#'
#' @param sim_output A list containing simulation output and observed data, as returned by \code{run_simulations}.
#'
#' @details
#' The function extracts observed summary data and simulated plant growth data from \code{sim_output}, formats dates, and creates a ggplot showing simulated growth as lines and observed data as points. The plot distinguishes treatments by color and includes custom legends for simulated and observed data.
#'
#' The function uses the \strong{dplyr} and \strong{ggplot2} packages for data manipulation and plotting.
#'
#' @return A ggplot object showing simulated and observed plant growth for each treatment.
#'
#' @examples
#' \dontrun{
#' plot_growth <- plot_output(sim_output)
#' print(plot_growth)
#' }
#'
#' @importFrom magrittr %>%
#' @importFrom dplyr filter mutate
#' @importFrom ggplot2 ggplot aes geom_line geom_point scale_colour_manual scale_size_manual scale_linewidth_manual labs guides guide_legend theme_bw theme element_text
#' 
#' @export
#' 

plot_output <- function(sim_output){
  
  # Observed data
  obs_summary_growth <- sim_output$SUMMARY %>%
    filter(TRNO %in% 1:3)
    # mutate(MDAT = as.POSIXct(as.Date(MDAT, format = "%y%j")),
    #        ADAT = as.POSIXct(as.Date(ADAT, format = "%y%j")))
  
  # Simulated data
  sim_growth <- sim_output$plant_growth
  
  # Plot
  plot_growth <- sim_growth %>%
    mutate(TRNO = as.factor(TRNO)) %>%
    ggplot(aes(x = DATE, y = GWAD)) +
    # Line plot for simulated data
    geom_line(aes(group = TRNO, colour = TRNO, linewidth = "Simulated")) +
    # Points for observed data
    #geom_point(data = obs_summary_growth, aes(x = MDAT, y = HWAM, colour = as.factor(TRTNO), size = "Observed"), shape = 20) +
    # General appearance
    scale_colour_manual(name = "Fertilization (kg[N]/ha)",
                        breaks = c("1","2","3","4"),
                        labels = c("0","100","200","300"),
                        values = c("#20854E","#FFDC91", "#E18727", "#BC3C29")) +
    scale_size_manual(values = c("Simulated" = 1, "Observed" = 2), limits = c("Simulated", "Observed")) +
    scale_linewidth_manual(values = c("Simulated" = 1, "Observed" = 2), limits = c("Simulated", "Observed")) +
    labs(size = NULL, linewidth = NULL, y = "Yield (kg/ha)") +
    guides(
      size = guide_legend(
        override.aes = list(linetype = c("solid", "blank"), shape = c(NA, 16))
      )
    ) +
    theme_bw() + 
    theme(legend.text = element_text(size = 8), legend.title = element_text(size = 8),
          axis.title.x = ggplot2::element_blank(), axis.title.y = element_text(size = 10),
          axis.text = element_text(size = 9, colour = "black"))
  
  return(plot_growth)
}
