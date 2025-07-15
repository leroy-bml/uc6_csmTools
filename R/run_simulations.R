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
#' @importFrom DSSAT write_filex write_filea write_filet write_wth write_sol write_cul write_dssbatch run_dssat read_output
#' 
#' @export
#' 

run_simulations <- function(dataset, framework = "dssat", dssat_dir = NULL, sim_dir = NULL, args = list()) {
  
  #
  filex <- dataset$X
  filea <- dataset$A
  filet <- dataset$`T`
  file_wth <- dataset$WTH
  file_sol <- dataset$SOL
  file_cul <- dataset$CUL
  
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
  
  # Set default starting date as the first management date if not specified
  # TODO: add controls and error handling for mandatory variables
  if (is.na(filex$SIMULATION_CONTROLS$SDATE)){  ### SPECIFY DATE
    
    all_dates <- na.omit(  # ignore warning
      as.POSIXct(
        as.numeric(
          unlist(lapply(filex, function(df) {
            df[grepl("DAT", colnames(df))]
          }), use.names = FALSE)
        )))
    min_date <- as.Date(min(all_dates))
    filex$SIMULATION_CONTROLS$SDATE <- format(min_date, "%y%j")
  }
  
  # Write all files in adequte location
  write_filex(filex, file_name = attr(filex, "file_name"))  # management
  if(!is.null(filea)) write_filea(filea, file_name = attr(filea, "file_name"))  # observed summary
  if(!is.null(filet)) write_filet(filet, file_name = attr(filet, "file_name"))  # observed time series
  write_wth2(file_wth, file_name = paste0(dssat_dir, "/Weather/", attr(file_wth, "file_name")))  # weather
  write_sol(file_sol, file_name = paste0(dssat_dir, "/Soil/", attr(file_sol, "file_name")), append = FALSE)  # soil profile
  if(!is.null(file_cul)) write_cul(file_cul, file_name = paste0(dssat_dir, "/Genotype/", attr(file_cul, "file_name")))  # cultivars
  
  # Default parameters
  treatments <- min(filex$TREATMENTS$N):max(filex$TREATMENTS$N)
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
    FILEX = rep(attr(filex, "file_name"), length(params$TRTNO)),  # TODO: retrieve filename is not associated as attr (read through regular filex call)
    TRTNO = params$TRTNO,
    RP = params$RP,
    SQ = params$SQ,
    OP = params$OP,
    CO = params$CO
  )
  
  write_dssbatch(batch_tbl)  # write batch file
  run_dssat(run_mode = "B")  # run simulations
  
  # Read output
  #out_files <- list.files(path = sim_dir, pattern = "\\.OUT$", full.names = TRUE)
  #message("UNO")
  
  #output <- list()
  #for (nm in out_files){
  #  print(nm)
  #  output[[nm]] <- read_output(file_name = nm)
  #}
  #names(output) <- out_files
  output <- read_output(file_name = paste0(sim_dir, "/PlantGro.OUT"))
  
  out <- list(plant_growth = as.data.frame(output), OBSERVED_Summary = filea, OBSERVED_TimeSeries = filet)  # Append observed data

  setwd(old_wd)  # reset wd
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
  obs_summary_growth <- sim_output$OBSERVED_Summary %>%
    filter(TRTNO %in% 1:4) %>%
    mutate(MDAT = as.POSIXct(as.Date(MDAT, format = "%y%j")),
           ADAT = as.POSIXct(as.Date(ADAT, format = "%y%j")))
  
  # Simulated data
  sim_growth <- output$plant_growth
  
  # Plot
  plot_growth <- sim_growth %>%
    mutate(TRNO = as.factor(TRNO)) %>%
    ggplot(aes(x = DATE, y = GWAD)) +
    # Line plot for simulated data
    geom_line(aes(group = TRNO, colour = TRNO, linewidth = "Simulated")) +
    # Points for observed data
    geom_point(data = obs_summary_growth, aes(x = MDAT, y = HWAM, colour = as.factor(TRTNO), size = "Observed"), shape = 20) +
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
          axis.title.x = element_blank(), axis.title.y = element_text(size = 10),
          axis.text = element_text(size = 9, colour = "black"))
  
  return(plot_growth)
}

# Mandatory soil profile variables
# soil_dssat$SOIL_METADATA$SLDR <- 0.1  # drainage rate
# soil_dssat$SOIL_METADATA$SLRO <- 70  # runoff curve
# soil_dssat$SOIL_METADATA$SALB <- 0.09  # albedo
# soil_dssat$SOIL_PROFILE_LAYERS$SLLL <- c(0.228,0.228,0.249,0.249,0.207,0.259)  # drained upper limit
# soil_dssat$SOIL_PROFILE_LAYERS$SDUL <- c(0.385,0.385,0.406,0.449,0.341,0.361)  # drained upper limit
# soil_dssat$SOIL_PROFILE_LAYERS$SSAT <- 0.45
# soil_dssat$SOIL_PROFILE_LAYERS$SRGF <- c(1,1,0.638,0.350,0.223,0.018)
# soil_dssat$SOIL_PROFILE_LAYERS$SLNF <- 1
# soil_dssat$SOIL_PROFILE_LAYERS$SLU1 <- 6
# soil_dssat$SOIL_PROFILE_LAYERS$SLPF <- 1


# TODO: eventually a wrapper for plotting essential results likes the obs vs. sim comparisons
# Should link to the input data to retrieve treatment names and levels
# Plot results: phenology
# 
# lteSe_sim_growth <- read_output(file_name = "./inst/extdata/lte_seehausen/2_sim/PlantGro.OUT")
#
# # Plot results: phenology
# lteSe_sim_growth %>%
#   mutate(TRNO = as.factor(TRNO)) %>%
#   ggplot(aes(x = DATE, y = DCCD)) +
#   # Zadoks lines for comparison
#   geom_hline(yintercept = 69, linetype = "dashed", colour = "black") +  # anthesis date (Zadoks65)
#   geom_hline(yintercept = 95, linetype = "dashed", colour = "black") +  # maturity date (Zadoks95)
#   # Line plot for simulated data
#   geom_line(aes(group = TRNO, colour = TRNO, linewidth = "Simulated")) +
#   # Points for observed data
#   geom_point(data = lteSe_obs_growth, aes(x = ADAT, y = 69, colour = as.factor(TRTNO), size = "Observed"),
#              shape = 20) +  # obs anthesis date (Zadosk65)
#   geom_point(data = lteSe_obs_growth, aes(x = MDAT, y = 95, colour = as.factor(TRTNO), size = "Observed"),
#              shape = 20) +  # obs maturity data (Zadoks95)
#   # General appearance
#   scale_colour_manual(name = "Fertilization (kg[N]/ha)",
#                       breaks = c("1","2","3","4"),
#                       labels = c("0","100","200","300"),
#                       values = c("#20854E","#FFDC91", "#E18727", "#BC3C29")) +
#   scale_size_manual(values = c("Simulated" = 1, "Observed" = 2), limits = c("Simulated", "Observed")) +
#   scale_linewidth_manual(values = c("Simulated" = 1, "Observed" = 2), limits = c("Simulated", "Observed")) +
#   labs(size = NULL, linewidth = NULL, y = "Zadoks scale") +
#   guides(
#     size = guide_legend(
#       override.aes = list(linetype = c("solid", "blank"), shape = c(NA, 16))
#     )
#   ) +
#   theme_bw() + 
#   theme(legend.text = element_text(size = 8), legend.title = element_text(size = 8),
#         axis.title.x = element_blank(), axis.title.y = element_text(size = 10),
#         axis.text = element_text(size = 9, colour = "black"))