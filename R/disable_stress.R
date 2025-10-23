#' Disable Water and/or Nitrogen Stress in DSSAT Simulations
#'
#' Modifies DSSAT management tables to add treatments with automatic irrigation
#' and/or nitrogen saturation, disabling water and/or nitrogen stress.
#'
#' @param xtables A named list of DSSAT tables (data frames), including management and treatment sections.
#' @param stress Character vector. Which stresses to disable. Options are \code{"water"}, \code{"nitrogen"}, or both (default: \code{c("water", "nitrogen")}).
#'
#' @details
#' For water stress, the function adds a simulation control row with auto-irrigation enabled. For nitrogen stress, it adds a fertilizer management row with high N application. It then creates a new treatment referencing these management levels, with a descriptive treatment name. The function uses helper functions such as \code{get_xfile_sec}, \code{add_management}, and \code{add_treatment}.
#'
#' @return The input list with the new treatment(s) added to disable the specified stress(es).
#'
#' @examples
#' \dontrun{
#' xtables <- disable_stress(xtables, stress = c("water", "nitrogen"))
#' }
#'
#' @export

disable_stress <- function(xtables, stress = c("water", "nitrogen")) {
  
  if ("water" %in% stress) {
    sm <- get_xfile_sec(xtables, "SIMULATION")
    sm <- sm[max(nrow(sm), 1), ]  # Keep last row if multiple levels exist
    sm_list <- lapply(sm, identity)
    sm_list$WATER <- "Y" 
    sm_list$IRRIG <- "A"
    
    xtables <- add_management(xtables, section = "simulation_controls", args = sm_list)
    
    sm_index <- max(get_xfile_sec(xtables, "SIMULATION")[1])
  }
  
  if ("nitrogen" %in% stress) {
    fe_list <- list(
      FDATE = c("1981-10-29", "1982-03-24", "1981-06-10"),
      FMCD = rep("FE041", 3), FACD = rep("AP001", 3), FDEP = 1,
      FAMN = 120, FAMP = 0, FAMK = 0, FAMC = 0, FAMO = 0
    )
    
    xtables <- add_management(xtables, section = "fertilizer", args = fe_list)
    
    fe_index <- max(get_xfile_sec(xtables, "FERTILIZER")[1])
  }
  
  if (identical(sort(stress), c("nitrogen", "water"))) {
    attrs <- list(TNAME = "Auto-irrigation + N saturation", SM = sm_index, MF = fe_index)
  } else if ("water" %in% stress) {
    attrs <- list(TNAME = "Auto-irrigation", SM = sm_index)
  } else if ("nitrogen" %in% stress) {
    attrs <- list(TNAME = "N saturation", MF = fe_index)
  }
  
  out <- add_treatment(xtables, args = attrs)
  return(out)
}