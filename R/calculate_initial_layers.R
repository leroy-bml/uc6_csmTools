#' Wrapper to calculate initial soil conditions for DSSAT simulations
#'
#' Calculates initial soil conditions for water and/or nitrogen based on single input values. 
#' 
#' @param soil_profile A data.frame representing a DSSAT soil profile.
#' @param percent_available_water A numeric value (0-100); available water for entire profile (%).
#' @param total_n_kgha A numeric value; total nitrogen fo entire profile (kg/ha).
#'
#' @return A data.frame representing the DSSAT initial condition layers section ('SH2O', 'SNO3', 'SNH4')
#'   for each soil layer defined in the input soil profile.
#'
#' @export
#'
#' @examples
#' # Define a sample soil profile
#' soil_df <- data.frame(
#'   SLB = c(15, 30, 60, 90),       # Depth at bottom of layer (cm)
#'   SLLL = c(0.10, 0.12, 0.15, 0.18), # Wilting Point (cm3/cm3)
#'   SDUL = c(0.25, 0.28, 0.32, 0.35), # Field Capacity (cm3/cm3)
#'   SBDM = c(1.5, 1.4, 1.4, 1.3)       # Bulk Density (g/cm3)
#' )
#'
#' # Calculate both water and nitrogen
#' calculate_initial_conditions(soil_df, percent_available_water = 80, total_n_kgha = 25)
#'
#' # Calculate only water
#' calculate_initial_conditions(soil_df, percent_available_water = 50)
#'
#' # Calculate only nitrogen
#' calculate_initial_conditions(soil_df, total_n_kgha = 40)
#' 

calculate_initial_layers <- function(soil_profile, percent_available_water = NULL, total_n_kgha = NULL) {
  
  if(is.null(percent_available_water) && is.null(total_n_kgha)){
    stop("Please provide a value for either percent_available_water or total_n_kgha.")
  }
  
  # Start with the original soil layer depths
  result <- data.frame(ICBL = soil_profile$SLB)
  
  # Calculate water content if a value is provided
  if (!is.null(percent_available_water)) {
    result$SH2O <- .calculate_soil_water(soil_profile, percent_available_water)
  } else {
    result$SH2O <- NA
  }
  
  # Calculate nitrogen concentration if a value is provided
  if (!is.null(total_n_kgha)) {
    nitrogen_values <- .calculate_soil_nitrogen(soil_profile, total_n_kgha)
    result$SNH4 <- nitrogen_values$SNH4
    result$SNO3 <- nitrogen_values$SNO3
  } else {
    result$SNH4 <- NA
    result$SNO3 <- NA
  }
  
  return(result)
}

#' Calculate initial volumetric soil water by layer (SH2O)
#'
#' Calculates water content (SH2O) for each soil layer based on a specified percentage of available water.
#' (Replicates DSSAT XBuild logic).
#'
#' @param soil_profile A data.frame; DSSAT soil profile with 'SLLL' (Wilting Point) and 'SDUL' (Field Capacity) columns.
#' @param percent_available_water Numeric percentage of available water.
#'
#' @return A numeric vector of the calculated SH2O for each soil layer.
#' 
#' @noRd
#' 

.calculate_soil_water <- function(soil_profile, percent_available_water) {
  
  if (!all(c("SLLL", "SDUL") %in% names(soil_profile))) {
    stop("Water retention properties ('SLLL' and 'SDUL') missing from soil profile data.")
  }
  if (percent_available_water < 0 || percent_available_water > 100) {
    warning("percent_available_water should be between 0 and 100.")
  }
  
  sh2o <- soil_profile$SLLL + (percent_available_water / 100) * (soil_profile$SDUL - soil_profile$SLLL)
  
  return(sh2o)
}


#' Calculate initial soil nitrate (SNO3) and ammonium (SNH4) content
#'
#' Calculates a uniform SNO3 and SNH4 concentration (ppm) for all layers based on a total N (kg/ha),
#' split 90% NO3 / 10% NH4. Replicates DSSAT XBuild logic.
#'
#' @param soil_profile A data.frame; DSSAT soil profile with 'SLB' (layer depth) and 'SBDM' (Bulk Density) columns.
#' @param total_n_kgha Numeric total N (kg/ha) in the profile.
#'
#' @return A list with calculated 'SNO3' and 'SNH4' concentrations (ppm).
#' 
#' @noRd
#' 

.calculate_soil_nitrogen <- function(soil_profile, total_n_kgha) {
  
  if (!"SBDM" %in% names(soil_profile)) {
    stop("Bulk density ('SBDM') missing from soil profile data.")
  }
  
  # Replace missing bulk density values with a default of 1.2
  soil_profile$SBDM[is.na(soil_profile$SBDM)] <- 1.2
  
  # Calculate layer thickness
  layer_depths_cm <- soil_profile$SLB
  layer_thicknesses_cm <- c(layer_depths_cm[1], diff(layer_depths_cm))
  
  # Total profile depth
  total_depth_cm <- tail(layer_depths_cm, 1)
  
  # Calculate depth-weighted average bulk density
  weighted_bd_sum <- sum(soil_profile$SBDM * layer_thicknesses_cm)
  bd_average <- weighted_bd_sum / total_depth_cm
  
  # Calculate NO3 and NH4 concentration in ppm (g[N]/Mg[soil])
  # The denominator (0.1 * bd_average * total_depth_cm) converts soil volume to Mg/ha
  # Total N (kg/ha) is split 90% to NO3 and 10% to NH4
  sno3_calculated <- (0.9 * total_n_kgha) / (0.1 * bd_average * total_depth_cm)
  snh4_calculated <- (0.1 * total_n_kgha) / (0.1 * bd_average * total_depth_cm)
  
  return(list(SNO3 = sno3_calculated, SNH4 = snh4_calculated))
}

