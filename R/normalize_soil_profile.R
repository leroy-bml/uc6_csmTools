#' Interpolate Soil Profile Data to a given depth sequence
#'
#' Interpolates numeric soil profile variables to a standard set of depths,
#' producing a harmonized profile and a diagnostic plot.
#'
#' @param data A data frame containing soil profile data, with a depth column named \code{"SLB"} and subsequent columns for profile variables.
#' @param depth_seq Numeric vector. The standard depths (in cm) to which the profile should be interpolated. Default: typical DSSAT depths.
#' @param method Character. Interpolation method to use (passed to \code{approx}). Default is \code{"linear"}.
#'
#' @details
#' The function identifies the depth column (\code{SLB}), separates header, profile, and categorical columns, and interpolates numeric profile variables to the specified standard depths using linear interpolation (or another method supported by \code{approx}). It combines the interpolated and original data, duplicates header values for interpolated layers, and returns a harmonized data frame. A diagnostic plot is also produced, showing input and interpolated values for each variable.
#'
#' The function uses the \strong{dplyr}, \strong{tidyr}, and \strong{ggplot2} packages for data manipulation and plotting.
#'
#' @return A list with two elements:
#' \describe{
#'   \item{\code{data}}{A data frame with interpolated profile data at standard depths.}
#'   \item{\code{plot}}{A ggplot object showing the interpolation results for each variable.}
#' }
#'
#' @examples
#' \dontrun{
#' result <- approx_profile(soil_data)
#' print(result$plot)
#' head(result$data)
#' }
#' 
#' @importFrom magrittr %>%
#' @importFrom tidyselect everything
#' @importFrom dplyr mutate relocate right_join filter arrange select across
#' @importFrom tidyr gather
#' @importFrom ggplot2 ggplot geom_point geom_line facet_wrap theme_minimal aes
#' 
#' @export

# soiltest <- read_sol("C:/DSSAT48/SoilGrids/dataverse_files/SoilGrids-for-DSSAT-10km v1.0 (by country)/GE.SOL", id_soil = "GE02408882")
# soiltest <- soiltest %>% unnest(all_of(colnames(soiltest)))
# seq_nwheat <- c(5,10,20,30,40,50,60,70,90,110,130,150,170,190,210)  # default soil layer sequence for Nwheat modelk
# TODO: compare base and given seq + aggregate?

normalize_soil_profile <- function(data,
                                   depth_seq = c(5,10,20,30,40,50,60,70,90,110,130,150,170,190,210),
                                   method = "linear") {
  
  depth_col <- which(colnames(data) == "SLB")
  headers <- data[, 1:depth_col]
  profile <- data[, depth_col:ncol(data)]  # profile profile starting with depth col in standard DSSAT format
  profile_nas <- profile[, colSums(is.na(profile)) == nrow(profile)]  # stored only NAs
  profile_fct <- profile[, sapply(profile, is.character)]  #
  profile <- profile[, colSums(is.na(profile)) < nrow(profile)]  # remove missing variables (only NAs)
  profile <- profile[, sapply(profile, is.numeric)]  # remove categorical variables
  
  interp <- data.frame(SLB = setdiff(depth_seq, profile$SLB))
  for (i in 2:ncol(profile)){
    approx <- approx(x = profile$SLB, y = profile[[i]], xout = setdiff(depth_seq, profile$SLB), method = method, rule = 2)
    interp <- cbind(interp, approx$y)
  }
  names(interp) <- names(profile)
  
  std_profile <- rbind(profile, interp)
  
  # Result control plots
  plot_df <- std_profile %>%
    mutate(src = ifelse(SLB %in% setdiff(depth_seq, profile$SLB), "interpolated", "input")) %>%
    relocate(src, .before = SLB) %>%
    gather("var", "value", 3:ncol(.))
  
  plot <- ggplot(plot_df) +
    geom_point(aes(x = value, y = -SLB, colour = src)) +
    geom_line(aes(x = value, y = -SLB)) +
    #geom_smooth(aes(x = SLB, y = value), method = "loess", formula = "y ~ x") +
    facet_wrap(~var, ncol = 4, scales = "free_x") +
    theme_minimal()
  
  data_out <- cbind(headers, profile_nas, profile_fct) %>%
    right_join(std_profile, by = "SLB") %>%
    mutate(across(everything(), ~ {
      unique_vals <- unique(na.omit(.))
      if (length(unique_vals) == 1) {
        replace_na(., unique_vals)
      } else {
        .
      }
    })) %>%  # duplicate all header values in interpolated layers
    mutate(DEPTH = max(SLB)) %>%  # update max depth in header if relevant
    filter(SLB %in% depth_seq) %>%
    arrange(SLB) %>%
    select(colnames(data))
  
  data_out <- collapse_cols(data_out, colnames(data_out[which(apply(data_out, 2, function(x) length(unique(x)))>1)]))
  
  out <- list()
  out$data <- data_out
  out$plot <- plot
  
  return(out)
}