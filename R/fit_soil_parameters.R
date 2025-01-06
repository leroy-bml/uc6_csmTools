library(DSSAT)
library(dplyr)
library(tidyr)
library(ggplot2)

#' ######
#' 
#' @param crop
#' @param model
#' 
#' @importFrom DSSAT read_cul read+eco
#' 
#' @return ###

soiltest <- read_sol("C:/DSSAT48/SoilGrids/dataverse_files/SoilGrids-for-DSSAT-10km v1.0 (by country)/GE.SOL", id_soil = "GE02408882")
soiltest <- soiltest %>% unnest(all_of(colnames(soiltest)))
seq_nwheat <- c(5,10,20,30,40,50,60,70,90,110,130,150,170,190,210)  # default soil layer sequence for Nwheat modelk


approx_profile <- function(data, depth_seq = c(5,10,20,30,40,50,60,70,90,110,130,150,170,190,210), method = "linear"){
  
  # data <- soiltest
  # depth_seq <- seq_nwheat
  # method <- "linear"
  
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
    geom_point(aes(x = SLB, y = value, colour = src)) +
    geom_smooth(aes(x = SLB, y = value), method = "loess", formula = "y ~ x") +
    facet_wrap(~var, ncol = 4, scales = "free_y") +
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

tmp <- approx_profile(data = soiltest, depth_seq = seq_nwheat, method = "linear")  # example: linear interpolation
tmp2 <- approx_profile(data = soiltest, depth_seq = seq_nwheat, method = "constant") # example: duplicate


#' ######
#' 
#' @param crop
#' @param model
#' 
#' @importFrom DSSAT read_cul read+eco
#' 
#' @return ###

# water turns IRRIG from R to A
# nitrogen add FERT regime, for winter wheat: 120kg/ha 15cm (y-1)300doy ; 120kg/ha 1cm 60doy ; 120kg/ha 1cm 150doy
# required: PDATE, CRID (winter, wheat!)