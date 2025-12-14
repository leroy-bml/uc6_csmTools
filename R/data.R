#' Growth stages reference data
#'
#' A dataset containing standardized growth stage scales for cereal crops.
#' Includes BBCH and Zadoks scales with corresponding codes.
#'
#' @format A data frame with `r nrow(growth_stages)` rows and `r ncol(growth_stages)` variables:
#' \describe{
#'   \item{scale}{(character) The growth stage scale (bbch, zadoks)}
#'   \item{growth_stage}{(character) Harmonized name of growth stage}
#'   \item{growth_stage_scalespec}{(character) Scale-specific growth stage name}
#'   \item{code}{(numeric) Numeric growth stage code}
#'   \item{description}{(character) description of the growth stage}
#' }
#' @source Internal project data
#' 

"growth_stages"