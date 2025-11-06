#' Enriches DSSAT metadata with imputed and geocoded data
#'
#' A wrapper function that orchestrates the enrichment of MANAGEMENT_CORE,
#' SOIL, and WEATHER data tables.
#'
#' @details
#' This function performs two main stages:
#' 1.  Imputes missing metadata (e.g., coordinates, year) from the
#'     experiment's core metadata.
#' 2.  Enriches all sections with spatial data (elevation, city, country)
#'     from geocoding APIs and formats this data into DSSAT-specific
#'     fields (e.g., SITE, WST_NAME).
#'
#' @param dataset The mapped dataset as a single-level list of data frames.
#'
#' @return A list containing the enriched 'MANAGEMENT_CORE', 'SOIL', and
#'   'WEATHER' data frames.
#'
#' @noRd
#' 

enrich_metadata <- function(dssat_components) {
  
  exp_metadata <- dssat_components$MANAGEMENT_CORE
  soil_data <- dssat_components$SOIL
  wth_data <- dssat_components$WEATHER

  # --- Impute missing metadata ---
  soil_data_imputed <- .impute_soil_metadata(soil_data, exp_metadata)
  wth_data_imputed <- .impute_wth_metadata(wth_data, exp_metadata)
  
  # --- Enrich with Spatial API Data ---
  # TODO: wrap in if statements?
  mngt_data_locations <- enrich_spatial_data(exp_metadata, lat_col = "YCRD", lon_col = "XCRD")
  soil_data_locations <- enrich_spatial_data(soil_data_imputed, lat_col = "LAT", lon_col = "LONG")
  wth_data_locations  <- enrich_spatial_data(wth_data_imputed, lat_col = "LAT", lon_col = "LONG")
  
  # --- Format Spatial Data into DSSAT Fields ---
  mngt_dssat_fmt <- .format_mngt_locations(mngt_data_locations)
  soil_dssat_fmt <- .format_soil_locations(soil_data_locations)
  wth_dssat_fmt  <- .format_wth_locations(wth_data_locations)
  
  # --- Return Final List ---
  out <- list(
    MANAGEMENT_CORE = mngt_dssat_fmt,
    SOIL = soil_dssat_fmt,
    WEATHER = wth_dssat_fmt
  )
  # TODO: add output flatteningto return similar format as output
  
  return(out)
}


#' Helper: Impute missing SOIL metadata
#'
#' Fills missing SOIL fields (INST_NAME, YEAR, LAT, LONG, TEXTURE)
#' using data from the experiment's core metadata.
#'
#' @param soil_data The merged SOIL data table.
#' @param exp_metadata The MANAGEMENT_CORE data table.
#'
#' @return The soil_data data frame with imputed values.
#' 
#' @noRd
#' 

.impute_soil_metadata <- function(soil_data, exp_metadata) {
  
  soil_data %>%
    dplyr::left_join(exp_metadata, by = intersect(names(soil_data), names(exp_metadata))) %>%
    dplyr::mutate(
      INST_NAME = if ("INST_NAME" %in% names(.)) INST_NAME else NA_character_,
      INST_NAME = dplyr::coalesce(INST_NAME, INSTITUTION),
      YEAR_FROM_DATE = if ("DATE" %in% names(.)) {
        suppressWarnings(
          as.Date(as.character(DATE), format = "%y%j") %>%
            lubridate::year() |>
            as.character()
        )
      } else {
        NA_character_
      },
      YEAR = if ("YEAR" %in% names(.)) YEAR else NA_character_,
      YEAR = dplyr::coalesce(YEAR, YEAR_FROM_DATE, EXP_YEAR),
      LAT = if ("LAT" %in% names(.)) LAT else NA_real_,
      LAT = dplyr::coalesce(LAT, YCRD),
      LONG = if ("LONG" %in% names(.)) LONG else NA_real_,
      LONG = dplyr::coalesce(LONG, XCRD),
      TEXTURE = if ("TEXTURE" %in% names(.)) {
        TEXTURE
      } else {
        # (Assuming get_soil_texture is a helper)
        get_soil_texture(
          clay_frac = SLCL,
          silt_frac = SLSI,
          sand_frac = 1.0 - SLCL - SLSI
        )
      }
    ) %>%
    dplyr::select(
      dplyr::all_of(intersect(c("EXP_ID", "PEDON", "INST_NAME", "YEAR", "TEXTURE"), names(.))),
      dplyr::all_of(setdiff(names(soil_data), c("EXP_ID", "PEDON", "INST_NAME", "YEAR", "TEXTURE"))),
    )
}


#' Helper: Impute missing WEATHER metadata
#'
#' Fills missing WEATHER fields (INSI, LAT, LONG, YEAR) using data from the experiment's core metadata.
#'
#' @param wth_data The merged WEATHER data table.
#' @param exp_metadata The MANAGEMENT_CORE data table.
#'
#' @return The wth_data data frame with imputed values.
#' 
#' @noRd
#' 

.impute_wth_metadata <- function(wth_data, exp_metadata) {
  
  wth_data %>%
    dplyr::left_join(exp_metadata, by = intersect(names(wth_data), names(exp_metadata))) %>%
    dplyr::mutate(
      INSI = if ("INSI" %in% names(.)) INSI else NA_character_,
      INSI = dplyr::coalesce(INSI, INSTITUTION),
      LAT = if ("LAT" %in% names(.)) LAT else NA_real_,
      LAT = dplyr::coalesce(LAT, YCRD),
      LONG = if ("LONG" %in% names(.)) LONG else NA_real_,
      LONG = dplyr::coalesce(LONG, XCRD),
      YEAR = if ("YEAR" %in% names(.)) YEAR else NA_character_,
      YEAR = substr(DATE, 1, 2),
    ) %>%
    dplyr::select(
      dplyr::all_of(intersect(c("EXP_ID", "INSI", "LAT", "LONG", "YEAR"), names(.))),
      dplyr::all_of(setdiff(names(wth_data), c("EXP_ID", "INSI", "LAT", "LONG", "YEAR"))),
    )
}


#' Helper: Format spatially-enriched MANAGEMENT data
#'
#' Takes API-enriched data and formats it into DSSAT-specific location metadata.
#'
#' @param mngt_data_locations The MANAGEMENT_CORE table after enrichment.
#'
#' @return A formatted data frame.
#' 
#' @noRd
#' 

.format_mngt_locations <- function(mngt_data_locations) {
  
  mngt_data_locations %>%
    dplyr::mutate(
      FLNAME = if ("FLNAME" %in% names(.)) FLNAME else NA_character_,
      FLNAME = ifelse(is.na(FLNAME), toupper(ShortLabel), FLNAME),
      # Fill site as address if missing
      SITE = if ("SITE" %in% names(.)) SITE else NA_character_,
      SITE_addr = tidyr::unite(data.frame(FLNAME, City, Region, CntryName),
                              "SITE", sep = ", ", na.rm = TRUE)$SITE,
      SITE = ifelse(is.na(SITE), SITE_addr, SITE),
      ELEV = if ("ELEV" %in% names(.)) ELEV else NA_real_,
      ELEV = dplyr::coalesce(ELEV, elevation)
    ) %>%
    dplyr::mutate(
      XCRD_tmp = round(XCRD, 2), 
      YCRD_tmp = round(YCRD, 2), 
      ELEV_tmp = round(ELEV, 2)
    ) %>%
    tidyr::unite("SITE", SITE, XCRD_tmp, YCRD_tmp, ELEV_tmp, sep = "; ", na.rm = TRUE, remove = TRUE)
}


#' Helper: Format spatially-enriched SOIL data
#'
#' Takes API-enriched data and formats it into DSSAT-specific location metadata.
#'
#' @param soil_data_locations The SOIL table after enrichment.
#'
#' @return A formatted data frame.
#' 
#' @noRd
#' 
#' 

.format_soil_locations <- function(soil_data_locations) {
  
  soil_data_locations %>%
    dplyr::group_by(PEDON) %>%
    dplyr::mutate(
      COUNTRY = if ("COUNTRY" %in% names(.)) COUNTRY else NA_character_,
      COUNTRY = ifelse("COUNTRY" %in% names(.),
                       dplyr::coalesce(COUNTRY, CountryCode),
                       countrycode::countrycode(CountryCode, origin = "iso3c", destination = "iso2c")),
      SITE = if ("SITE" %in% names(.)) SITE else NA_character_,
      SITE = ifelse("SITE" %in% names(.),
                    dplyr::coalesce(SITE, paste(City, Region, sep = ",")),
                    paste(City, Region, sep = ", ")),
      ELEV = if ("ELEV" %in% names(.)) ELEV else NA_real_,
      ELEV = dplyr::coalesce(ELEV, elevation)
    )
}


#' Helper: Format spatially-enriched WEATHER data
#'
#' Takes API-enriched data and formats it into DSSAT-specific location metadata.
#'
#' @param wth_data_locations The WEATHER table after enrichment.
#'
#' @return A formatted data frame.
#' 
#' @noRd
#' 

.format_wth_locations <- function(wth_data_locations) {
  
  wth_data_locations %>%
    dplyr::group_by(INSI, LAT, LONG) %>%
    dplyr::mutate(
      WST_NAME = if ("WST_NAME" %in% names(.)) WST_NAME else NA_character_,
      WST_NAME = dplyr::coalesce(WST_NAME, 
                                 tidyr::unite(data.frame(ShortLabel, City, Region, CntryName),
                                              "WST_SITE", sep = ", ", na.rm = TRUE)$WST_SITE),
      ELEV = if ("ELEV" %in% names(.)) ELEV else NA_real_,
      ELEV = dplyr::coalesce(ELEV, elevation),
      REFHT = if ("REFHT" %in% names(.)) REFHT else NA_real_,
      WNDHT = if ("WNDHT" %in% names(.)) WNDHT else NA_real_
    )
}
