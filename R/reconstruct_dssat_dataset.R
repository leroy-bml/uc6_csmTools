#' Helper: Rebuild the final dataset and link IDs
#'
#' Takes the original dataset and the processed tables, links the IDs
#' in the FIELDS table, reconstructs the final list, and updates
#' EXP_ID globally.
#'
#' @param dataset The original dataset list.
#' @param data_nms The list of processed tables (MANAGEMENT_CORE,
#'   SOIL, WEATHER) that have 'code_map' attributes.
#'
#' @return The final, correctly structured dataset list.
#' 
#' @noRd
#' 

reconstruct_dssat_dataset <- function(dataset, data_nms) {

  dataset_out <- dataset

  # --- Extract code maps ---
  exp_code_map <- attr(data_nms$MANAGEMENT_CORE, "code_map")
  wsta_code_map <- attr(data_nms$WEATHER, "code_map")
  soil_code_map <- attr(data_nms$SOIL, "code_map")

  # --- Update soil and weather links in FIELDS table ---
  dataset_out[["FIELDS"]] <- dataset[["FIELDS"]] %>%
    dplyr::left_join(
      exp_code_map,
      by = intersect(colnames(dataset[["FIELDS"]]), colnames(exp_code_map))
    ) %>%
    dplyr::left_join(
      wsta_code_map,
      by = intersect(colnames(dataset[["FIELDS"]]), colnames(wsta_code_map))
    ) %>%
    dplyr::left_join(
      soil_code_map,
      by = intersect(colnames(dataset[["FIELDS"]]), colnames(soil_code_map))
    ) %>%
    dplyr::mutate(
      ID_FIELD = ID_FIELD_new,
      WSTA = INSI_new,
      ID_SOIL = PEDON_new,
      EXP_ID = EXP_ID_new
    ) %>%
    dplyr::left_join(
      data_nms$MANAGEMENT_CORE,
      by = c("EXP_ID", "L", "XCRD", "YCRD") # Your original keys
    ) %>%
    dplyr::select(EXP_ID, dplyr::any_of(colnames(FIELDS_template)),
                  dplyr::any_of(grep("_NOTES|_COMMENTS", names(.), value = TRUE)))

  # --- Reconstruct Management tables ---
  mngt_data_out <- list()
  mngt_data_out[["GENERAL"]] <- data_nms$MANAGEMENT_CORE %>%
    dplyr::select(file_name,
                  dplyr::any_of(union(colnames(dataset[["GENERAL"]]),
                                      colnames(GENERAL_template))),
                  any_of(grep("_NOTES|_COMMENTS", names(.), value = TRUE)))
  mngt_data_out[["CULTIVARS"]] <- data_nms$MANAGEMENT_CORE %>%
    dplyr::select(dplyr::any_of(union(colnames(dataset[["CULTIVARS"]]),
                                      colnames(CULTIVARS_template))),
                  any_of(grep("_NOTES|_COMMENTS", names(.), value = TRUE)))
  dataset_out[names(mngt_data_out)] <- mngt_data_out

  # --- Reconstruct Soil profile table ---
  soil_data_out <- list()
  soil_data_out[["SOIL_META"]] <- data_nms$SOIL %>%
    dplyr::select(file_name, EXP_ID, INST_NAME, PEDON, YEAR,
                  intersect(colnames(SOIL_META_template), colnames(data_nms$SOIL)),
                  dplyr::any_of(grep("_NOTES|_COMMENTS", names(.), value = TRUE))) %>%
    dplyr::distinct()
  soil_data_out[["SOIL_GENERAL"]] <- data_nms$SOIL %>%
    dplyr::select(file_name, EXP_ID, INST_NAME, PEDON, YEAR,
                  intersect(colnames(SOIL_GENERAL_template), colnames(data_nms$SOIL)),
                  dplyr::any_of(grep("_NOTES|_COMMENTS", names(.), value = TRUE))) %>%
    dplyr::distinct()
  soil_data_out[["SOIL_LAYERS"]] <- data_nms$SOIL %>%
    dplyr::select(file_name, EXP_ID, INST_NAME, PEDON, YEAR,
                  intersect(colnames(SOIL_template), colnames(data_nms$SOIL)),
                  dplyr::any_of(grep("_NOTES|_COMMENTS", names(.), value = TRUE)))
  dataset_out[names(soil_data_out)] <- soil_data_out

  # --- Reconstruct Weather station tables ---
  wth_data_out <- list()
  wth_data_out[["WEATHER_DAILY"]] <- data_nms$WEATHER %>%
    dplyr::select(file_name, WST_NAME, EXP_ID, INSI, YEAR,
                  intersect(colnames(WEATHER_template), colnames(data_nms$WEATHER)),
                  dplyr::any_of(grep("_NOTES|_COMMENTS", names(.), value = TRUE)))
  wth_data_out[["WEATHER_METADATA"]] <- data_nms$WEATHER %>%
    dplyr::select(file_name, WST_NAME, EXP_ID, INSI, YEAR,
                  intersect(colnames(WEATHER_header_template), colnames(data_nms$WEATHER)),
                  dplyr::any_of(grep("_NOTES|_COMMENTS", names(.), value = TRUE))) %>%
    dplyr::distinct()
  dataset_out[names(wth_data_out)] <- wth_data_out


  # --- Update experiment code globally ---
  exp_code_vector <- exp_code_map |>
    dplyr::select(EXP_ID, EXP_ID_new) |>
    tibble::deframe()

  dataset_out <- lapply(dataset_out, function(df) {
    if ("EXP_ID" %in% names(df)) {
      map_filter <- names(exp_code_vector) %in% unique(df$EXP_ID)
      map_filtered <- exp_code_vector[map_filter]

      if (length(map_filtered) > 0) {
        df <- dplyr::mutate(
          df,
          EXP_ID = dplyr::recode(EXP_ID, !!!map_filtered)
        )
      }
    }
    return(df)
  })

  return(dataset_out)
}
