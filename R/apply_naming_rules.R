#'
#'

apply_naming_rules <- function(dataset) {
  
  # --- Extract data components ---
  dssat_components <- split_dssat_components(
    dataset,
    sec = c("MANAGEMENT_CORE", "SOIL", "WEATHER"),
    merge = TRUE
  )
  
  # --- Fill gaps in metadata for DSSAT code components ---
  data_enriched <- enrich_metadata(dssat_components)
  
  # --- Generate DSSAT codes ---
  data_nms <- resolve_dssat_codes(data_enriched)
  # TODO: add file names to DSSAT code logic
  
  # --- Update input dataset with standardized names ---
  dataset_out <- reconstruct_dssat_dataset(dataset, data_nms)
  
  # --- Data cleaning ---
  # Deduplicate all dataframes
  dataset_out <- apply_recursive(dataset_out, dplyr::distinct)
  
  return(dataset_out)
}
