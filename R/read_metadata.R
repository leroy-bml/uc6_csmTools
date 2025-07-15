#' Read and format experiment metadata from an BonaRes repository metadata XML file
#'
#' This function reads a metadata XML file based on the BonaRes metadata schema and extracts key fields for use as input arguments in crop experiment data wrangling functions.
#' It parses spatial, temporal, provenance, contact, legal, and funding information, and returns a tidy one-row tibble suitable for downstream processing.
#'
#' @param path Character. Path to the metadata XML file.
#' @param repo Character. Name of the data repository (default: "bonares"). Currently not used in the function logic, but reserved for future extensions.
#' @param sheet Character or NULL. Optional sheet name for variable key extraction (not currently implemented).
#'
#' @return A tibble (one row) with the following columns:
#' \describe{
#'   \item{METADATA.Experiment_name}{Experiment name(s) as a single string.}
#'   \item{METADATA.Experiment_title}{Experiment title(s) as a single string.}
#'   \item{METADATA.Experiment_doi}{Experiment DOI(s) as a single string.}
#'   \item{METADATA.Contact_persons}{Contact person(s), separated by "; ".}
#'   \item{METADATA.Institutions}{Institution(s), separated by "; ".}
#'   \item{METADATA.Contact_email}{Contact email(s), separated by "; ".}
#'   \item{METADATA.Legal_constraints}{Legal constraints, separated by "; ".}
#'   \item{METADATA.Funding}{Funding information, separated by "; ".}
#'   \item{METADATA.Date_published}{Earliest date found in the metadata (as.Date).}
#'   \item{METADATA.Date_revised}{Most recent date found in the metadata (as.Date).}
#'   \item{FIELDS.Coordinate_system}{Coordinate system as a string (e.g., "EPSG:4326 v8.9").}
#'   \item{FIELDS.Longitude}{Mean longitude (numeric) from west/east bounds.}
#'   \item{FIELDS.Latitude}{Mean latitude (numeric) from north/south bounds.}
#' }
#'
#' @details
#' The function is designed for BonaRes and similar metadata standards, but can be adapted for other ISO 19139-based schemas. It uses XPath queries to extract relevant fields and collapses multiple values with "; " where appropriate. Dates are parsed as `Date` objects and the earliest/latest are used for publication and revision, respectively.
#'
#' @seealso [xml2::read_xml()], [tibble::tibble()]
#'
#' @examples
#' \dontrun{
#' # Read metadata from XML file
#' meta <- read_metadata("path/to/metadata.xml")
#' print(meta)
#' }
#'
#' @importFrom xml2 read_xml xml_ns xml_text xml_find_all xml_find_first
#' @importFrom tibble tibble
#'
#' @export
#' 

read_metadata <- function(path, repo = "bonares", sheet = NULL) {
  
  # Helper to extract, filter, and collapse text
  extract_collapse <- function(doc, xpath, ns, collapse = "; ") {
    vals <- xml2::xml_text(xml2::xml_find_all(doc, xpath, ns))
    vals <- vals[nzchar(vals)]
    paste(vals, collapse = collapse)
  }
  extract_first <- function(doc, xpath, ns) {
    val <- xml2::xml_text(xml2::xml_find_first(doc, xpath, ns))
    ifelse(nzchar(val), val, NA_character_)
  }
  
  # Read metadata file
  metadata <- xml2::read_xml(path)
  # Get namespace mapping
  ns <- xml2::xml_ns(metadata)
  
  
  ##---- Spatial coverage ----
  coord_system <- paste0(
    extract_first(extent, ".//gmd:referenceSystemIdentifier/gmd:RS_Identifier/gmd:codeSpace/gco:CharacterString", ns), ":",
    extract_first(extent, ".//gmd:referenceSystemIdentifier/gmd:RS_Identifier/gmd:code/gco:CharacterString", ns), " v",
    extract_first(extent, ".//gmd:referenceSystemIdentifier/gmd:RS_Identifier/gmd:version/gco:CharacterString", ns)
  )
  west_bound <- as.numeric(extract_first(metadata, ".//gmd:westBoundLongitude/gco:Decimal", ns))
  east_bound <- as.numeric(extract_first(metadata, ".//gmd:eastBoundLongitude/gco:Decimal", ns))
  north_bound <- as.numeric(extract_first(metadata, ".//gmd:northBoundLatitude/gco:Decimal", ns))
  south_bound <- as.numeric(extract_first(metadata, ".//gmd:southBoundLatitude/gco:Decimal", ns))
  
  
  ##---- Temporal coverage ----
  # NOTE 2025-07-14: not in schema anymore!
  # exp_start <- as.numeric(extract_first(metadata, ".//gml:beginPosition", ns))
  # exp_end <- as.numeric(extract_first(metadata, ".//gml:endPosition", ns))
  
  
  ##---- Provenance metadata ----
  # Experiment metadata
  exp_name  <- extract_collapse(metadata, ".//gmd:hierarchyLevelName/gco:CharacterString", ns)
  exp_title <- extract_collapse(metadata, ".//gmd:identificationInfo/bnr:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString", ns)
  exp_doi   <- extract_collapse(metadata, ".//gmd:CI_Citation/gmd:identifier/gmd:MD_Identifier/gmd:code/gco:CharacterString", ns)
  # Contacts
  persons <- extract_collapse(metadata, ".//gmd:contact/gmd:CI_ResponsibleParty/gmd:individualName/gco:CharacterString", ns)
  institutions <- extract_collapse(metadata, ".//gmd:contact/gmd:CI_ResponsibleParty/gmd:organisationName/gco:CharacterString", ns)
  emails <- extract_collapse(metadata, ".//gmd:contact/gmd:CI_ResponsibleParty/gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:electronicMailAddress/gco:CharacterString", ns)
  # Legal constraints
  legalConstraints <- extract_collapse(metadata, ".//gmd:identificationInfo/bnr:MD_DataIdentification/gmd:resourceConstraints", ns)
  # Funding
  funding <- c(
    extract_first(metadata, ".//bnr:funderName/gco:CharacterString", ns),
    extract_first(metadata, ".//bnr:MD_FundingReference/bnr:identifier/bnr:MD_ReferenceIdentifier/bnr:identifier/gco:CharacterString", ns),
    extract_first(metadata, ".//bnr:awardNumber/gco:CharacterString", ns),
    extract_first(metadata, ".//bnr:awardTitle/gco:CharacterString", ns)
  )
  funding <- paste(funding[nzchar(funding)], collapse = "; ")
  
  
  ##---- Dates ----
  dates <- xml2::xml_text(
    xml2::xml_find_all(
      metadata,
      ".//gmd:identificationInfo/bnr:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gco:Date",
      ns))
  dates <- as.Date(dates)
  dates <- dates[!is.na(dates)]
  date_published <- if (length(dates) > 0) min(dates) else NA
  date_revised   <- if (length(dates) > 0) max(dates) else NA
  
  
  ##---- Variable key ----
  # TODO: on pause
  # var_key <- read_var_key(path, repo, sheet)
  
  
  ##---- Output ----
  tibble(
    METADATA.Experiment_name   = exp_name,
    METADATA.Experiment_title  = exp_title,
    METADATA.Experiment_doi    = exp_doi,
    METADATA.Contact_persons   = persons,
    METADATA.Institutions      = institutions,
    METADATA.Contact_email     = emails,
    METADATA.Legal_constraints = legalConstraints,
    METADATA.Funding           = funding,
    METADATA.Date_published    = date_published,
    METADATA.Date_revised      = date_revised,
    FIELDS.Coordinate_system   = coord_system,
    FIELDS.Longitude           = mean(c(west_bound, east_bound), na.rm = TRUE),
    FIELDS.Latitude            = mean(c(north_bound, south_bound), na.rm = TRUE)
  )
}

#' OUTDATE; To revise
#' 
#' 

# TODO: revise var key; redefine objectives?
#
# read_var_key <- function(path, repo, sheet = NULL){
#   
#   # Read metadata file
#   metadata <- xml2::read_xml(path)
#   # Get namespace mapping
#   ns <- xml2::xml_ns(metadata)
#   
#   # Extract variable key
#   key <- switch(repo,
#                 "bnr" = {
#                   # Find all nodes that match a given XPath expression
#                   var_nodes <- xml2::xml_find_all(metadata, "//bnr:MD_Column")
#                   # Extract the data for each variable as a named vector
#                   vars <- map(var_nodes, function(x) {
#                     tbl_name <- xml2::xml_text(xml2::xml_find_first(x, ".//bnr:tableName"))
#                     name <- xml2::xml_text(xml2::xml_find_first(x, ".//bnr:name"))
#                     descript <- xml2::xml_text(xml2::xml_find_first(x, ".//bnr:description"))
#                     methods <- xml2::xml_text(xml2::xml_find_first(x, "//bnr:methods"))
#                     unit <- xml2::xml_text(xml2::xml_find_first(x, ".//bnr:unit"))
#                     data_type <- xml2::xml_text(xml2::xml_find_first(x, ".//bnr:dataType"))
#                     nas <- xml2::xml_text(xml2::xml_find_first(x, "//bnr:missingValue"))
#                     # Group into a list
#                     list(tbl_name = tbl_name, name = name, description = descript, methods = methods, unit = unit, 
#                          data_type = data_type, missing_vals = nas)
#                   })
#                   # Combine the list of named vectors into a data frame
#                   key <- bind_rows(vars)
#                 },
#                 "dwd" = {
#                   # Find all nodes that match a given XPath expression
#                   var_nodes <- xml2::xml_find_all(metadata, "//MetElement")
#                   # Extract the data for each variable as a named vector
#                   vars <- map(var_nodes, function(x) {
#                     name <- xml2::xml_text(xml2::xml_find_first(x, ".//ShortName"))
#                     unit <- xml2::xml_text(xml2::xml_find_first(x, ".//UnitOfMeasurement"))
#                     descript <- xml2::xml_text(xml2::xml_find_first(x, ".//Description"))
#                     # Group into a list
#                     list(name = name, description = descript, unit = unit)
#                   })
#                   # Combine the list of named vectors into a data frame
#                   key <- bind_rows(vars)
#                 },
#                 c("hdv","zdp","opa","sradi","odja") %in% {
#                   # Determine file format
#                   ext <- gsub("\\.", "", substr(metadata, nchar(metadata)-4+1, nchar(metadata)))
#                   # Read key
#                   switch(ext,
#                          "txt" = {
#                            key <- read.delim(metadata, header = TRUE, sep = "\t", fill = TRUE)
#                          },
#                          "csv" = {
#                            lines <- readLines(metadata, n = 1)
#                            if (grepl(",", lines)) { 
#                              key <- read.csv(metadata, header = TRUE, sep = ",", fill = TRUE)
#                            } else if (grepl(";", lines)) {
#                              key <- read.csv(metadata, header = TRUE, sep = ";", fill = TRUE)
#                            }
#                          },
#                          "xlsx" = {
#                            key <- read_excel(metadata, sheet = sheet)
#                          },
#                          "ods" = {
#                            key <- read_ods(metadata, sheet = sheet)
#                          },
#                          {
#                            print("Invalid file format") # add json/xml
#                          }
#                   )
#                   
#                 }
#   )
#   
#   return(key)
# }
