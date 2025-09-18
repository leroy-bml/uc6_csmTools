#' Extract Leaf Table Identifiers from Dataset Metadata
#'
#' @importFrom xml2 read_xml xml_ns xml_find_all xml_find_first xml_text
#' @importFrom dplyr bind_rows
#' @importFrom purrr map
#'
#' @param mother_path Character. The path or URL to the main dataset metadata XML file.
#' @param schema Character. The database schema to use for extraction. Default is 'bonares'.
#'
#' @return A data frame (tibble) with columns: \code{identifier}, \code{tbl_name}, and \code{foreign_key}, one row per linked table.
#'
#' @importFrom xml2 read_xml xml_ns xml_find_all xml_text xml_find_first
#' @importFrom purrr map
#' @importFrom dplyr bind_rows
#' 
#' @export

get_leaf_ids <- function(mother_path, schema = "bonares") {
  
  metadata <- xml2::read_xml(mother_path)
  ns <- xml2::xml_ns(metadata)  # Namespace mapping
  
  # Find all variable nodes linked to mother table's foreign keys
  var_nodes <- xml2::xml_find_all(metadata, "//bnr:MD_ForeignKey")
  
  leaves <- map(var_nodes, function(x) {
    id <- xml2::xml_text(xml2::xml_find_first(x, ".//bnr:identifier"))
    tbl_name <- xml2::xml_text(xml2::xml_find_first(x, ".//bnr:table"))
    fkey <- xml2::xml_text(xml2::xml_find_first(x, ".//bnr:column"))
    # Group into a list
    list(identifier = id, tbl_name = tbl_name, foreign_key = fkey)
  })
  bind_rows(leaves)
}

#' Extract Variable Key Metadata from Table XML
#'
#' Parses a table metadata XML file to extract variable (column) information, including names, descriptions, methods, units, data types, and missing value codes.
#'
#' @param path Character. The path or URL to the table metadata XML file.
#' @param schema Character. The database schema to use for extraction. Default is 'bonares'.
#'
#' @return A data frame (tibble) with columns: \code{file_name}, \code{name}, \code{description}, \code{methods}, \code{unit}, \code{data_type}, and \code{missing_vals}, one row per variable.
#'
#' @details
#' This function reads the provided XML file, finds all column nodes, and extracts relevant metadata for each variable. The result is a tidy data frame suitable for further processing or documentation.
#'
#' @importFrom xml2 read_xml xml_ns xml_find_all xml_find_first xml_text
#' @importFrom purrr map
#' @importFrom dplyr bind_rows
#'
#' @export

get_varkey <- function(path, schema = "bonares") {
  
  metadata <- xml2::read_xml(path)
  ns <- xml2::xml_ns(metadata)  # Namespace mapping
  
  # Find all nodes macthing table column descriptions
  var_nodes <- xml2::xml_find_all(metadata, "//bnr:MD_Column")
  # Extract the data for each variable as a named vector
  vars <- map(var_nodes, function(x) {
    file_name <- xml2::xml_text(xml2::xml_find_first(x, ".//bnr:tableName"))
    name <- xml2::xml_text(xml2::xml_find_first(x, ".//bnr:name"))
    descript <- xml2::xml_text(xml2::xml_find_first(x, ".//bnr:description"))
    methods <- xml2::xml_text(xml2::xml_find_first(x, "//bnr:methods"))
    unit <- xml2::xml_text(xml2::xml_find_first(x, ".//bnr:unit"))
    data_type <- xml2::xml_text(xml2::xml_find_first(x, ".//bnr:dataType"))
    nas <- xml2::xml_text(xml2::xml_find_first(x, "//bnr:missingValue"))
    # Group into a list
    list(file_name = file_name,
         name = name,
         description = descript,
         methods = methods,
         unit = unit,
         data_type = data_type,
         missing_vals = nas)
  })
  # Combine the list of named vectors into a data frame
  key <- bind_rows(vars)
  
  return(key)
}

#' Retrieve and Process Variable Keys for Dataset Tables
#'
#' Retrieves variable key metadata for all tables associated with a given dataset path, processes the table names, and returns a combined data frame of variable keys.
#'
#' @param mother_path Character. The root URL or path to the main dataset metadata XML file.
#' @param schema Character. The database schema to use for variable key extraction. Default is 'bonares'.
#'
#' @return A data frame containing processed variable key information for each table, including file names and simplified table names. Duplicate rows are removed.
#'
#' @details
#' This function collects all table metadata XMLs linked to the provided `mother_path`, extracts their variable keys, and processes the table names by removing common prefixes, file extensions, and optional table numbers. The result is a data frame suitable for further data import or processing tasks.
#'
#' @export

get_dataset_varkeys <- function(mother_path, schema = "bonares") {
  
  # Build URL for all table metadata xml
  leaves <- get_leaf_ids(mother_path)
  paths <- paste0("https://maps.bonares.de/finder/resources/dataform/xml/", leaves$identifier)
  paths <- c(mother_path, paths)
  
  keys <- lapply(paths, function(id) get_varkey(path = id, schema = "bonares"))
  names(keys) <- sapply(keys, function(df) as.character(df$file_name[1]))
  keys <- do.call(rbind, keys)
  
  # Simplify table names
  prefix <- find_common_prefix(keys$file_name)  # find common prefix
  tbl_name <- sub(paste0("^", prefix), "", keys$file_name)  # drop common prefix
  tbl_name <- sub("\\..*$", "", tbl_name)  # drop file extension
  tbl_name <- sub("^\\d+_V\\d+_\\d+_", "", tbl_name)  # (optional) table number
  file_name <- paste0(keys$file_name, ".csv")
  
  out <- cbind(file_name, tbl_name, keys[,2:ncol(keys)])
  out[!duplicated(out),]
}


#' Read and format experiment metadata from the BonaRes repository metadata XML file
#'
#' This function reads a metadata XML file based on the BonaRes metadata schema and extracts key fields for use as input arguments in crop experiment data wrangling functions.
#' It parses spatial, temporal, provenance, contact, legal, and funding information, and returns a tidy one-row tibble suitable for downstream processing.
#'
#' @param path Character. Path to the metadata XML file.
#' @param schema Character. Name of the metadata schema (default: "bonares"). Currently not used in the function logic, but reserved for future extensions.
#'
#' @return A tibble (one row) with the following columns:
#' \describe{
#'   \item{Experiment_name}{Experiment name(s) as a single string.}
#'   \item{Experiment_title}{Experiment title(s) as a single string.}
#'   \item{Experiment_doi}{Experiment DOI(s) as a single string.}
#'   \item{Contact_persons}{Contact person(s), separated by "; ".}
#'   \item{Institutions}{Institution(s), separated by "; ".}
#'   \item{Contact_email}{Contact email(s), separated by "; ".}
#'   \item{Legal_constraints}{Legal constraints, separated by "; ".}
#'   \item{Funding}{Funding information, separated by "; ".}
#'   \item{Date_published}{Earliest date found in the metadata (as.Date).}
#'   \item{Date_revised}{Most recent date found in the metadata (as.Date).}
#'   \item{Coordinate_system}{Coordinate system as a string (e.g., "EPSG:4326 v8.9").}
#'   \item{Longitude}{Mean longitude (numeric) from west/east bounds.}
#'   \item{Latitude}{Mean latitude (numeric) from north/south bounds.}
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
#' meta <- read_metadata("path/to/xml")
#' print(meta)
#' }
#'
#' @importFrom xml2 read_xml xml_ns xml_text xml_find_all xml_find_first
#' @importFrom tibble tibble as_tibble
#'
#' @export
#' 

read_metadata <- function(mother_path, schema = "bonares") {
  
  # Helper to extract, filter, and collapse text
  extract_collapse <- function(doc, xpath, ns, collapse = TRUE) {
    vals <- xml2::xml_text(xml2::xml_find_all(doc, xpath, ns))
    vals <- vals[nzchar(vals)]
    if (collapse) {
      vals <- paste(vals, collapse = "; ")
    }
    vals
  }
  extract_first <- function(doc, xpath, ns) {
    val <- xml2::xml_text(xml2::xml_find_first(doc, xpath, ns))
    ifelse(nzchar(val), val, NA_character_)
  }
  
  # Read metadata file
  metadata <- xml2::read_xml(mother_path)
  # Get namespace mapping
  ns <- xml2::xml_ns(metadata)
  
  
  ##---- Spatial coverage ----
  coord_system <- paste0(
    extract_first(metadata, ".//gmd:referenceSystemIdentifier/gmd:RS_Identifier/gmd:codeSpace/gco:CharacterString", ns), ":",
    extract_first(metadata, ".//gmd:referenceSystemIdentifier/gmd:RS_Identifier/gmd:code/gco:CharacterString", ns), " v",
    extract_first(metadata, ".//gmd:referenceSystemIdentifier/gmd:RS_Identifier/gmd:version/gco:CharacterString", ns)
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
  persons <- extract_collapse(metadata, ".//gmd:contact/gmd:CI_ResponsibleParty/gmd:individualName/gco:CharacterString", ns, collapse = FALSE)
  institutions <- extract_collapse(metadata, ".//gmd:contact/gmd:CI_ResponsibleParty/gmd:organisationName/gco:CharacterString", ns)
  emails <- extract_collapse(metadata, ".//gmd:contact/gmd:CI_ResponsibleParty/gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:electronicMailAddress/gco:CharacterString",
                             ns, collapse = FALSE)
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
  var_key <- tryCatch({
    as_tibble(
      get_dataset_varkeys(mother_path, schema = "bonares")
    )
  }, error = function(e) {
    warning("Failed to retrieve variable keys. Server may be down. Original error: ", e$message)
    return(NULL)
  })
  
  
  ##---- Output ----
  exp_metadata <- tibble(
    Experiment_name = exp_name,
    Experiment_title = exp_title,
    Experiment_doi = exp_doi,
    Legal_constraints = legalConstraints,
    Funding = funding,
    Date_published = date_published,
    Date_revised = date_revised,
    Coordinate_system = coord_system,
    Longitude = mean(c(west_bound, east_bound), na.rm = TRUE),
    Latitude = mean(c(north_bound, south_bound), na.rm = TRUE)
  )
  persons <- tibble(
    Contact_persons = persons,
    Contact_email = emails
  )
  institutions <- tibble(
    Institutions = institutions
  )
  
  list(
    metadata = list(METADATA = exp_metadata,
                    PERSONS = persons,
                    INSTITUTIONS = institutions),
    variable_key = var_key
  )
}

