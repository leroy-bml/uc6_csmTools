#' Load a Mapping Data Frame
#'
#' Loads the core mapping data frame from a specified JSON file.
#' If no path is provided, a default mapping file included with the package is used.
#'
#' @param path Character. Path to the mapping JSON file.
#' If \code{NULL}, the default mapping file in the package is used.
#'
#' @return A data frame containing the mapping information.
#'
#' @importFrom jsonlite read_json
#' @export
#' 

load_map <- function(path = NULL) {

  # Use default dataset from package unless a custom path is specified
  if (is.null(path)) {
    #path <- system.file("extdata", "icasa_mappings.csv", package = "csmTools")
    path <- "inst/extdata/icasa_mappings.json"  #tmp; switch after package built
  }
  
  # Load the data
  if (file.exists(path)) {
    map <- read_json(path, simplifyVector = TRUE)  # TO APPEND NEW, NO SIMPLIFY
  } else {
    stop("File not found: ", path)
  }

  return(map)
}


#' Create a Custom Mapping File
#'
#' Copies the core mapping file to a user directory for customization.
#'
#' @param dir Directory to create the custom mapping file in.
#'   Defaults to \code{~/.csmTools}.
#'
#' @return Path to the custom mapping JSON file.
#'

create_map <- function(dir = NULL) {
  
  # Define user data directory
  if(is.null(dir)) {
    dir <- file.path("~/.csmTools")
    message("Custom data map created in user's home directory, as 'dir' is not specified.")
    print(dir)
  }
  
  # Ensure directory exists
  if (!dir.exists(dir)) {
    dir.create(dir, recursive = TRUE)
  }
  cstm_path <- file.path(dir, "custom_icasa_mappings.json")
  #src_path <- system.file("extdata", "icasa_mappings.csv", package = "csmTools")
  src_path <- "inst/extdata/icasa_mappings.json"  #tmp; switch after package built
  
  # Define full file path for storing user-modified data
  if (!file.exists(cstm_path)) {
    file.copy(src_path, cstm_path)
  }
  return(cstm_path)
}


#'
#'
#'
#' @importFrom magrittr %>%
#' @importFrom dplyr select starts_with distinct bind_rows filter add_row
#' @importFrom jsonlite read_json toJSON
#'

make_mapping <- function(path = NULL,
                         standard,
                         section = NULL,
                         header,
                         code_mappings = list(),
                         unit,
                         description = NULL,
                         section_icasa,
                         header_icasa,
                         fun_headers = NULL,
                         fun_headers_args = list(),
                         fun_values = NULL,
                         fun_values_args = list(),
                         write = TRUE) {
  
  
  path = NULL
  standard = "ogcAgrovoc"
  section = NULL
  header = "air_temperature"
  code_mappings = list()
  unit = "degrees_C"
  description = NULL
  section_icasa = "WEATHER_DAILY"
  header_icasa = "TMAX"
  fun_headers = NULL
  fun_headers_args = list()
  fun_values = funtmp
  fun_values_args = list("W_DATE")
  write = TRUE
  
  # Identify the ICASA structure of the mapped ICASA term
  icasa_str <- load_map() %>%
    select(template_section, starts_with("icasa_")) %>%
    distinct()
  icasa_str_rec <- icasa_str[which(icasa_str$template_section == section_icasa & icasa_str$icasa_header_short == header_icasa),]
  
  # Create the new record dynamically
  new_record <- list(
    data_model = standard,
    section = section,
    header = header,
    description = description,
    unit = unit,
    code_mappings = code_mappings,
    fun_header = if (!is.null(fun_headers)) list(paste(deparse(fun_headers), collapse = "\n")) else NULL,
    fun_header_args = fun_headers_args,
    fun_values = if (!is.null(fun_values)) list(paste(deparse(fun_values), collapse = "\n")) else NULL,
    fun_values_args = fun_values_args
  )
  new_record <- append(icasa_str_rec, new_record)
  
  # Reorder as map
  map_str <- names(load_map())
  map_str <- map_str[map_str %in% names(new_record)]
  new_record <- new_record[match(map_str, names(new_record))]
  new_record <- new_record[lapply(new_record, length) != 0]
  
  # Define user data directory
  is_file <- function(path) {
    # Checks if path contains a file extension
    return(grepl("\\.[a-zA-Z0-9]+$", basename(path)))
  }
  if (!is.null(path)) {
    if (is_file(path)) path <- dirname(path) else path <- path
  }
  path <- create_map(path)
  
  # Append to custom map
  cst_map <- load_map(path)

  ###------ FIGURE OUT ROUTINE, CLEANUP --------
  
  # Convert list to a dataframe while flattening any nested elements
  new_row <- as.data.frame(lapply(new_record, function(x) if (is.list(x)) unlist(x) else x), stringsAsFactors = FALSE)
  
  # Ensure all missing columns are added as NA
  missing_cols <- setdiff(names(cst_map), names(new_row))
  new_row[missing_cols] <- NA
  
  # Append the row to cst_map
  cst_map <- bind_rows(cst_map, new_row)
  write_json(cst_map, "test.json", pretty = TRUE, auto_unbox = TRUE)
  
  
  # Problem parsing
  tmp <- cst_map %>% filter(!is.na(fun_values))
  str2 <- tmp$fun_values[13]
  eval(parse(text = unlist(str2)))
  
  ###-----------------------------
  
  
  # Write into the custom map
  if (write) {
    create_map(dir = path)
    write_json(cst_map, path, pretty = TRUE, auto_unbox = TRUE)
    message(paste("Mapping written in user custom map at", path))
  } 
  

  
  
  map_tmp3 <- read_json("submap2.json", simplifyVector = TRUE)
  
  
  

  map_tmp4 <- read.csv("submap.csv")
  
  map_tmp <- rbind(map_tmp, as.data.frame(new_record, stringsAsFactors = FALSE))
  
  
  map_tmp1 <- map_tmp %>% add_row(new_record)

  fun_headers_str <- paste0("[\"", paste(deparse(fun), collapse = " "), "\"]")
  
  # Manually fix spacing and formatting issues
  fun_headers_str <- gsub("^function ", "function(", fun_headers_str)  # Ensure consistent function header format
  fun_headers_str <- gsub("\\s+", " ", fun_headers_str)  # Remove extra spaces
  fun_headers_str <- paste(fun_headers_str, collapse = " ")  # Collapse into one line
  
  # Format as final string
  final_string <- paste0("[\"", fun_headers_str, "\"]")
  
  
  
  tmp <- "function(x, df, grp, threshold) { df <- aggregate(df[[x]] ~ df[[grp]], data = df, FUN = function(x) max(x, na.rm = TRUE))\n return(df) \n }"
  eval(parse(text = tmp))

  submap <- map %>% filter(icasa_header_short == "TMAX" & header == "air_temperature")
  jsonmap <- toJSON(submap, pretty = TRUE)
  write(jsonmap, "submap.json")

  submap$fun_values <- sapply(submap$fun_values, toString)
  submap$fun_values_args <- sapply(submap$fun_values_args, toString)
  write.csv(submap, "submap.csv", row.names = FALSE)

  submap2 <- read.csv("submap.csv")
  
  
  submap2 <- read_json("submap.json", simplifyVector = TRUE)
  tmp <- eval(parse(text = unlist(submap2$fun_values)))
  
}


#' Convert a Mapping Code List String to a Lookup Table
#'
#' Converts a code mapping list, stored as a string, into a lookup table.
#' The string format is analogous to the \code{code_mapping} field in standard
#' ARDN SC2 JSON files. See:
#' \url{https://agmip.github.io/ARDN/Annotation_SC2.html}
#'
#' @param vec A code mapping list stored as a string.
#'   Format: \code{list('source1: target1', 'source2: target2')}
#'
#' @return A data frame with two columns:
#'   \describe{
#'     \item{source}{Original data values}
#'     \item{target}{Standardized data values}
#'   }
#'
#' @importFrom rlang eval_tidy parse_exprs
#' 

make_code_lookup <- function(vec){
  
  # Ensure vec contains exactly one expression
  if (vec == "1" | length(parse_exprs(vec)) != 1) {
    lkp <- data.frame(source = NA_character_, target = NA_character_)
  } else {
    
    ls <- eval_tidy(parse_expr(vec))
    ls <- strsplit(as.character(ls), ": ")
    ls <- lapply(ls, function(x) list(source = x[1], target = x[2]))
    
    ls_lkp <- lapply(ls, function(lst) {
      if (grepl("c\\(", lst$source)) {
        df <- data.frame(source = strsplit(gsub("[c()]", "", lst$source), ", ")[[1]],
                         target = lst$target)
        return(df)
      } else {
        df <- as.data.frame(do.call(cbind, lst))
        return(df)
      }
      return(ls)
    })
    
    lkp <- as.data.frame(do.call(rbind, ls_lkp))
    
    lkp$target <- ifelse(lkp$target == 1, lkp$source, lkp$target)
  }

  return(lkp)
}


#' Map Column Headers to Standard Codes
#'
#' Maps the column headers of a data frame to standard codes using a lookup table.
#'
#' @param df A data frame containing the data to be mapped.
#' @param map A lookup table of categories to be mapped to standard codes,
#'   as generated by \code{make_code_lookup}.
#' @param direction Character. Direction of the mapping:
#'   \code{"to_icasa"} (to standard ICASA codes) or
#'   \code{"from_icasa"} (from standard ICASA codes).
#'
#' @return A list with two elements:
#'   \describe{
#'     \item{data}{A data frame with headers mapped to the annotation format specified in \code{map_data}.}
#'     \item{mapped}{A character vector of columns that were mapped.}
#'   }
#'
#' @importFrom magrittr %>%
#' @importFrom dplyr rename
#'

map_headers <- function(df, map, direction = c("to_icasa", "from_icasa"), header = c("short","long")) {
  
  mapped_cols <- c()  # empty vector to store mapped column names = column in both data models, whether same-named of different
  
  # Determine which data model is the input/output
  if (direction == "to_icasa"){
    if (header == "short") {
      map <- map %>% rename(header_in = header, header_out = icasa_header_short)
    }
    map <- map %>% rename(header_in = header, header_out = icasa_header_long)
  } else if (direction == "from_icasa"){
    if (header == "short") {
      map <- map %>% rename(header_in = icasa_header_short, header_out = header)
    }
    map <- map %>% rename(header_in = icasa_header_long, header_out = header)
  }
  
  for (i in seq_along(colnames(df))) {
    
    matches <- which(map$header_in == colnames(df)[i])  # get index of matches
    
    if (length(matches) > 0) {
      for (j in seq_along(matches)) {
        
        match_index <- matches[j]
        if (is.na(map$header_in[match_index]) | map$header_out[match_index] == "") {
          next
        }
        
        # Only map headers if they are not identical in input and output models (i.e., as in some cases in ICASA-DSSAT)
        if (map$header_in[match_index] != map$header_out[match_index]){
          mapped_cols <- c(mapped_cols, colnames(df)[i])
          new_col_nm <- map$header_out[match_index]
          df[[new_col_nm]] <- df[[colnames(df)[i]]]  # add mapped column as a new column
        } else {
          mapped_cols <- c(mapped_cols, colnames(df)[i])
          new_col_nm <- paste0(map$header_out[match_index], ".2")
          df[[new_col_nm]] <- df[[colnames(df)[i]]]  # duplicate columns to keep original order
        }
      }
    }
  }
  
  df <- df[ , !(colnames(df) %in% mapped_cols)]  # remove the original columns after mapping
  colnames(df) <- gsub("\\.2", "", colnames(df))  # remove temporary suffixes used to preserve column order
  
  out <- list(data = df, mapped = mapped_cols)
  
  return(out)
} 


#' Map Categorical Data to Standard Codes
#'
#' Maps categorical variables in a data frame to standard codes using a lookup table.
#' Currently supports lookup tables as data frames; support for JSON SC2 files may be added in the future.
#'
#' @param df A data frame containing the data to be mapped.
#' @param map A lookup table of categories to be mapped to standard codes,
#'   as generated by \code{make_code_lookup}.
#' @param direction Character. Direction of the mapping:
#'   \code{"to_icasa"} (to standard ICASA codes) or
#'   \code{"from_icasa"} (from standard ICASA codes).
#' @param ... Additional arguments (currently unused).
#'
#' @return A data frame with codes mapped to the format specified in the supplied map.
#'
#' @importFrom magrittr %>%
#' @importFrom dplyr rename recode_factor
#' @importFrom rlang !!!
#' 

# TODO: revise: mapping direction + json strings
map_codes <- function(df, map, direction, ...){

  if (direction == "to_icasa"){ # !!run only after mapping headers!
    map <- map %>% rename(header_in = icasa_header_short, header_out = header, unit_in = unit, unit_out = icasa_unit)  
  } else if (direction == "from_icasa"){
    map <- map %>% rename(header_in = header, header_out = icasa_header_short, unit_in = icasa_unit, unit_out = unit)
  }
  
  if (nrow(map) == 0) { # end if map is empty
    return(df)
  }
  
  for (i in 1:nrow(map)){
    
    if (is.na(map$unit_in[i])) {
      next
    }

    if (map$unit_in[i] == "code") {

      header <- map$header_in[i]
      
      mappings <- map$code_mappings[i]
      lookup <- make_code_lookup(mappings)  # TODO: revise using json strings instead of R list()
      var <- df[[header]]
      
      if (all(is.na(lookup))) {
        next
      } else {
        df[[header]] <- recode_factor(var, !!!setNames(as.list(lookup$target), lookup$source), .default = NA_character_)
      }
    }
  }
  return(df)
}


#' Convert Numeric Vector Between Units
#'
#' Converts a numeric vector from one unit to another, using unit strings.
#'
#' @param x Numeric vector to be converted.
#' @param u1 Input unit (as a string) in which \code{x} is expressed.
#' @param u2 Target unit (as a string) for the output.
#'
#' @return Numeric vector converted to the target unit.
#'
#' @importFrom magrittr %>%
#' @importFrom units set_units
#'

convert_unit <- function(x, u1, u2){
  set_units(x, u1, mode = "standard") %>% set_units(u2, mode = "standard") %>% as.numeric()
}


#' Convert Units in a Data Frame
#'
#' Converts numeric columns in a data frame to target units based on mapping information
#' or metadata attributes.
#'
#' @param df A data frame with columns to be converted.
#' @param metadata Optional. Metadata containing unit information for columns
#'   where units are not set in the map.
#' @param map A data frame specifying input and output units for each variable.
#' @param direction Character. Direction of the mapping:
#'   \code{"to_icasa"} (to standard ICASA units) or
#'   \code{"from_icasa"} (from standard ICASA units).
#'
#' @return A data frame with numeric columns converted to the target units.
#'
#' @importFrom magrittr %>%
#' @importFrom dplyr rename
#'

convert_units <- function(df, metadata = NULL, map, direction) {

  if (direction == "to_icasa"){ # !!run only after mapping headers!
    map <- map %>% rename(header_in = icasa_header_short, header_out = header, unit_in = unit, unit_out = icasa_unit)  
  } else if (direction == "from_icasa"){
    map <- map %>% rename(header_in = header, header_out = icasa_header_short, unit_in = icasa_unit, unit_out = unit)
  }
  
  # Convert units by checking variable sequentially
  # TODO: check error when colname is not in input data model (may happen in 2-step mapping, e.g., bonares->icasa->dssat)
  for (i in seq_along(colnames(df))) {
    print(colnames(df)[i])
    if(nrow(map) > 0){
      
      for (j in 1:nrow(map)) {
        
        if (map$unit_in[j] == "not_set") {
          if (!is.null(metadata)) {
            map$unit_in[j] <- metadata$unitOfMeasurement_symbol  # retrieve unit in metadata if "not_set" (for sensors)
          } else {
            map$unit_in[j] <- ""
          }
        }
        
        if (colnames(df)[i] == map$header_in[j]){
          if (is.na(map$header_in[j]) | map$unit_out[j] %in% c("","date","code","text","yyyy")) {
            next
          }
          df[[i]] <- convert_unit(df[[i]], map$unit_in[j], map$unit_out[j])
        }
      }
    }
  }
  return(df)
}


#' Apply a Wrangling Function to a Data Frame
#'
#' Applies a user-supplied function (as a character string) to a data frame,
#' with specified variable and additional arguments.
#'
#' @param var Character. The target variable for the function (\code{x}).
#' @param fun Character string containing the function definition.
#' @param args List of additional arguments for the function.
#' @param df Data frame to which the function will be applied.
#'
#' @return A data frame with the function applied.
#'

apply_function <- function(var, fun, args, df) {
  
  fun_parsed <- eval(parse(text = fun))  # parse function
  args_parsed <- if (all(sapply(args, is.null)) | length(args) == 0) { # format argument as simple list
    list()
  } else {
    unlist(args, recursive = FALSE)
  }

  cols <- colnames(df[colnames(df) %in% c(var, args_parsed)])  # save column names for output

  if(is.null(fun_parsed)){
    fun_parsed <- eval(parse(text = "function(x, ...){as.data.frame(identity(x))}"))
  } 
  out <- do.call(fun_parsed, c(list(x = var), list(df = df), args_parsed))
  colnames(out) <- cols          
  
  return(out)
}


#' Apply Wrangling Functions from a Map to a Data Frame
#'
#' Applies a set of wrangling functions, as specified in a mapping data frame,
#' to the columns of a data frame.
#'
#' @param df A data frame to be transformed.
#' @param map A data frame containing the mapping information and functions
#'   (full map available with \code{load_map()}).
#' @param ... Additional arguments (currently unused).
#'
#' @return A data frame with transformations applied as specified in the map.
#'

# TODO: fun_headers, complete missing funs in the map
transform_data <- function(df, map, ...) {
  
  if (nrow(map) == 0 || length(map$fun_values) == 0) {
    return(df)  # If there are no rows, return df immediately
  }
  
  if(all(is.na(map$fun_values) |  ##TODO: check if all conditions required
         sapply(map$fun_values, is.null) | 
         map$fun_values == ""
         )
     ){
    return(df)
  }
  
  # Identify argument and transform argument variables
  arg_vars <- map[map$icasa_header_short %in% unlist(map$fun_values_args),]$icasa_header_short
  df_grp <- apply_function(arg_vars, map[map$icasa_header_short%in%arg_vars,]$fun_values, map[map$icasa_header_short%in%arg_vars,]$fun_values_args, df)
  colnames(df_grp) <- colnames(df)
  
  # Transform variables based on updated argument variables
  map <- map[!map$icasa_header_short %in% arg_vars,]
  out_ls <- lapply(1:nrow(map), function(i) {
    apply_function(map$icasa_header_short[i], map$fun_values[i], map$fun_values_args[i], df_grp)
  })
  
  # Merge output into a dataframe
  if(length(out_ls)>1){
    cmn_cols <- Reduce(intersect, lapply(out_ls, colnames))
    out <- unique(
      Reduce(function(x, y) merge(x, y, by = cmn_cols, all = TRUE), out_ls)  # aggregated data
    )
  } else {
    out <- do.call(cbind, out_ls)  # non-aggregative transformed data
  }
  
  return(out)
}


#' Map a Data Table to a Standard Format
#'
#' Maps a data frame's headers, units, and codes to a standard format using a mapping table.
#' Currently supports only exact matches for header names, units, and codes.
#'
#' @param df A data frame to be mapped.
#' @param input_model Character. Name of the input data model.
#' @param output_model Character. Name of the output (target) data model.
#' @param map A lookup table detailing input and target headers, units, and codes.
#' @param keep_unmapped Logical. Whether to keep unmapped variables in the output.
#'   Defaults to \code{TRUE}.
#' @param col_exempt Character vector of column names to keep in the output if
#'   \code{keep_unmapped = FALSE}.
#'
#' @return A data frame with headers, units, and codes mapped to the format specified in the map.
#'
#' @importFrom dplyr distinct
#' 
#' @export
#' 

map_data <- function(df, input_model, output_model, header,
                     map, keep_unmapped = TRUE, col_exempt = NULL){
  
  # TODO: workaround to skip dataframes with no single match in the output model [currently fails]
  
  # for (i in seq_along(template_icasa)) {
  #   df <- template_icasa[[i]]
  # 
  #   data_mapped <- mapping_sequence(df, map)  # map data table
  #   if(!is.null(metadata)){
  #     attr(data_mapped, "metadata") <- mapping_sequence(metadata, map)  # map metadata table
  #   }
  #   
  #   print(paste(i, names(template_icasa)[i]))
  # }
  
  # df = tmp$AUSSAAT
  # input_model = "bonares-lte_de"
  # output_model = "icasa"
  # map = load_map()
  # keep_unmapped = FALSE
  # col_exempt = NULL
  # header = "long"

  
  metadata <- attributes(df)$metadata  # store metadata

  # Perform the mapping sequence (i.e., headers -> codes -> units -> transformations)
  mapping_sequence <- function(df, map, ...){
    # Perform the mapping
    if ("icasa" %in% output_model){
      df0 <- df  # store original data
      map <- map[map$data_model == input_model & map$header %in% colnames(df),]
      headers <- map_headers(df, map, "to_icasa", header = header)  # map headers
      df <- headers$data
      #df <- fuse_duplicate_columns(df, sep = "-")
      mapped_cols <- unique(headers$mapped)
      #df <- map_codes(df, map, "to_icasa")  # map codes
      #df <- convert_units(df, metadata, map, "to_icasa")  # convert units
      #df <- transform_data(df, map)  # apply transformations
    } else if ("icasa" %in% input_model){
      df0 <- df  # store original data
      map <- map[map$data_model == output_model & map$icasa_header_short %in% colnames(df),]
      headers <- map_headers(df, map, "from_icasa", header = header)  # map headers
      df <- headers$data
      mapped_cols <- unique(headers$mapped)
      df <- map_codes(df, map, "from_icasa")
      df <- convert_units(df, metadata, map, "from_icasa", header = header)  # convert units
    } else {
      df0 <- df  # store original data
      map_icasa <- map[map$data_model == input_model & map$header %in% colnames(df),]
      map <- map[map$data_model == output_model & map$icasa_header_short %in% colnames(df),]
      headers <- map_headers(df, map, "to_icasa", header = header)
      df_icasa <- headers$data
      mapped_cols <- unique(headers$mapped)
      df_icasa <- map_codes(df_icasa, map_icasa, "to_icasa")  # map codes
      df_icasa <- convert_units(df_icasa, metadata = NULL, map_icasa, "to_icasa")  # convert units  #! METADATA ARG? TOTEST
      df_icasa <- transform_data(df_icasa, map_icasa)  # apply transformations
      ### TODO: test
      df <- map_headers(df_icasa, map, "from_icasa")
      df <- headers$data
      df <- map_codes(df, map, "from_icasa")  # map codes
      df <- convert_units(df, metadata, map, "from_icasa")  # convert units
    }
    # Store unmapped variables
    unmapped_cols <- setdiff(colnames(df0), mapped_cols)  # check main branch
    # Drop columns not in standard if required
    if (keep_unmapped) {
      out <- distinct(df[, !colnames(df) %in% col_exempt])
    } else {
      out <- distinct(df[, !colnames(df) %in% setdiff(unmapped_cols, col_exempt), drop = FALSE])
    }
    return(out)
  }
  
  data_mapped <- mapping_sequence(df, map)  # map data table
  if(!is.null(metadata)){
    attr(data_mapped, "metadata") <- mapping_sequence(metadata, map)  # map metadata table
  }
  
  return(data_mapped)
}

