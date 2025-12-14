#'
#'
#' @noRd
#' 

export_output <- function(dataset, output_path) {
  
  if (is.null(output_path)) {
    return(dataset)
  }
  
  # Write output if not null
  if (is.character(output_path) && length(output_path) == 1) {
    file_extension <- tools::file_ext(output_path)
    if (tolower(file_extension) == "json") {
      tryCatch({
        write_json_dataset(dataset, file = output_path)
      }, error = function(e) {
        stop(paste("Failed to write JSON file from", dataset, "Error:", e$message))
      })
    } else {
      stop(paste("Output must be a JSON file:", output_path))
    }
  }
  
  return(invisible(dataset))
}


#' Writes a list of dataframes (dataset) as a unique JSON file,
#' preserving attributes and nested structures, recursively.
#'
#' @param dataset An R list where elements can be dataframes (potentially with custom attributes),
#'                other lists (potentially nested dataframes), or NULL.
#' @param file A character string specifying the path to the output JSON file.
#' @param pretty Logical. If TRUE (default), the JSON will be pretty-printed.
#' @param auto_unbox Logical. Passed to jsonlite::toJSON. If TRUE, single-element arrays are unboxed.
#'                   Set to TRUE to prevent attributes like single character strings from becoming arrays.
#' @param ... Additional arguments passed to jsonlite::write_json.
#'
#' @importFrom tibble tibble is_tibble
#' @importFrom jsonlite write_json
#'
#' @export
#'

# write_json_dataset <- function(dataset, file, pretty = TRUE, auto_unbox = TRUE, ...) {
#   
#   if (!is.list(dataset)) {
#     stop("Input 'dataset' must be an R list.")
#   }
#   
#   # Recursive helper to process individual R objects for JSON serialization
#   object_to_json <- function(obj) {
#     obj_attrs <- attributes(obj)
#     
#     # 1. Check if the object should be wrapped (Has relevant attributes OR is a DF)
#     should_wrap <- (is.data.frame(obj) || tibble::is_tibble(obj)) ||
#       (length(obj_attrs) > 0 &&
#          !all(names(obj_attrs) %in% c("names", "row.names", "class", "reference"))) ||
#       (is.atomic(obj) && !is.null(names(obj)) && !inherits(obj, "json"))
#     
#     if (should_wrap) {
#       
#       # --- Handle attributes ---
#       filtered_attrs_list <- list()
#       
#       # Handle class
#       if ("class" %in% names(obj_attrs)) {
#         filtered_attrs_list[["_class_attribute"]] <- obj_attrs[["class"]]
#       }
#       # Handle atomic names
#       if (is.atomic(obj) && !is.null(names(obj))) {
#         filtered_attrs_list[["_names_attribute"]] <- names(obj)
#       }
#       
#       # Handle custom attributes (recursive)
#       attrs_to_exclude <- c("names", "row.names", "class", "reference")
#       for (attr_name in setdiff(names(obj_attrs), attrs_to_exclude)) {
#         filtered_attrs_list[[attr_name]] <- object_to_json(obj_attrs[[attr_name]])
#       }
#       
#       # --- Handle data ---
#       if (is.data.frame(obj) || tibble::is_tibble(obj)) {
#         # Convert DF to list of columns, then process each column recursively
#         # This ensures column-level attributes are preserved
#         data_for_json <- lapply(as.list(obj), object_to_json)
#       } else if (is.list(obj)) {
#         # It's a list with attributes. Process its children recursively.
#         data_for_json <- lapply(obj, object_to_json)
#       } else {
#         # Atomic vector / other leaves
#         data_for_json <- obj
#       }
#       
#       return(list(
#         data = data_for_json,
#         attributes = if (length(filtered_attrs_list) > 0) filtered_attrs_list else list()
#       ))
#       
#     } else if (is.list(obj) && !inherits(obj, "json")) {
#       
#       # --- Standard list (no attributes) ---
#       lapply(obj, object_to_json)
#       
#     } else {
#       # --- Leaf nodes ---
#       obj
#     }
#   }
#   
#   # Start recursion
#   json_data <- object_to_json(dataset)
#   
#   # Write to file
#   jsonlite::write_json(json_data, path = file, pretty = pretty, auto_unbox = auto_unbox, ...)
# }

write_json_dataset <- function(dataset, file, pretty = TRUE, auto_unbox = TRUE, ...) {
  
  if (!is.list(dataset)) {
    stop("Input 'dataset' must be an R list.")
  }
  
  # Recursive helper to process individual R objects for JSON serialization
  object_to_json <- function(obj) {
    obj_attrs <- attributes(obj)
    
    # Check if the object should be wrapped
    should_wrap <- (is.data.frame(obj) || tibble::is_tibble(obj)) ||
      (length(obj_attrs) > 0 &&
         !all(names(obj_attrs) %in% c("names", "row.names", "class", "reference"))) ||
      (is.atomic(obj) && !is.null(names(obj)) && !inherits(obj, "json"))
    
    if (should_wrap) {
      
      # --- Handle attributes ---
      filtered_attrs_list <- list()
      
      # Handle class
      if ("class" %in% names(obj_attrs)) {
        filtered_attrs_list[["_class_attribute"]] <- obj_attrs[["class"]]
      }
      # Handle atomic names
      if (is.atomic(obj) && !is.null(names(obj))) {
        filtered_attrs_list[["_names_attribute"]] <- names(obj)
      }
      
      # Handle custom attributes (recursive)
      attrs_to_exclude <- c("names", "row.names", "class", "reference")
      for (attr_name in setdiff(names(obj_attrs), attrs_to_exclude)) {
        filtered_attrs_list[[attr_name]] <- object_to_json(obj_attrs[[attr_name]])
      }
      
      # --- Handle data ---
      if (is.data.frame(obj) || tibble::is_tibble(obj)) {
        # No recursion for data frames (no capture of column-level attributes)
        data_for_json <- as.list(obj)
      } else if (is.list(obj)) {
        # It's a list with attributes. Process its children recursively.
        data_for_json <- lapply(obj, object_to_json)
      } else {
        # Atomic vector / other leaves
        data_for_json <- obj
      }
      
      return(list(
        data = data_for_json,
        attributes = if (length(filtered_attrs_list) > 0) filtered_attrs_list else list()
      ))
      
    } else if (is.list(obj) && !inherits(obj, "json")) {
      
      # --- Standard list (no attributes) ---
      lapply(obj, object_to_json)
      
    } else {
      # --- Leaf nodes ---
      obj
    }
  }
  
  # Start recursion
  json_data <- object_to_json(dataset)
  
  # Write to file
  jsonlite::write_json(json_data, path = file, pretty = pretty, auto_unbox = auto_unbox, ...)
}

