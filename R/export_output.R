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

# write_json_dataset <- function(dataset, file, pretty = TRUE, auto_unbox = TRUE, ...) {
#   
#   if (!is.list(dataset)) {
#     stop("Input 'dataset' must be an R list.")
#   }
#   if (!is.character(file) || length(file) != 1) {
#     stop("Input 'file' must be a single character string.")
#   }
#   
#   # Recursive helper to process individual R objects for JSON serialization
#   object_to_json <- function(obj) {
#     obj_attrs <- attributes(obj)
#     
#     # Determine if the object should be wrapped
#     should_wrap <- (is.data.frame(obj) || tibble::is_tibble(obj)) ||
#       (length(obj_attrs) > 0 &&
#          # Has non-standard R attributes, excluding "names", "row.names", "class"
#          !all(names(obj_attrs) %in% c("names", "row.names", "class", "reference"))) || # Added "reference" for common cases
#       # Named atomic vector
#       (is.atomic(obj) && !is.null(names(obj)) && !inherits(obj, "json"))
#     # `!inherits(obj, "json")` prevents re-wrapping objects already serialized by jsonlite
#     
#     if (should_wrap) {
#       filtered_attrs_list <- list()
#       
#       # Special handling for `class` attribute
#       if ("class" %in% names(obj_attrs)) {
#         filtered_attrs_list[["_class_attribute"]] <- obj_attrs[["class"]]
#       }
#       # Special handling for `names` attribute of atomic vectors.
#       if (is.atomic(obj) && !is.null(names(obj))) {
#         filtered_attrs_list[["_names_attribute"]] <- names(obj)
#       }
#       
#       # Iterate over other attributes for recursive processing and filtering
#       attrs_to_exclude_from_direct_serialization <- c("names", "row.names", "class", "reference") # Added "reference"
#       for (attr_name in setdiff(names(obj_attrs), attrs_to_exclude_from_direct_serialization)) {
#         filtered_attrs_list[[attr_name]] <- object_to_json(obj_attrs[[attr_name]])
#       }
#       
#       # Prepare data part: For dataframes/tibbles, convert to a simple list of columns.
#       # For other objects, the object itself is the data.
#       data_for_json <- obj
#       if (is.data.frame(obj) || tibble::is_tibble(obj)) {
#         # Convert data.frame/tibble to a list where each element is a column.
#         # This prevents jsonlite from simplifying it directly into a complex JSON structure
#         # that might lose attribute-like information.
#         data_for_json <- as.list(obj) # Convert to list of columns
#       }
#       
#       return(list(
#         data = data_for_json,
#         attributes = if (length(filtered_attrs_list) > 0) filtered_attrs_list else list()
#       ))
#     } else if (is.list(obj) && !inherits(obj, "json")) {
#       # Generic R list (recursive)
#       lapply(obj, object_to_json)
#     } else {
#       # Atomic objects, or objects already marked as 'json'
#       # jsonlite::toJSON will handle these directly.
#       obj
#     }
#   }
#   
#   # Apply the recursive processing to the entire dataset
#   json_data <- lapply(dataset, object_to_json)
#   
#   # Write the processed list to a JSON file
#   jsonlite::write_json(json_data, path = file, pretty = pretty, auto_unbox = auto_unbox, ...)
# }

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

write_json_dataset <- function(dataset, file, pretty = TRUE, auto_unbox = TRUE, ...) {
  
  if (!is.list(dataset)) {
    stop("Input 'dataset' must be an R list.")
  }
  
  # Recursive helper to process individual R objects for JSON serialization
  object_to_json <- function(obj) {
    obj_attrs <- attributes(obj)
    
    # 1. Check if the object should be wrapped (Has relevant attributes OR is a DF)
    should_wrap <- (is.data.frame(obj) || tibble::is_tibble(obj)) ||
      (length(obj_attrs) > 0 &&
         !all(names(obj_attrs) %in% c("names", "row.names", "class", "reference"))) ||
      (is.atomic(obj) && !is.null(names(obj)) && !inherits(obj, "json"))
    
    if (should_wrap) {
      # --- A. HANDLE ATTRIBUTES ---
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
      
      # --- B. HANDLE DATA (THE FIX) ---
      # Even if we wrap this level, we must RECURSE into the data content
      # so that children also get their attributes preserved.
      
      if (is.data.frame(obj) || tibble::is_tibble(obj)) {
        # Convert DF to list of columns, then process each column recursively
        # This ensures column-level attributes are preserved
        data_for_json <- lapply(as.list(obj), object_to_json)
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
      # --- C. STANDARD LIST (NO ATTRIBUTES) ---
      # No wrapper needed for this level, but recurse deeper
      lapply(obj, object_to_json)
      
    } else {
      # --- D. LEAF NODES ---
      obj
    }
  }
  
  # Start recursion
  json_data <- object_to_json(dataset)
  
  # Write to file
  jsonlite::write_json(json_data, path = file, pretty = pretty, auto_unbox = auto_unbox, ...)
}
