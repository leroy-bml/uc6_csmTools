#'
#'
#' @noRd
#'

resolve_input <- function(x) {
  
  resolved_data <- NULL
  
  if (is.character(x) && length(x) == 1) {
    if (file.exists(x)) {
      file_extension <- tools::file_ext(x)
      if (tolower(file_extension) == "json") {
        tryCatch({
          resolved_data <- read_json_dataset(file = x)
        }, error = function(e) {
          stop(paste("Failed to load JSON file from", x, "Error:", e$message))
        })
      }
      else if (tolower(file_extension) == "csv") {
        resolved_data <- read.csv(file = x)
      } else {
        stop(paste("Input format not handled", x))
      }
    } else {
      stop(paste("Input file not found:", x))
    }
  }

  else if (!is.data.frame(x) && is.list(x)) {
    
    # Preserve custom attributes
    attr <- lapply(x, function(obj) {
      attributes(obj)[!names(attributes(obj)) %in% c("names", "row.names", "class")]
    })
    
    # Unwrap nested structure (DSSAT write-ready formats)
    nested_cols <- apply_recursive(x, identify_nested_cols)
  
    
    .unnest_recursive <- function(data_node, names_node) {
      
      # CASE 1: We reached a DataFrame (The "Leaf")
      if (is.data.frame(data_node)) {
        # Check if there are columns to unnest for this specific dataframe
        if (length(names_node) > 0) {
          # Use all_of() to ensure safety if names_node is a character vector
          data_unnest <- tidyr::unnest(data_node, cols = dplyr::all_of(names_node))
          return(data_unnest)
        } else {
          return(data_node)
        }
      }
      
      # CASE 2: We are at a List (The "Branch")
      if (is.list(data_node)) {
        # Call this function again on the children
        # This walks down both lists simultaneously
        return(purrr::map2(data_node, names_node, .unnest_recursive))
      }
      
      # Fallback (e.g. if data_node is just a loose number or NULL)
      return(data_node)
    }
    
    # --- Usage ---
    x <- .unnest_recursive(x, nested_cols)
    # x <- purrr::map2(x, nested_cols, ~ {
    #   if (length(.y) > 0) {
    #     tidyr::unnest(.x, cols = .y)
    #   } else {
    #     .x
    #   }
    # })
    
    # Restore attributes
    x <- purrr::map2(x, attr, ~{
      attributes(.x) <- c(attributes(.x), .y)
      return(.x)
    })
    
    resolved_data <- x
  }
  else {
    stop("Unsupported input type. Input must be a character string (JSON filepath) or an R list.")
  }
  
  return(resolved_data)
}

#'
#'
#' @noRd
#' 

# Helper function to remove specific attributes
remove_named_attributes <- function(obj, names_to_remove) {
  if (!is.null(attributes(obj))) {
    attrs <- attributes(obj)
    attrs <- attrs[!names(attrs) %in% names_to_remove]
    attributes(obj) <- attrs
  }
  return(obj)
}


#'
#'
#' @noRd
#'

.identify_nested_vector_cols <- function(json_obj) {
  # Recursive helper to traverse the object
  find_nested <- function(x, parent_path = "") {
    if (is.null(x) || (is.atomic(x) && length(x) <= 1)) {
      return(NULL)
    }
    
    # Check if this is a nested vector (matrix/array with dim > 1)
    if ((is.matrix(x) || is.array(x)) && length(dim(x)) > 1 && dim(x)[1] == 1) {
      return(tail(strsplit(parent_path, "\\$")[[1]], 1))  # Extract last part of path
    }
    
    # Recursively check list/environment children
    if (is.list(x) || is.environment(x)) {
      results <- c()
      for (nm in names(x)) {
        child_path <- if (parent_path == "") nm else paste0(parent_path, "$", nm)
        child_result <- find_nested(x[[nm]], child_path)
        if (!is.null(child_result)) {
          results <- c(results, child_result)
        }
      }
      return(results)
    }
    
    return(NULL)
  }
  
  # Flatten results and extract unique column names
  nested_names <- find_nested(json_obj)
  if (is.null(nested_names)) {
    return(character(0))  # Return empty vector if nothing found
  } else {
    return(unique(unlist(nested_names)))
  }
}


#' Reads a dataset from a JSON file created by `write_json_dataset`,
#' reconstructing dataframes, tibbles, and preserving attributes recursively.
#'
#' @param file A character string specifying the path to the input JSON file.
#'
#' @importFrom tibble tibble as_tibble
#' @importFrom jsonlite fromJSON
#'
#' @export
#'

read_json_dataset <- function(file_path) {
  # Read the entire JSON file as a nested list.
  # simplifyVector = TRUE: Tries to simplify JSON arrays into R atomic vectors.
  # simplifyDataFrame = FALSE: Prevents jsonlite from creating data.frames immediately,
  #                            allowing more control over structure.
  # flatten = FALSE: Maintains the nested structure.
  raw_json_data <- jsonlite::fromJSON(file_path, simplifyVector = TRUE, simplifyDataFrame = FALSE, flatten = FALSE)
  
  # Identify nested vector columns in the whole dataset for downstream wrapping
  nested_cols <- .identify_nested_vector_cols(raw_json_data)
  
  # Recursive function to reconstruct R objects from the parsed JSON
  object_from_json <- function(json_obj) {
    if (is.list(json_obj) && "data" %in% names(json_obj) && "attributes" %in% names(json_obj)) {
      # This block handles data frames / tibbles based on the "data" and "attributes" convention
      
      data_part <- json_obj$data
      attrs_part <- json_obj$attributes
      
      class_attr <- NULL
      reconstructed_attrs <- list()
      
      # Attempt to extract original R class attribute
      if (is.character(attrs_part) && length(attrs_part) > 0 &&
          any(c("data.frame", "tbl_df", "tibble") %in% attrs_part)) {
        class_attr <- attrs_part
      } else if (is.list(attrs_part) && "_class_attribute" %in% names(attrs_part)) {
        class_attr <- attrs_part[["_class_attribute"]]
        attrs_part[["_class_attribute"]] <- NULL # Remove handled attribute
      }
      
      # Handle other attributes (if any, like names or custom ones)
      if (is.list(attrs_part)) {
        # Recursively reconstruct other custom attributes
        reconstructed_attrs <- lapply(attrs_part, object_from_json)
      }
      
      # Process data_part (the actual column data)
      if (!is.null(class_attr) && any(c("data.frame", "tbl_df", "tibble") %in% class_attr)) {
        if (!is.null(data_part)) {
          processed_columns <- lapply(names(data_part), function(col_name) {
            col <- data_part[[col_name]]
            
            # Convert JSON 'null' values to NA_character_ (Note: potentially problematic!)
            if (is.null(col)) {
              return(NA_character_)
            }
            
            # Conditionally wrap atomic vectors into a list to create list-columns
            if (is.matrix(col) && nrow(col) == 1 && is.atomic(col)) {
              # Flatten 1-row matrices (e.g., chr [1,1:2]) to vectors (e.g., chr [1:2])
              col <- as.vector(col)
            }
            if (col_name %in% nested_cols && is.atomic(col) && !is.list(col)) {
              # Ensure we only wrap non-empty atomic vectors designated as list-columns
              if (length(col) > 0 || !is.null(col)) { # Ensure it's not trying to wrap a truly empty NULL
                return(list(col))
              }
            }
            
            return(col)
          })
          names(processed_columns) <- names(data_part) # Re-apply original column names
          
          # Convert to tibble
          reconstructed_obj <- tibble::as_tibble(processed_columns)
          
        } else {
          # Empty tibble if data is NULL
          reconstructed_obj <- tibble::tibble()
        }
        # Apply the original classes
        class(reconstructed_obj) <- class_attr
      } else {
        # If it's not a data.frame/tibble structure, just return the data_part,
        # after recursively processing it if it's a list
        reconstructed_obj <- lapply(data_part, object_from_json)
        # Apply generic attributes if available
        if (!is.null(attrs_part)) {
          current_attrs <- attributes(reconstructed_obj)
          attributes(reconstructed_obj) <- c(current_attrs, reconstructed_attrs)
        }
      }
      
      # Apply any remaining custom attributes that were recursively reconstructed
      if (length(reconstructed_attrs) > 0) {
        current_attrs <- attributes(reconstructed_obj)
        # Filter out NULL attributes that might have resulted from stripping _class_attribute etc.
        reconstructed_attrs <- reconstructed_attrs[!sapply(reconstructed_attrs, is.null)]
        if(length(reconstructed_attrs) > 0) {
          attributes(reconstructed_obj) <- c(current_attrs, reconstructed_attrs)
        }
      }
      
      return(reconstructed_obj)
      
    } else if (is.list(json_obj) && !inherits(json_obj, c("data.frame", "tbl_df", "tibble", "json"))) {
      # Generic R list (recursive processing for its elements)
      lapply(json_obj, object_from_json)
    } else {
      # Atomic objects, unwrapped data frames (if jsonlite simplified directly), or already processed JSON.
      return(json_obj)
    }
  }
  
  # Apply the recursive reconstruction to the parsed JSON data
  dataset_dssat <- lapply(raw_json_data, object_from_json)
  
  return(dataset_dssat)
}



# read_json_dataset <- function(file, ...) {
#   
#   if (!file.exists(file)) {
#     stop(paste("File not found:", file))
#   }
#   
#   # Parse the entire JSON file.
#   # Use simplifyVector = TRUE to get atomic vectors for simple arrays where possible,
#   # but rely on custom logic to wrap them for list-columns if needed.
#   raw_json_data <- jsonlite::fromJSON(
#     file,
#     simplifyVector = TRUE,   # Convert JSON arrays of primitives (e.g., `["a","b"]`) to R vectors (`c("a","b")`).
#     simplifyDataFrame = TRUE # Convert arrays of JSON objects to R data frames.
#     , ...
#   )
#   
#   # Recursive helper function to reconstruct the R object from the parsed JSON structure
#   object_from_json <- function(json_obj) {
#     # Check if the object is a wrapped data.frame/tibble or an object with attributes
#     if (is.list(json_obj) && !is.null(names(json_obj)) && "data" %in% names(json_obj) && "attributes" %in% names(json_obj)) {
#       
#       data_part <- json_obj$data
#       attrs_payload <- json_obj$attributes
#       
#       class_attr <- NULL
#       names_attr <- NULL
#       reconstructed_attrs <- list()
#       
#       # Process attrs_payload for class and custom attributes
#       if (is.atomic(attrs_payload) && is.character(attrs_payload)) {
#         # If attributes is a direct character vector (from JSON array like ["tbl_df", "tbl"]),
#         # it's assumed to be the class attribute.
#         class_attr <- attrs_payload
#       } else if (is.list(attrs_payload) && !is.null(names(attrs_payload))) {
#         # If attributes is a named list (e.g., contains "_class_attribute" or other custom attributes)
#         if ("_class_attribute" %in% names(attrs_payload)) {
#           class_attr <- attrs_payload[["_class_attribute"]]
#           attrs_payload[["_class_attribute"]] <- NULL
#         }
#         if ("_names_attribute" %in% names(attrs_payload)) {
#           names_attr <- attrs_payload[["_names_attribute"]]
#           attrs_payload[["_names_attribute"]] <- NULL
#         }
#         # Process any remaining named elements as custom attributes (recursive)
#         for (attr_name in names(attrs_payload)) {
#           reconstructed_attrs[[attr_name]] <- object_from_json(attrs_payload[[attr_name]])
#         }
#       }
#       
#       reconstructed_obj <- data_part
#       
#       # Pre-process data_part specifically for tibble column handling
#       if (!is.null(class_attr) && any(c("data.frame", "tbl_df", "tibble") %in% class_attr)) {
#         if (is.list(reconstructed_obj)) { # data_part will be a list of columns
#           reconstructed_obj <- lapply(reconstructed_obj, function(col) {
#             if (is.atomic(col) && length(col) > 1 && !is.list(col)) {
#               # This is the crucial fix: If jsonlite::fromJSON simplified a JSON array
#               # (e.g., ["a", "b"]) to an R atomic vector c("a", "b"), and this column
#               # is part of a dataframe that needs list-columns, wrap it in a list.
#               return(list(col))
#             } else if (is.null(col)) {
#               # Handle JSON 'null' values. If the original was NA, convert to NA.
#               # Use NA_character_ for consistency with common tibble behavior for missing char values.
#               return(NA_character_)
#             } else {
#               return(col)
#             }
#           })
#         }
#         
#         if (!is.null(reconstructed_obj)) {
#           reconstructed_obj <- as_tibble(reconstructed_obj)
#         } else {
#           reconstructed_obj <- tibble() # Return an empty tibble if data_part was NULL
#         }
#         class(reconstructed_obj) <- class_attr
#       } else {
#         # Non-dataframe objects: apply names and class if available
#         if (!is.null(names_attr) && is.atomic(reconstructed_obj)) {
#           names(reconstructed_obj) <- names_attr
#         }
#         if (!is.null(class_attr)) {
#           class(reconstructed_obj) <- class_attr
#         }
#       }
#       
#       # Apply any remaining custom attributes that were recursively reconstructed
#       if (length(reconstructed_attrs) > 0) {
#         current_attrs <- attributes(reconstructed_obj)
#         attributes(reconstructed_obj) <- c(current_attrs, reconstructed_attrs)
#       }
#       
#       return(reconstructed_obj)
#       
#     } else if (is.list(json_obj) && !inherits(json_obj, c("data.frame", "tbl_df", "tibble", "json"))) {
#       # Generic R list (recursive processing for its elements)
#       lapply(json_obj, object_from_json)
#     } else {
#       # Atomic objects, unwrapped dataframe (e.g., if jsonlite simplified directly), or already processed JSON.
#       return(json_obj)
#     }
#   }
#   
#   # Apply the recursive reconstruction to the parsed JSON data
#   dataset_dssat <- lapply(raw_json_data, object_from_json)
#   
#   return(dataset_dssat)
# }

# read_json_dataset <- function(file, ...) {
#   raw_json_data <- jsonlite::fromJSON(file, simplifyVector = TRUE, simplifyDataFrame = FALSE, flatten = FALSE)
#   
#   # Helper function to detect if a column should be a list-column
#   should_be_list_column <- function(col) {
#     # If it's already a list, keep it as is
#     if (is.list(col)) return(FALSE)
#     
#     # If it's not atomic, don't convert
#     if (!is.atomic(col)) return(FALSE)
#     
#     # Check if the column contains vectors of varying lengths
#     # (a sign it should be a list-column)
#     if (length(col) > 0) {
#       # For character vectors, check if any element contains the list marker
#       if (is.character(col) && any(grepl("^c\\(.*\\)$", col, perl = TRUE))) {
#         return(TRUE)
#       }
#       
#       # For other atomic types, check if the first element is a vector
#       if (length(dim(col)) > 1) {
#         return(TRUE)
#       }
#     }
#     
#     return(FALSE)
#   }
#   
#   object_from_json <- function(json_obj) {
#     if (is.list(json_obj) && "data" %in% names(json_obj) && "attributes" %in% names(json_obj)) {
#       data_part <- json_obj$data
#       attrs_part <- json_obj$attributes
#       
#       class_attr <- NULL
#       reconstructed_attrs <- list()
#       
#       # Extract class attribute
#       if (is.character(attrs_part) && length(attrs_part) > 0 &&
#           any(c("data.frame", "tbl_df", "tibble") %in% attrs_part)) {
#         class_attr <- attrs_part
#       } else if (is.list(attrs_part) && "_class_attribute" %in% names(attrs_part)) {
#         class_attr <- attrs_part[["_class_attribute"]]
#         attrs_part[["_class_attribute"]] <- NULL
#       }
#       
#       # Handle other attributes
#       if (is.list(attrs_part)) {
#         reconstructed_attrs <- lapply(attrs_part, object_from_json)
#       }
#       
#       # Process data_part
#       if (!is.null(class_attr) && any(c("data.frame", "tbl_df", "tibble") %in% class_attr)) {
#         if (!is.null(data_part)) {
#           processed_columns <- lapply(names(data_part), function(col_name) {
#             col <- data_part[[col_name]]
#             
#             # Handle JSON 'null' values
#             if (is.null(col)) {
#               return(NA_character_)
#             }
#             
#             # Flatten 1-row matrices
#             if (is.matrix(col) && nrow(col) == 1 && is.atomic(col)) {
#               col <- as.vector(col)
#             }
#             
#             # Automatically detect and convert atomic vectors that should be list-columns
#             if (is.atomic(col) && !is.list(col) && should_be_list_column(col)) {
#               # Convert to list-column
#               return(as.list(col))
#             }
#             
#             return(col)
#           })
#           names(processed_columns) <- names(data_part)
#           
#           reconstructed_obj <- tibble::as_tibble(processed_columns)
#         } else {
#           reconstructed_obj <- tibble::tibble()
#         }
#         class(reconstructed_obj) <- class_attr
#       } else {
#         reconstructed_obj <- lapply(data_part, object_from_json)
#         if (!is.null(attrs_part)) {
#           current_attrs <- attributes(reconstructed_obj)
#           attributes(reconstructed_obj) <- c(current_attrs, reconstructed_attrs)
#         }
#       }
#       
#       # Apply remaining attributes
#       if (length(reconstructed_attrs) > 0) {
#         current_attrs <- attributes(reconstructed_obj)
#         reconstructed_attrs <- reconstructed_attrs[!sapply(reconstructed_attrs, is.null)]
#         if(length(reconstructed_attrs) > 0) {
#           attributes(reconstructed_obj) <- c(current_attrs, reconstructed_attrs)
#         }
#       }
#       
#       return(reconstructed_obj)
#     } else if (is.list(json_obj) && !inherits(json_obj, c("data.frame", "tbl_df", "tibble", "json"))) {
#       lapply(json_obj, object_from_json)
#     } else {
#       return(json_obj)
#     }
#   }
