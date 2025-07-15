#' Obtain an Access Token from a Keycloak Server Using Resource Owner Password Credentials
#'
#' Requests an OAuth2 access token from a Keycloak authentication server using the Resource Owner Password Credentials Grant (password grant).
#'
#' @param url Character. The token endpoint URL of the Keycloak server (typically ends with \code{/protocol/openid-connect/token}).
#' @param client_id Character. The client ID registered in Keycloak.
#' @param client_secret Character. The client secret associated with the client ID.
#' @param username Character. The username of the Keycloak user.
#' @param password Character. The password of the Keycloak user.
#'
#' @details
#' This function sends a POST request to the Keycloak token endpoint with the provided credentials and client information, using the OAuth2 Resource Owner Password Credentials Grant. The function expects a successful response to contain an \code{access_token} field.
#'
#' The function uses the \strong{httr} package for HTTP requests. The token is extracted from the response and returned as a character string.
#'
#' @return A character string containing the access token, or \code{NULL} if the request fails or the token is not found.
#'
#' @examples
#' \dontrun{
#' token <- get_kc_token(
#'   url = "https://my-keycloak-server/auth/realms/myrealm/protocol/openid-connect/token",
#'   client_id = "myclient",
#'   client_secret = "mysecret",
#'   username = "myuser",
#'   password = "mypassword"
#' )
#' }
#'
#' @importFrom httr POST content add_headers verbose
#' 
#' @export
#' 

get_kc_token <- function(url, client_id, client_secret, username, password) {
  
  response <- POST(
    url = url,
    body = list(
      grant_type = "password",
      client_id = client_id,
      client_secret = client_secret,
      username = username,
      password = password
    ),
    encode = "form",
    add_headers(`Content-Type` = "application/x-www-form-urlencoded"),
    verbose()
  )
  
  token <- content(response)$access_token
  
  return(token)
}


#' POST Data to an OGC SensorThings API Endpoint
#'
#' Sends a POST request to an OGC SensorThings API endpoint to create a new resource (e.g., Thing, Sensor, ObservedProperty, Datastream, or Observation).
#'
#' @param object Character. The type of resource to create. Must be one of \code{c("Things", "Sensors", "ObservedProperties", "Datastreams", "Observations")}.
#' @param body A list representing the JSON body to be sent in the request.
#' @param url Character. The base URL of the OGC SensorThings API endpoint (should end with a slash).
#' @param token Character. The Bearer token for authentication.
#'
#' @details
#' This function converts the provided \code{body} to JSON and sends it as a POST request to the specified OGC SensorThings API endpoint, appending the resource type (\code{object}) to the base URL. The request includes the provided Bearer token for authentication.
#'
#' The function uses the \strong{httr} package for HTTP requests and the \strong{jsonlite} package for JSON conversion.
#'
#' @return The response object from the POST request (an \code{httr::response} object).
#'
#' @examples
#' \dontrun{
#' post_ogc_iot(
#'   object = "Things",
#'   body = list(name = "MyThing", description = "A test thing"),
#'   url = "https://example.com/SensorThings/v1.0/",
#'   token = "your_access_token"
#' )
#' }
#'
#' @importFrom httr POST add_headers verbose
#' @importFrom jsonlite toJSON
#' 
#' @export
#' 

post_ogc_iot <- function(object = c("Things","Sensors","ObservedProperties","Datastreams","Observations"), body, url, token){
  
  body_json <- toJSON(body, auto_unbox = TRUE)
  
  url <- paste0(url, object)
  response <- POST(url, body = body_json, encode = "json",
                   add_headers(
                     `Content-Type` = "application/json",
                     `Authorization` = paste("Bearer", token)
                   ),
                   verbose())
}


#' Delete a Resource from an OGC SensorThings API Endpoint
#'
#' Sends a DELETE request to an OGC SensorThings API endpoint to remove a specified resource (e.g., Thing, Sensor, ObservedProperty, Datastream, or Observation) by its ID.
#'
#' @param object Character. The type of resource to delete. Must be one of \code{c("Things", "Sensors", "ObservedProperties", "Datastreams", "Observations")}.
#' @param object_id The unique identifier of the resource to delete.
#' @param url Character. The base URL of the OGC SensorThings API endpoint (should end with a slash).
#' @param token Character. The Bearer token for authentication.
#'
#' @details
#' This function constructs the full URL for the resource by appending the resource type (\code{object}) and the resource ID (\code{object_id}) in OData format to the base URL. It then sends a DELETE request to this URL, including the provided Bearer token for authentication.
#'
#' The function checks that the URL starts with \code{http://} or \code{https://} and stops with an error if not.
#'
#' The function uses the \strong{httr} package for HTTP requests.
#'
#' @return The response object from the DELETE request (an \code{httr::response} object).
#'
#' @examples
#' \dontrun{
#' delete_ogc_iot(
#'   object = "Things",
#'   object_id = 123,
#'   url = "https://example.com/SensorThings/v1.0/",
#'   token = "your_access_token"
#' )
#' }
#'
#' @importFrom httr DELETE add_headers
#' 
#' @export
#' 

delete_ogc_iot <- function(object = c("Things","Sensors","ObservedProperties","Datastreams","Observations"), object_id, url, token){
  
  if (!grepl("^http[s]?://", url)) {
    stop("Invalid URL: Must start with http:// or https://")
  }
  
  url <- paste0(url, object, "(", object_id, ")")
  response <- DELETE(url,
                     add_headers(
                       `Content-Type` = "application/json",
                       `Authorization` = paste("Bearer", token)
                       ))
}


#' Update a Resource on an OGC SensorThings API Endpoint (PATCH)
#'
#' Sends a PATCH request to an OGC SensorThings API endpoint to update a specified resource (e.g., Thing, Sensor, ObservedProperty, Datastream, or Observation) by its ID.
#'
#' @param object Character. The type of resource to update. Must be one of \code{c("Things", "Sensors", "ObservedProperties", "Datastreams", "Observations")}.
#' @param object_id The unique identifier of the resource to update.
#' @param url Character. The base URL of the OGC SensorThings API endpoint (should end with a slash).
#' @param token Character. The Bearer token for authentication.
#' @param body A list representing the JSON body with the fields to update.
#'
#' @details
#' This function constructs the full URL for the resource by appending the resource type (\code{object}) and the resource ID (\code{object_id}) in OData format to the base URL. It converts the \code{body} to JSON and sends a PATCH request to this URL, including the provided Bearer token for authentication.
#'
#' The function checks that the URL starts with \code{http://} or \code{https://} and stops with an error if not.
#'
#' The function uses the \strong{httr} package for HTTP requests and the \strong{jsonlite} package for JSON conversion.
#'
#' @return The response object from the PATCH request (an \code{httr::response} object).
#'
#' @examples
#' \dontrun{
#' patch_ogc_iot(
#'   object = "Things",
#'   object_id = 123,
#'   url = "https://example.com/SensorThings/v1.0/",
#'   token = "your_access_token",
#'   body = list(name = "Updated Thing Name")
#' )
#' }
#'
#' @importFrom httr PATCH add_headers
#' @importFrom jsonlite toJSON
#' 
#' @export
#' 

patch_ogc_iot <- function(object = c("Things","Sensors","ObservedProperties","Datastreams","Observations"), object_id, url, token, body){
  
  if (!grepl("^http[s]?://", url)) {
    stop("Invalid URL: Must start with http:// or https://")
  }
  body_json <- toJSON(body, auto_unbox = TRUE)
  
  url <- paste0(url, object, "(", object_id, ")")
  response <- PATCH(url, body = body_json, encode = "json",
                    add_headers(
                      `Content-Type` = "application/json",
                      `Authorization` = paste("Bearer", token)
                    ))
}


#' Locate OGC SensorThings Datastreams by Location, Variable, and Timeframe
#'
#' Queries an OGC SensorThings API endpoint to find datastreams at a specified longitude and latitude, for selected observed properties, and within a given time range.
#'
#' @param url Character. The base URL of the OGC SensorThings API endpoint (should end with a slash).
#' @param token Character or NULL. The Bearer token for authentication, or NULL if not required.
#' @param var Character vector. The names of observed properties (variables) to search for (e.g., \code{c("air_temperature", "solar_radiation", "rainfall")}).
#' @param lon Numeric. Longitude of the target location.
#' @param lat Numeric. Latitude of the target location.
#' @param from Character or Date. Start date of the desired time range (inclusive).
#' @param to Character or Date. End date of the desired time range (inclusive).
#' @param ... Additional arguments passed to internal functions.
#'
#' @details
#' The function first identifies all devices (Things) from the SensorThings API, extracting their locations. It then retrieves all datastreams for devices at the specified coordinates, including metadata about observed properties and measurement periods. It filters datastreams to those matching the requested observed properties (\code{var}) and checks if their measurement periods encompass the requested time range (\code{from} to \code{to}).
#'
#' The function returns a data frame of matching datastreams, or a message if no suitable data is found.
#'
#' The function uses the \strong{httr}, \strong{jsonlite}, \strong{dplyr}, \strong{tidyr}, and \strong{tibble} packages.
#'
#' @return A data frame of matching datastreams, or a character message if no data is found for the specified criteria.
#'
#' @examples
#' \dontrun{
#' locate_sta_datastreams(
#'   url = "https://example.com/SensorThings/v1.0/",
#'   token = "your_access_token",
#'   var = c("air_temperature", "rainfall"),
#'   lon = 12.34,
#'   lat = 56.78,
#'   from = "2022-01-01",
#'   to = "2022-12-31"
#' )
#' }
#'
#' @importFrom httr GET add_headers content
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr %>% filter mutate pull rowwise select rename group_by_at summarise across all_of bind_rows
#' @importFrom tidyr separate
#' @importFrom tibble tibble
#' 

# TODO: update function to handle multiple devices in single location
locate_sta_datastreams <- function(url, token = NULL, var = c("air_temperature","solar_radiation","rainfall"), lon, lat, from, to, ...){
  
  # Identify all devices from the target server
  locate_sta_devices <- function(...) {
    
    url_locs <- paste0(url, "Things?$expand=Locations")
    response <- GET(url_locs, add_headers(`Authorization` = paste("Bearer", token)))
    
    devices <- fromJSON(
      content(response, as = "text", encoding = "UTF-8")
    )
    
    locations <- devices$value$Locations
    
    devices <- do.call(
      rbind,
      lapply(locations, function(df){
        df %>%
          rowwise() %>%
          mutate(x = location$coordinates[[1]][1],
                 y = location$coordinates[[1]][2],
                 url = paste0(url, "Things(", `@iot.id`, ")")) %>%
          rename(location_name = name, location_description = description) %>%
          #filter(x == lon & y == lat) %>%
          select(`@iot.id`, url, location_name, location_description, x, y) %>%
          as.data.frame()
      })
    )
    
    # if (nrow(devices) == 0) {
    #   return("No device was found at the specified coordinates.")
    # } else {
    #   return(devices)
    # }
  }
  
  devices <- locate_sta_devices(url, token)
  #devices <- rbind(devices, devices)  #tmp
  devices_nms <- paste0("device_", devices$`@iot.id`)
  
  # Retrieve all datastreams for the selected devices
  url_dev_ds <- paste0(devices$url, "?$expand=Datastreams")
  response <- lapply(url_dev_ds, function(url) GET(url, add_headers(`Authorization` = paste("Bearer", token))))
  
  url_ds <- lapply(response, function(x) {
    fromJSON(
      content(x, as = "text", encoding = "UTF-8")
    )$Datastreams %>%
      pull(`@iot.selfLink`)
  })
  
  # Get datastreams metadata incl. observed properties
  url_ds_prop <- lapply(url_ds, function(url) paste0(url, "?$expand=ObservedProperty"))
  response <- lapply(url_ds_prop, function(urls) {
    lapply(urls, function(url) {
      GET(url, add_headers(`Authorization` = paste("Bearer", token)))
    })
  })
  
  ds_ls <- lapply(response, function(dev) {
    lapply(dev, function(x){
      fromJSON(
        content(x, as = "text", encoding = "UTF-8")
      )
    })
  })
  names(ds_ls) <- devices_nms
  
  ds_out <- list()
  for (i in seq_along(ds_ls)) {
    ds_out[[i]] <- 
      do.call(
        rbind,
        lapply(ds_ls[[i]], function(ds) {
          tibble(Datastream_id = ds$`@iot.id`,
                 Datastream_link = ds$`@iot.selfLink`,
                 Datastream_name = ds$name,
                 Datastream_description = ds$description,
                 observationType = ds$observationType,
                 ObservedProperty_id = ds$ObservedProperty$`@iot.id`,
                 ObservedProperty_name = ds$ObservedProperty$name,
                 unitOfMeasurement_name = ds$unitOfMeasurement$name,
                 unitOfMeasurement_symbol = ds$unitOfMeasurement$symbol,
                 longitude = as.numeric(ds$observedArea$coordinates[1]),
                 latitude = as.numeric(ds$observedArea$coordinates[2]),
                 phenomenonTime = ds$phenomenonTime) %>%
            separate(phenomenonTime, into = c("start_date","end_date"), sep = "/") %>%
            mutate(across(start_date:end_date, ~ as.Date(.x)))
        })
      )
  }
  
  datastreams <- do.call(rbind, ds_out)

  # Find focal datastream(s)
  out <- datastreams %>% filter(longitude == lon & latitude == lat)

  if (nrow(out) == 0) {
    return("No data was measured at the specified location")
  } else {
    out <- out %>% filter(ObservedProperty_name %in% var)
    if (nrow(out) == 0) {
      return("No data for the focal property could be retrieved at the specified location.")
    } else {
      out <- out %>%
        mutate(is_contained = as.Date(from) >= start_date & as.Date(to) <= end_date) %>%
        filter(is_contained)
      if (nrow(out) == 0) {
        return("Measured data does not encompass the requested timeframe.")
      } else {
        return(out[-ncol(out)])
      }
    }
  }
}

#' Retrieve All Observations from a SensorThings Datastream
#'
#' Fetches all observations from a specified OGC SensorThings API datastream, handling server-side pagination as needed.
#'
#' @param url Character. The URL of the datastream endpoint (should include \code{?$expand=Observations} or similar).
#' @param token Character. The Bearer token for authentication.
#'
#' @details
#' This function repeatedly queries the SensorThings API for observations associated with a datastream, following pagination links if present. It accumulates all observations into a single data frame, including the \code{phenomenonTime} and \code{result} fields.
#'
#' The function uses the \strong{httr} and \strong{jsonlite} packages for HTTP requests and JSON parsing.
#'
#' @return A data frame containing all observations for the specified datastream, with columns \code{phenomenonTime} and \code{result}.
#'
#' @examples
#' \dontrun{
#' get_all_obs(
#'   url = "https://example.com/SensorThings/v1.0/Datastreams(1)?$expand=Observations",
#'   token = "your_access_token"
#' )
#' }
#'
#' @importFrom httr GET add_headers content
#' @importFrom jsonlite fromJSON
#' 

get_all_obs <- function(url, token) {
  
  url_ds <- sub("\\?.*", "", url)  # datastream url
  
  all_obs <- data.frame(phenomenonTime = character(0), result = numeric(0))
  i = 0
  repeat {
    response <- GET(url, add_headers(`Authorization` = paste("Bearer", token)))
    content <- fromJSON(content(response, as = "text", encoding = "UTF-8"))
    all_obs <- rbind(all_obs, content$Observations)
    if (is.null(content$`Observations@iot.nextLink`)) break
    i <- i+100
    url <- paste0(url_ds, "?$expand=Observations($select=phenomenonTime,result;$skip=",i,"),ObservedProperty($select=name)")
  }
  return(all_obs)
}


#' Extract and Format IoT Observational Data from OGC SensorThings API
#'
#' Retrieves, merges, and formats observational data for specified variables, location, and time range from an OGC SensorThings API endpoint.
#'
#' @param url Character. The base URL of the OGC SensorThings API endpoint (should end with a slash).
#' @param token Character or NULL. The Bearer token for authentication, or NULL to attempt retrieval via \code{get_kc_token}.
#' @param var Character vector. The names of observed properties (variables) to extract (e.g., \code{c("air_temperature", "solar_radiation", "rainfall")}).
#' @param lon Numeric. Longitude of the target location.
#' @param lat Numeric. Latitude of the target location.
#' @param from Character or Date. Start date of the desired time range (inclusive).
#' @param to Character or Date. End date of the desired time range (inclusive).
#' @param format Character. Output format for the data. Either \code{"default"} (raw merged data) or \code{"icasa"} (mapped to ICASA format). Default is \code{"icasa"}.
#'
#' @details
#' This function orchestrates the retrieval of datastream metadata and observations for the specified variables, location, and time range. It merges the resulting data frames, optionally maps them to the ICASA format using \code{map_data}, and attaches metadata as an attribute. If \code{token} is NULL, it attempts to retrieve one using \code{get_kc_token}.
#'
#' The function uses the \strong{httr}, \strong{jsonlite}, \strong{dplyr}, \strong{lubridate}, and other helper functions such as \code{locate_sta_datastreams}, \code{get_all_obs}, and \code{map_data}.
#'
#' @return A data frame containing merged observational data for the specified variables, with metadata attached as an attribute. The format depends on the \code{format} argument.
#'
#' @examples
#' \dontrun{
#' extract_iot(
#'   url = "https://example.com/SensorThings/v1.0/",
#'   token = "your_access_token",
#'   var = c("air_temperature", "rainfall"),
#'   lon = 12.34,
#'   lat = 56.78,
#'   from = "2022-01-01",
#'   to = "2022-12-31",
#'   format = "icasa"
#' )
#' }
#'
#' @importFrom dplyr %>% mutate select
#' @importFrom lubridate ymd_hms
#' 
#' @export
#' 

extract_iot <- function(url, token = NULL, var = c("air_temperature","solar_radiation","rainfall"),
                        lon, lat, from, to, format = "icasa") {
  
  if(is.null(token)) {
    token <- get_kc_token(url = url, client_id, client_secret, username, password)  # check token
  }
  
  # Find datastream
  ds_metadata <- locate_sta_datastreams(url, token, var, lon, lat, from, to)
  
  urls_obs <- paste0(
    ds_metadata$Datastream_link,
    "?$expand=Observations($select=phenomenonTime,result;$skip=0),ObservedProperty($select=name)"
  )
  data <- lapply(urls_obs, function(url) get_all_obs(url, token))
  names(data) <- var
  
  # Append metadata and format raw data
  metadata <- list()
  for (i in seq_along(data)){
    metadata[[i]] <- ds_metadata[i,]  #TODO: enrich metadata w/ device/sensor name and description
    
    data[[i]] <- data[[i]] %>%
      mutate(measurement_date = ymd_hms(phenomenonTime)) %>%
      mutate(!!metadata[[i]]$ObservedProperty_name := as.numeric(result)) %>%
      select(-phenomenonTime, -result)
  }
  
  data_cmn <- Reduce(intersect, lapply(data, colnames))
  data <- Reduce(function(x, y) merge(x, y, by = data_cmn, all = TRUE), data)
  metadata_cmn <- Reduce(intersect, lapply(metadata, colnames))
  metadata <- Reduce(function(x, y) merge(x, y, by = metadata_cmn, all = TRUE), metadata)

  # Map data to specified format
  if (format == "default") {
    
    # No transformation needed, just merge
  } else if (format == "icasa") {

      tmp <- map_data(data, input_model = "ogcAgrovoc", output_model = "icasa",
                      map = load_map(), keep_unmapped = FALSE)
      tmp1 <- map_data(metadata, input_model = "ogcAgrovoc", output_model = "icasa",
                       map = load_map(), keep_unmapped = FALSE)
      
  } else {
    
    stop("Error: format must be either 'default' or 'icasa'.")
  }
  

  
  attr(data, "metadata") <- do.call(
    rbind, 
    lapply(names(data), function(name) cbind(var = name, attributes(data[[name]])$metadata))
  )
  
  return(data)
}

