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
#' @importFrom magrittr %>%
#' @importFrom tidyr unnest unnest_wider separate
#' @importFrom dplyr mutate select pull bind_rows across distinct filter
#' @importFrom purrr map_dbl
#' @importFrom tibble tibble
#' 

locate_sta_datastreams <- function(url, token = NULL, var, lon, lat,  radius = 0, from, to,...){
  
  # --- Identify all devices from the target server ---
  locate_sta_devices <- function(...) {
    
    url_locs <- paste0(url, "Things?$expand=Locations")
    response <- GET(url_locs, add_headers(`Authorization` = paste("Bearer", token)))
    
    devices <- fromJSON(content(response, as = "text", encoding = "UTF-8"))
    
    devices <- devices$value %>%
      # Unnest the 'Locations' list-column.
      unnest(Locations, names_sep = "_") %>%
      # Unnest 'Locations_location' data frame column; creates new columns for 'type' and 'coordinates'.
      unnest_wider(Locations_location, names_sep = "_") %>%
      unnest(Locations_location_coordinates) %>%  # simplify
      # Split coordinates into separate vectors
      mutate(longitude = map_dbl(Locations_location_coordinates, ~ .x[1]),
             latitude  = map_dbl(Locations_location_coordinates, ~ .x[2])) %>%
      select(-Locations_location_coordinates)

    return(devices)
  }
  
  # --- Data retrieval ---
  devices <- locate_sta_devices(url, token)
  devices_names <- devices$name
  
  # Retrieve all datastreams for the selected devices
  # CHECK: here could add logic to retrieve sensor type info and structure data model
  url_dev_ds <- paste0(devices$`@iot.selfLink`, "?$expand=Datastreams")
  response <- lapply(url_dev_ds, function(url) GET(url, add_headers(`Authorization` = paste("Bearer", token))))
  
  url_ds_with_nulls <- lapply(response, function(x) {
    tryCatch({
      fromJSON(content(x, as = "text", encoding = "UTF-8"))$Datastreams %>%
        pull(`@iot.selfLink`)
    }, error = function(e) {
      iot_id <- gsub(".*Things\\((\\d+)\\).*", "\\1", x$url)
      warning(paste("Could not retrieve datastreams for Thing with @iot.id:", iot_id), 
              call. = FALSE)
      return(NULL)
    })
  })
  
  # Create a logical index of which devices were successful
  is_valid <- !sapply(url_ds_with_nulls, is.null)
  devices_names <- devices_names[is_valid]
  url_ds <- url_ds_with_nulls[is_valid]

  # Get datastreams metadata incl. observed properties and device identification
  url_ds_prop <- lapply(url_ds, function(url) paste0(url, "?$expand=ObservedProperty"))
  response <- lapply(url_ds_prop, function(urls) {
    lapply(urls, function(url) GET(url, add_headers(`Authorization` = paste("Bearer", token))))
  })
  
  ds_ls <- lapply(response, function(dev) {
    lapply(dev, function(x) {
      tryCatch({
        fromJSON(content(x, as = "text", encoding = "UTF-8"))
      }, error = function(e) {
        datastream_id <- gsub(".*Datastreams\\((\\d+)\\).*", "\\1", x$url)  # TODO: fix regex
        warning(
          paste("Could not parse content for Datastream with @iot.id:", datastream_id),
          call. = FALSE
        )
        return(NULL)
      })
    })
  })
  names(ds_ls) <- devices_nms
  
  # Compile output [FIXED NESTED DEVICES [multiple sensors per device]]
  ds_out <- list()
  for (i in seq_along(ds_ls)) {

    ds_out[[i]] <- 
      bind_rows(
        lapply(ds_ls[[i]], function(ds) {
          
          coords <- na.omit(as.numeric(ds$observedArea$coordinates))
          if (length(coords) < 2) return(NULL)  # Skip if coordinates are invalid
          
          tibble(Datastream.id = ds$`@iot.id`,
                 Datastream.link = ds$`@iot.selfLink`,
                 Datastream.name = ds$name,
                 Datastream.description = ds$description,
                 observationType = ds$observationType,
                 ObservedProperty.id = ds$ObservedProperty$`@iot.id`,
                 ObservedProperty.name = ds$ObservedProperty$name,
                 unitOfMeasurement.name = ds$unitOfMeasurement$name,
                 unitOfMeasurement.symbol = ds$unitOfMeasurement$symbol,
                 longitude = coords[1],
                 latitude = coords[2],
                 phenomenonTime = ds$phenomenonTime) %>%
            separate(phenomenonTime, into = c("start_date","end_date"), sep = "/") %>%
            mutate(across(start_date:end_date, ~ as.Date(.x)))
        })
      )
  }
  
  # Re-assign names  
  is_valid <- !sapply(ds_out, is.null)  # not null (has 2 geocoordinates)
  ds_out_filtered <- ds_out[is_valid]
  ds_names <- names(ds_ls)[is_valid]
  names(ds_out_filtered) <- ds_names
  ds_out_filtered <- ds_out_filtered[sapply(ds_out_filtered, function(df) length(df) > 0)]

  # Combine datastreams in one dataframe
  datastreams <- bind_rows(ds_out_filtered, .id = "Thing.name")
  if (is.null(datastreams) || nrow(datastreams) == 0) {
    return("No valid datastreams could be retrieved from the server.")
  }

  # Find focal datastreams based on coordinate and radius inputs
  focal_datastreams_list <- lapply(seq_along(lon), function(i) {
    current_lon <- lon[i]
    current_lat <- lat[i]
    
    distances <- haversine_dist(current_lat, current_lon, datastreams$latitude, datastreams$longitude)
    in_radius_idx <- which(distances <= radius)
    datastreams_in_radius <- datastreams[in_radius_idx, ]
    
    if (nrow(datastreams_in_radius) > 0) {
      datastreams_in_radius$input_lon <- current_lon
      datastreams_in_radius$input_lat <- current_lat
      datastreams_in_radius$distance_m <- distances[in_radius_idx]
    }
    return(datastreams_in_radius)
  })
  out <- bind_rows(focal_datastreams_list) %>% distinct()

  if (nrow(out) == 0) {
    return("No data was measured within the specified radius of the given location(s).")
  }
  
  out <- out %>% filter(ObservedProperty.name %in% var)
  if (nrow(out) == 0) {
    return("No data for the focal property could be retrieved at the specified location(s).")
  }
  
  out <- out %>%
    mutate(is_contained = start_date >= as.Date(from) & end_date <= as.Date(to)) %>%
    filter(is_contained)
  
  if (nrow(out) == 0) {
    return("Measured data does not encompass the requested timeframe.")
  } else {
    return(out %>% select(-is_contained)) # Remove the temporary column
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
#' @importFrom magrittr %>%
#' @importFrom rlang !! :=
#' @importFrom dplyr mutate select bind_rows group_by summarise across where
#' @importFrom lubridate ymd_hms
#' @importFrom purrr map pluck
#' 
#' @export
#' 

extract_iot <- function(url, token = NULL, var = c("air_temperature","solar_radiation","rainfall"),
                        lon, lat, radius, from, to, raw = FALSE, merge_ds = TRUE) {
  
  # --- Authentication ---
  if(is.null(token)) {
    token <- get_kc_token(url = url, client_id, client_secret, username, password)  # check token
  }
  
  # --- Identify datastreams macthing the input ---
  message("Retrieving data streams...")
  
  ds_metadata <- locate_sta_datastreams(
    url = url,
    token = token,
    var = var,
    lon = lon,
    lat = lat,
    radius = radius,
    from = from,
    to = to
  )
  
  # --- Download all observations for the focal datastreams ---
  message("Downloading observations ...")
  
  urls_obs <- paste0(
    ds_metadata$Datastream.link,
    "?$expand=Observations($select=phenomenonTime,result;$skip=0),ObservedProperty($select=name)"
  )
  obs <- lapply(urls_obs, function(url) get_all_obs(url, token))
  
  # Attach device metadata to each datastream
  dataset <- list()
  for (i in seq_along(obs)){
    obs_df <- obs[[i]] %>%
      mutate(phenomenonTime = ymd_hms(phenomenonTime)) %>%
      mutate(!!ds_metadata$ObservedProperty.name[i] := as.numeric(result)) %>%
      select(-result)
    dataset[[i]] <- list(
      DATASTREAM = obs_df,
      METADATA = ds_metadata[i, ]
    )
  }
  names(dataset) <- paste(ds_metadata$Thing.name, ds_metadata$Datastream.id, sep = "_DS")

  # --- Map data to ICASA ---
  if (!raw) {
    message("Mapping to ICASA ...")
    dataset <- lapply(dataset, function(ls) {
      convert_dataset(
        dataset = ls,
        input_model = "user_sta",  # TODO: input routine
        output_model = "icasa"
      )
    })
  }
  
  # --- Group by device ---
  keys <- sub("_[^_]+$", "", names(dataset))
  aggregated_daily <- split(dataset, keys)
  
  # Merge same-attribute datastream per device
  if (merge_ds) {
    out_dataset <- map(aggregated_daily, function(station_data) {
      
      wth_daily <- map(station_data, ~ .x$WEATHER_DAILY)
      
      wth_daily_merged <- bind_rows(wth_daily) %>%
        group_by(weather_date) %>%
        summarise(across(
          .cols = where(is.numeric),
          .fns = ~mean(.x, na.rm = TRUE)
        ))
      WEATHER_METADATA <- pluck(station_data, 1, "WEATHER_METADATA")
      list(WEATHER_METADATA = WEATHER_METADATA, WEATHER_DAILY = wth_daily_merged)
    })
  }

  return(out_dataset)
}
