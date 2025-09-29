#' Select DWD weather stations measuring solar radition closest to the given coordinates
#' 
#' Select DWD stations within a given radius around a set of coordinates. Workaround for rdwd::nearbyStations not
#' functioning for solar raidation data
#' 
#' @export
#'
#' @param lat a numeric depicting the latitude (y component) of the target location [degrees N/S, range 47:55]
#' @param lon a numeric depicting the longitude (x component) of the target location [degrees N/S, range 47:55]
#' @param res a character setting the maximum data recording starting date and minimum ending date
#' @param per ###
#' 
#' @return a data frame containing information on weather stations that best  meet the specified location, date range,
#' variables, and temporal resolution parameters
#' 
#' @importFrom magrittr %>%
#' @importFrom rdwd nearbyStations selectDWD dataDWD metaInfo
#' @importFrom dplyr filter arrange distinct left_join mutate
#' @importFrom spsUtil quiet
#' @importFrom geosphere distVincentyEllipsoid
#' @importFrom lubridate as_date
#' 


nearbyStations_solar <- function(lat, lon, res, max_radius = 50){  ##inherit
  
  data_ls <- selectDWD(var = "solar", res = c("daily","hourly"), expand = TRUE)
  
  # Extract the IDs of all stations corresponding to the criteria
  wst_ids <- as.numeric(sapply(data_ls, function(x) {
    x <- strsplit(x, "_")[[1]]
    result <- x[length(x) - 1]
  }))
  
  wst_res <- unlist(
    lapply(data_ls, function(x) {
    strsplit(x, "/")[[1]][8]
  }
  ), use.names = FALSE)
  
  data_urls <- data.frame(Stations_id = as.integer(wst_ids), res = wst_res, url = data_ls)
  
  # Get station data directly from the metadata as nearbyStations does not return solar radiation data consistently
  # Cause: "per" argument set to "" for multiple records, should either "historical" or "recent" for fun to work normally
  stations <- do.call(
    rbind, 
    lapply(wst_ids, function(df) quiet(metaInfo(df)))
  )
  stations <- stations[stations$var == "solar" & stations$res %in% res, ]
  
  # Calculate distance from target location
  target <- as.data.frame(matrix(NA, nrow = 1, ncol = ncol(stations)))
  names(target) <- names(stations)
  target$geoBreite <- lat
  target$geoLaenge <- lon
  
  stations <- rbind(target, stations)
  # Calculate distance between station and queried location
  stations$dist <- distVincentyEllipsoid(
    cbind(stations$geoLaenge, stations$geoBreite),   # all points: (lon, lat)
    c(stations$geoLaenge[1], stations$geoBreite[1])  # reference point
  )
  
  stations <- stations %>%
    # Format dates so filter can be applied
    mutate(von_datum = as_date(von_datum), bis_datum = as_date(bis_datum),
           # Fil gaps in period (as of 21.12.23, missing data in solar for some reason)
           per = ifelse(per == "", "historical", per)) %>%
    #relocate(var_ipt, .before = everything()) %>%
    left_join(data_urls, by = c("Stations_id", "res")) %>%
    filter(dist <= max_radius) %>%
    arrange(dist) %>%
    distinct()

  return(stations)
}

#' Select closest DWD weather stations
#' 
#' Download DWD past weather data for crop modeling relevant variables: air temperature, precipitation, solar radiation,
#' dew point temperature, relative humidity, and wind speed. The function picks the best quality data based on the provided
#' list of weather stations separately for each target variable. Additional arguments for picking data types, like
#' temporal resolution, are specified within the weather station data frame.
#' 
#' @export
#'
#' @param lat a numeric depicting the latitude (y component) of the target location [degrees N/S, range 47:55]
#' @param lon a numeric depicting the longitude (x component) of the target location [degrees N/S, range 47:55]
#' @param min_date a character setting the maximum data recording starting date and minimum ending date
#' @param params ###
#' 
#' @return a data frame containing information on weather stations that best  meet the specified location, date range,
#' variables, and temporal resolution parameters
#' 
#' @importFrom magrittr %>%
#' @importFrom rdwd nearbyStations
#' @importFrom dplyr filter
#' 


# Get station data for each year based on the set quality parameters
find_stations <- function(lat, lon, min_date, max_date, params){
  
  message("Searching for weather stations...")
  
  # Determine which periods to search (historical, recent, or both) ---
  min_date <- as.Date(min_date)
  max_date <- as.Date(max_date)
  boundary_date <- Sys.Date() %m-% months(18)
  
  # Set search period
  search_periods <- c()
  if (min_date <= boundary_date) {
    search_periods <- c(search_periods, "historical")
  }
  if (max_date > boundary_date) {
    search_periods <- c(search_periods, "recent")
  }
  
  if (length(search_periods) == 0) {
    stop("Date range does not fall into a searchable period.")
  }
  message(paste("Searching in:", paste(search_periods, collapse = " & ")))
  
  # Find stations (HACK: except solar_radiation)
  params_ipt <- params[!names(params) == "solar_radiation"]
  
  wst_list <- lapply(names(params_ipt), function(param_name) {
    x <- params_ipt[[param_name]]
    
    # Search in each required period
    stations_per_period <- lapply(search_periods, function(per) {
      rdwd::nearbyStations(
        lat = lat,
        lon = lon,
        radius = x[["max_radius"]][[1]],
        var = x[["pars"]][[1]],
        res = x[["res"]][[1]],
        per = per,
        mindate = min_date,
        quiet = TRUE
      )
    })
    # Combine results from historical/recent and keep unique stations
    combined_stations <- bind_rows(stations_per_period) %>% 
      distinct(Stations_id, .keep_all = TRUE)
    
    return(combined_stations)
  })
  
  # Handle special case for solar radiation
  if ("solar_radiation" %in% names(params)) {
    params_solar <- params$solar_radiation
    wst_solar <- nearbyStations_solar(
      lat = lat,
      lon = lon,
      res = params_solar[["res"]][[1]],
      max_radius = params_solar[["max_radius"]][[1]]
    )
    wst_solar$var <- "solar" # Assign var for later grouping
    wst_list <- append(wst_list, list(wst_solar))
  }
  
  # Combine all found stations into a single data frame
  all_stations <- bind_rows(wst_list)
  
  if (nrow(all_stations) == 0) {
    message("No stations found within the given search criteria.")
    return(NULL)
  }
  
  # --- 3. Filter stations based on the precise date range and current-year logic ---
  current_year <- as.integer(format(Sys.Date(), "%Y"))
  max_date_year <- as.integer(format(max_date, "%Y"))
  
  valid_stations <- all_stations %>%
    filter(
      # Basic requirement: station's activity must overlap with the requested period
      von_datum <= max_date & bis_datum >= min_date
    ) %>%
    filter(
      # End-date requirement: must cover the period until max_date OR meet the special current-year condition
      bis_datum >= max_date | (max_date_year == current_year & as.integer(format(bis_datum, "%Y")) == current_year)
    )
  
  if (nrow(valid_stations) == 0) {
    message("Found stations, but none meet the required date criteria.")
    return(NULL)
  }
  
  # --- 4. For each variable, select the single closest station that meets all criteria ---
  closest_stations <- valid_stations %>%
    group_by(var) %>%
    slice_min(order_by = dist, n = 1, with_ties = FALSE) %>%
    ungroup()
  
  message(paste("Successfully identified", nrow(closest_stations), "closest station(s)."))
  return(closest_stations)
}


#' Download DWD past weather in a specific year for a given set of variables
#' 
#' Download DWD past weather data for crop modeling relevant variables: air temperature, precipitation, solar radiation,
#' dew point temperature, relative humidity, and wind speed. The function picks the best quality data based on the provided
#' list of weather stations separately for each target variable. Additional arguments for picking data types, like
#' temporal resolution, are specified within the weather station data frame.
#' 
#' @export
#'
#' @param pars a character vector specifying the variable(s) to download
#' @param year an integer indicating the target year
#' @param stations a data frame listing target weather stations, a generated by the functions nearbyStations",
#' "nearbyStations_solar", and "find_stations"
#' 
#' @return a list of named data frames, each containing weather data for one of the target variable(s)
#' 
#' @importFrom magrittr %>%
#' @importFrom rdwd selectDWD dataDWD
#' @importFrom dplyr filter arrange
#' @importFrom lubridate as_date year
#' 

download_dwd <- function(
    pars = c("air_temperature","precipitation","solar_radiation","dewpoint","relative_humidity","wind_speed"),
    year,
    stations,
    dir = tempdir()){
  
  from_date <- paste0(year, "-01-01")
  to_date <- paste0(year, "-12-31")
  
  dwd_data <- lapply(pars, function(x){
    
    # Sort the stations by distance from the field, from lower to higher
    station <- stations %>%
      filter(var_ipt == x) %>%
      arrange(dist) %>%
      filter(as_date(von_datum) <= as_date(from_date) & as_date(bis_datum) >= as_date(to_date))
    
    if (nrow(station) == 0){
      data <- data.frame()
      
    } else {

      # Download data starting from the closest station
      for (i in 1:nrow(station)){
        
        url <- selectDWD(id = station[i, ]$Stations_id,
                         res = station[i, ]$res,
                         per = station[i, ]$per,
                         var = station[i, ]$var)
        data <- dataDWD(url, read = TRUE, dir = dir, quiet = TRUE)
        # Append station metadata for later reuse
        metadata <- list(var = x,
                         res = station[i, ]$res,
                         wst_id = paste0("DWD", station[i, ]$Stations_id),
                         wst_name = station[i, ]$Stationsname,
                         wst_lat = station[i, ]$geoBreite,
                         wst_lon = station[i, ]$geoLaenge,
                         wst_elev = station[i, ]$Stationshoehe,
                         dist = station[i, ]$dist)
        
        attr(data, "metadata") <- metadata
        
        # Break the loop if data is available for the target year
        # This check is necessary as some stations have significant time gaps than are not reported in stations metadata
        if (from_date %in% as_date(data$MESS_DATUM) & to_date %in% as_date(data$MESS_DATUM)){
          break
        }
      }
    }
    
    # Print progress information as the process can take a while
    if (nrow(data) == 0) {
      
      message(paste0(year, ": ",  x, " - data not available"))
      return(data)
      
    } else {
      
      data <- data %>%
        filter(as_date(MESS_DATUM) >= as_date(from_date) &  as_date(MESS_DATUM) <= as_date(to_date))
      
      message(paste0(year, ": ",  x, " - data download complete"))
      return(data)
    }

    return(data)
  })
  
  return(dwd_data)
}


#' Format DWD weather data into a specified data standard
#' 
#' Transform DWD weather data into a specified data standard (as of 14.12.23: ICASA or DSSAT)
#' 
#' @export
#'
#' @param data a character vector specifying the variable(s) to download
#' @param lookup a map for a target standards (TODO: replace by standard name, fetch the map internally)
#' 
#' @return a data frame; weather data tabes formatted into the DSSAT standard structure
#' 
#' @importFrom lubridate is.POSIXct is.POSIXlt is.POSIXt as_date
#' 


format_weather <- function(data, lookup) {
  
  # Find date column
  data <- lapply(data, function(x){
    if(is.POSIXct(x)|is.POSIXlt(x)|is.POSIXt(x)){ 
      as_date(x)
    } else {
      x
    }
  })
  data <- as.data.frame(data)
  
  # Drop date column for mapping numeric variables
  date <- data[sapply(data, is_date)]
  data <- data[!names(data) %in% names(date)]
  
  # Create empty dataframe with model-standard naming
  lookup_sub <- lookup[lookup$repo_var %in% names(data),]
  out_fmt <- as.data.frame(matrix(ncol = nrow(lookup_sub), nrow = nrow(data)))
  names(out_fmt) <- lookup_sub$std_var
  
  out_fmt[ncol(out_fmt)+1] <- date

  for (i in 1:nrow(lookup_sub)) {
    
    old_name <- lookup_sub$repo_var[i]
    new_name <- lookup_sub$std_var[i]
    
    if (old_name %in% names(data)) {
      out_fmt[[new_name]] <- data[[old_name]]
    } else {
      out_fmt[[new_name]] <- NA
    }
  }
  
  # Aggregate and convert to model-standard units
  agg_data <- list()
  
  for (i in seq_len(nrow(lookup_sub))) {
    
    column <- lookup_sub$std_var[i]
    agg_fun <- lookup_sub$agg_fun[i]
    conv_fct <- lookup_sub$conv_fct[i]
    
    if (is.na(agg_fun)) {
      agg_var <- cbind(unique(date), var = NA)
      colnames(agg_var)[2] <- column
    } else {
      agg_var <- aggregate(out_fmt[column],
                           by = list(out_fmt[, ncol(out_fmt)]),
                           FUN = function(x) safe_aggregate(x, match.fun(agg_fun)))
      agg_var[2] <- agg_var[2] * conv_fct
    }
    
    agg_data[[i]] <- agg_var
  }
  
  out <- Reduce(function(x,y) merge(x,y), agg_data)
  
  # Map date variable
  names(out)[1] <- na.omit(lookup$std_var[lookup$repo_var == names(date)])[1]
  
  return(out)
}

#' Convert weather metadata table into data frame attributes
#' 
#' @export
#' 
#' @param df a data frame containing weather data
#' @param header a data frame containing weather metadata associated to df, as returned by
#' 
#' @return a data frame containing weather data with attributes set to the values in the header table
#' 

headers_to_attr <- function(df, header) {
  for (col in names(header)) {
    attr(df, col) <- header[[col]]
  }
  return(df) 
}


#' Impute missing values in daily weather time series data
#' 
#' So far only performs linear interpolation and extrapolation. Other methods will be added in the future.
#' (e.g., imputation with remote sensing NASA power data, sequential imputation, random forests, etc.)
#' 
#' @export
#' 
#' @param df a data frame with weather time series data in which NAs are to be replaced
#' @param na.rm logical. If the result of the (spline) interpolation still results in leading and/or trailing NAs,
#' should these be removed (using na.trim)?
#' @param rule an integer (of length 1 or 2) specifying how leading and trailing NAs are handled. 
#' If rule is 1 then NAs are returned for such points and if it is 2, the value at the closest data extreme is used.
#' 
#' @return a data frame with NAs replaced according to the input arguments
#' 
#' @importFrom zoo na.approx
#'

impute_weather <- function(df, na.rm = TRUE, rule = 2) {
  
  na_cols <- colSums(is.na(df)) > 0
  df_na <- df[, na_cols, drop = FALSE]
  
  df_na[] <- lapply(df_na, function(x) {
    if (is.numeric(x)) {
      x <- na.approx(x, na.rm = na.rm, rule = rule)
    }
    return(x)
  })
  
  df <- cbind(df[, !na_cols, drop = FALSE], df_na)
  return(df)
  
  # TODO: other methods (NASA-power imputation, sequential imputation, random forest...)
}

#' Wrapping function for downloading, formatting and imputing daily weather time series
#' 
#' Source repositories are currently limited to DWD Open Data Server, and imputation method to linear interpolation
#' 
#' @export
#'  
#' @param lat a numeric; latitude (y coordinate) of the target location [degrees E/W, range 6:15]
#' @param lon a numeric; longitude (x coordinate) of the target location [degrees N/S, range 47:55]
#' @param years a numeric vector specifying the target years
#' @param src a character vector specifying the source repository (currently only "dwd" is supported).
#' @param map_to a character vector specifying the target data standard (currently only "icasa" and "dssat" are supported)
#' @param pars a character vector specifying the target weather variables.
#' @param res a list of character vectors specifying the temporal resolution of the target weather variables. Length and
#' indices should match the variable vector. A the function returns daily weather, only "hourly" and "daily" are supported.
#' @param max_radius a numeric vector specifying the maximum distance around the location within which weather stations
#' will be selected. Length and indices should match the variable and res vectors.
#' 
#' @return a list of dataframes containing daily weather for the requested location, years and variables
#' Each data frame contains one full year of data.
#' 
#' @importFrom magrittr %>%
#' @importFrom tidyselect everything
#' @importFrom dplyr distinct mutate select group_by summarise pull relocate
#' @importFrom tidyr unnest drop_na all_of 
#' @importFrom tibble rownames_to_column
#' @importFrom purrr map2
#' @importFrom lubridate month
#' @importFrom nasapower get_power
#' 
#' 

get_weather <- function(lon, lat, from, to, src, raw = TRUE, pars, res,
                        # max radius in km; defined separately for each variable as quality requirements differ
                        max_radius = 50){
  
  # Check arguments
  src_handlers <- c("dwd", "nasa-power")
  src <- match.arg(src, src_handlers)
  pars_handlers <- c("air_temperature", "precipitation", "solar_radiation", "dewpoint",
                     "relative_humidity", "wind_speed", "par", "evaporation")
  pars <- match.arg(pars, pars_handlers, several.ok = TRUE)
  res_handlers <- c("daily", "hourly")
  res <- match.arg(res, res_handlers)
  
  # --- Fetch data from source ---
  switch(src,
         "dwd" = {
           
           # --- Define parameters ---
           vars_ipt <- lapply(pars, function(x) {
             switch(x,
                    precipitation = c("precipitation","more_precip"),
                    solar_radiation = "solar",
                    wind_speed = "wind",
                    dewpoint = "dew_point",
                    relative_humidity = "moisture",
                    x)
           }
           )
           params <- data.frame(pars = I(vars_ipt), res = I(res), max_radius = max_radius)
           params <- split(params, seq(nrow(params)))
           for (i in 1:length(params)){
             names(params)[i] <- unique(pars[i])
           }
           
           # --- Find stations meeting the query ---  # TO CHECK
           stations <- find_stations(lat = lat, lon = lon, min_date = from, params = params)
           #names(stations) <- paste0("Y", substr(start_dates, 1, 4))  # old version
           
           stations <- do.call(rbind, stations) %>%
             distinct() %>%
             rownames_to_column("var_ipt") %>%
             # Remove the data frame name prefix appended during row binding
             mutate(var_ipt = gsub("^[^.]*\\.", "", var_ipt)) %>%
             # Remove the station suffix
             mutate(var_ipt = gsub("\\..*", "", var_ipt))
           
           # --- Downdload data ---
           # Here one caveat is that the rdwd::dataDWD function downloads the entire historical data for the specified station(s)
           # Target year is only filtered afterwards in the custom function
           # this makes the runtime needlessly slow for multiple years/variables
           wth_raw <- lapply(years, function(x){
             data <- download_dwd(pars = names(params), year = x, stations)
             names(data) <- names(params)
             return(data)
           }
           )
           names(wth_raw) <- paste0("Y", years)
           
           # metadata <- lapply(wth_raw, function(ls){
           #   nest_df <- as.data.frame(
           #     t(sapply(ls, function(df){
           #       if(is.null(attr(df, "metadata"))){
           #         return(rep(NA, 8))
           #       } else {
           #         return(attr(df, "metadata"))
           #       }
           #     }))
           #   )
           #   df <- unnest(nest_df, cols = colnames(nest_df), keep_empty = TRUE) %>% distinct()
           #   df <- df %>% drop_na()
           # })
         },
         "nasa-power" = {
           
           # --- Define parameters ---
           params <- unlist(
             lapply(pars, function(x) {
               switch(x,
                      air_temperature = c("T2M", "T2M_MAX", "T2M_MIN"), 
                      precipitation = "PRECTOTCORR",
                      solar_radiation = "ALLSKY_SFC_SW_DWN",
                      wind_speed = "WS2M",
                      dewpoint = "T2MDEW",
                      relative_humidity = "RH2M",
                      par = "ALLSKY_SFC_PAR_TOT",
                      evaporation = "EVLAND",
                      x)
             })
           )

           # --- Downdload data ---
           wth_raw <- get_power(
             community = "ag",
             pars = params,
             temporal_api = res,
             lonlat = c(lon, lat),
             dates = c(as.character(from), as.character(to))
           )
           wth_raw <- list(WEATHER_DAILY = wth_raw)
         })
  

  # --- Map data to ICASA ---
  if (!raw) {
    wth_icasa <- convert_dataset(
      dataset = wth_raw,
      input_model = src,
      output_model = "icasa"
    )
  }

  # pars <- c("MESS_DATUM", lookup[["repo_var"]][!is.na(lookup[["repo_var"]])])
  # 
  # # Apply mapping
  # dwd_ipt <- lapply(dwd_ipt, function(ls) {
  #   lapply(ls, function(df){
  #     common_vars <- intersect(pars, names(df))
  #     df[, common_vars, drop = FALSE]
  #   })
  # })
  # 
  # dwd_trans <- lapply(dwd_ipt, function(ls){
  #   lapply(ls, function(df) data <- format_weather(df, lookup = lookup))
  # })
  # 
  # dwd_out <- lapply(dwd_trans, function(ls){
  #   Reduce(function(x, y) merge(x, y, by = intersect(names(x), names(y)), all = TRUE), ls)
  # })
  # 
  # 
  # # Impute missing data -----------------------------------------------------
  # 
  # dwd_out <- lapply(dwd_out, function(df){
  #   if (anyNA(df)) {
  #     
  #     col_order <- colnames(df)
  #     
  #     df <- impute_weather(df, na.rm = TRUE, rule = 2)
  #     #df <- df %>% select(all_of(col_order))
  #   }
  #   return(df)
  # })
  # 
  # # Calculate summary data for weather data header
  # TAV <- lapply(dwd_out, function(df){
  #   df %>% summarise(TAV = mean((TMAX + TMIN)/2, na.rm = TRUE)) %>% pull(TAV)
  # })
  # metadata <- map2(metadata, TAV, ~cbind(.x, TAV = .y))
  # 
  # AMP <- lapply(dwd_out, function(df){
  #   df %>%
  #     mutate(mo = month(W_DATE)) %>%
  #     group_by(mo) %>%
  #     summarise(mo_TAV = mean((TMAX + TMIN)/2, na.rm = TRUE)) %>%
  #     summarise(AMP = (max(mo_TAV)-min(mo_TAV))/2) %>%
  #     pull(AMP)
  # })
  # metadata <- map2(metadata, AMP, ~cbind(.x, AMP = .y))
  # 
  # 
  # # Bind all years in a single dataframe
  # dwd_out <- lapply(dwd_out, function(df){
  #   df %>%
  #     mutate(Year = year(W_DATE)) %>%  # add year column
  #     relocate(Year, .before = everything())
  # })
  # 
  # all_cols <- unique(unlist(lapply(dwd_out, colnames)))
  # 
  # for (i in seq_along(dwd_out)) {
  #   missing_cols <- setdiff(all_cols, names(dwd_out[[i]]))
  #   for (j in missing_cols) {
  #     dwd_out[[i]][[j]] <- NA
  #   }
  # }
  # 
  # dwd_out <- lapply(dwd_out, function(x) x[, all_cols])
  # dwd_out_df <- do.call(rbind, dwd_out)
  # row.names(dwd_out_df) <- NULL

  return(wth_icasa)
}
