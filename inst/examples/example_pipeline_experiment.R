## -----------------------------------------------------------------------------------
## Script name: example_pipeline_experiment.R
## Purpose of script: showcasing etl pipeline prototype of FAIRagro 
## UC6 as an example of application of FAIR RDM
##
## Author: Benjamin Leroy
## Date Created: 2024-01-31
## Copyright (c) Benjamin Leroy, 2024
## Email: benjamin.leroy@tum.de
## -----------------------------------------------------------------------------------
## Notes: data are at least partially simulated and do not represent the actual
## experiment results
## 
## -----------------------------------------------------------------------------------

###----- Crop management/manually input data (template) ------------------------------
## -----------------------------------------------------------------------------------
## This section demonstrates ###
##
## -----------------------------------------------------------------------------------

# Parameters
template_path <- "./inst/extdata/template_icasa_vba.xlsm"

# Extract template data
template_icasa <- extract_template(template_path, headers = "long", keep_null_events = FALSE)
mngt_obs_icasa <- template_icasa$HWOC2501  # select focal experiment

# Retrieve cultivation season temporal coverage to identify weather requirements
mngt_tables <-
  mngt_obs_icasa[!names(mngt_obs_icasa) %in% c("GENERAL", "PERSONS", "INSTITUTIONS")]
cseason <- identify_production_season(
  mngt_tables,
  period = "cultivation_season",
  output = "bounds"
)
print(cseason)  # Note: the cultivation season overlaps 2024 and 2025


###----- Weather data ----------------------------------------------------------------
## -----------------------------------------------------------------------------------
## This section showcase the acquisition of IoT sensor data (weather ## time series)
## from a weather station operating on the study field in Triesdorf and stored on
## FROST server (OGC SensorThings API). The steps include (1) pulling the data;
## (2) transform to DSSAT input format.The 'extract' routine requires FROST server
## credentials.
##
## -----------------------------------------------------------------------------------

# --- Extract all sensor weather data from the field location for 2025 ---

# Set FROST server credentials
base_url <- "https://keycloak.hef.tum.de/realms/master/protocol/openid-connect/token"
user_url <- Sys.getenv("FROST_USER_URL")
client_id <- Sys.getenv("FROST_CLIENT_ID")
client_secret <- Sys.getenv("FROST_CLIENT_SECRET")
username <- Sys.getenv("FROST_USERNAME")
password <- Sys.getenv("FROST_PASSWORD")
keycloak_token <- get_kc_token(base_url, client_id, client_secret, username, password)

# Set query arguments
years <- sapply(cseason, lubridate::year)
start_date <- paste0(years[1], "-01-01")
end_date <- paste0(years[2], "-12-31")
end_date <- if (Sys.Date() <= end_date) cseason[2] else end_date
wth_parameters = c("air_temperature","volume_of_hydrological_precipitation")
longitude = 10.645269
latitude = 49.20868

# Data extraction and mapping
wth_field_allstations_icasa <- extract_iot(
  url = user_url,
  token = keycloak_token,
  var = wth_parameters,
  lon = longitude,
  lat = latitude, 
  radius = 100,
  from = start_date,
  to = end_date,
  raw = FALSE,
  merge_ds = TRUE
)
names(wth_field_allstations_icasa)  # 3 weather stations found

# Average data over all three stations
wth_field_icasa <- wth_field_allstations_icasa %>%
  map(~ .x$WEATHER_DAILY) %>%
  bind_rows() %>%
  group_by(weather_date) %>%
  summarise(across(
    .cols = where(is.numeric), 
    .fns = ~mean(.x, na.rm = TRUE)
  ))
wth_field_metadata_merged <- wth_field_allstations_icasa %>%
  map(~ .x$WEATHER_METADATA) %>%
  bind_rows() %>%
  summarise(across(where(is.numeric), \(x) mean(x, na.rm = TRUE))) %>%
  distinct()

weather_field_icasa <- list(
  WEATHER_DAILY = wth_field_icasa,
  WEATHER_METADATA = wth_field_metadata_merged
)

# Check coverage of field weather data
c(min(weather_field_icasa$WEATHER_DAILY$weather_date),
  max(weather_field_icasa$WEATHER_DAILY$weather_date))  # Only one month of data!

# Get complementary weather source
weather_np_icasa <- get_weather(lon = longitude,
                                lat = latitude,
                                pars = c("air_temperature", "precipitation", "solar_radiation"),
                                res = "daily",
                                from = start_date,
                                to = end_date,
                                src = "nasa-power",
                                raw = FALSE)

# Assemble composite dataset
weather_icasa <- list()
weather_icasa$WEATHER_METADATA <- build_composite_data(
  list(weather_field_icasa$WEATHER_METADATA, weather_np_icasa$WEATHER_METADATA),
  groups = c("weather_station_id", "weather_station_name"),
  action = "coalesce"
)
weather_icasa$WEATHER_DAILY <- build_composite_data(
  list(weather_field_icasa$WEATHER_DAILY, weather_np_icasa$WEATHER_DAILY),
  groups = c("weather_station_id", "weather_station_name", "weather_date"),
  action = "coalesce"
)


###----- Soil profile data - external database --------------------------------------
## ----------------------------------------------------------------------------------
## This section demonstrates the sourcing of soil profile data from a a published
## dataset (Global 10-km Soil Grids DSSAT profiles). The data is first downloaded
## with the Dataverse API, and the closest profile to the set of input coordinates
## extracted. As the dataset already provides ready-to-use DSSAT formats, no
## mapping step is necesary here. 
## Data reference: https://doi.org/10.7910/DVN/1PEEY0
##
## ----------------------------------------------------------------------------------

#####----- Extract profile data from SoilGrids --------------------------------------
soil_icasa <- get_soilGrids_profile(lon = lon, lat = lat)


###----- Data integration and mapping to DSSAT --------------------------------------
## ----------------------------------------------------------------------------------
## xxxxxx
##
## ----------------------------------------------------------------------------------

# Merge all datasets
dataset_icasa <- c(mngt_obs_icasa, weather_icasa, soil_icasa)
# UPDATE TEMPLATE NAME OUTPUTS W/ ICASA MAP

# Map to DSSAT
dataset_dssat <- convert_dataset(
  dataset = dataset_icasa,
  input_model = "icasa",
  output_model = "dssat"
)

###----- Perform simulations --------------------------------------------------------
## ----------------------------------------------------------------------------------
## Compiles DSSAT input files from all the different data sections (weather, soil,
## crop management, cultivar parameters, measured data) and set controls for the
## simulations (DSSAT documentation ###).
## Simulation are performed using the written inputs
## 
## NB: this part require a local installation of DSSATCSM
## (default installation dir: "C:/DSSAT48")
##
## ----------------------------------------------------------------------------------

###----- Compile crop modeling data -------------------------------------------------

# test runs and corrections
dataset_dssat$MANAGEMENT$PLANTING_DETAILS$PLDP <- 2
dataset_dssat$MANAGEMENT$PLANTING_DETAILS$PLME <- "S"
dataset_dssat$MANAGEMENT$TILLAGE$TDEP <- 15
dataset_dssat$MANAGEMENT$FERTILIZERS$FDEP <- 2
dataset_dssat$MANAGEMENT$CULTIVARS$CNAME <- "Lely"
dataset_dssat$MANAGEMENT$CULTIVARS$INGENO <- "IB0003"
dataset_dssat$MANAGEMENT$FERTILIZERS <-
  dataset_dssat$MANAGEMENT$FERTILIZERS %>%
  mutate(FDATE = ifelse(is.na(FDATE), "25140", FDATE)) # 20 May 2025

# from comparing with KSAS file
dataset_dssat$MANAGEMENT$PLANTING_DETAILS$PPOE <- 250
dataset_dssat$MANAGEMENT$INITIAL_CONDITIONS$ICRT <- 1200
dataset_dssat$MANAGEMENT$INITIAL_CONDITIONS$ICND <- 0
dataset_dssat$MANAGEMENT$INITIAL_CONDITIONS$ICRN <- 1
dataset_dssat$MANAGEMENT$INITIAL_CONDITIONS$ICRE <- 1
dataset_dssat$MANAGEMENT$INITIAL_CONDITIONS$ICRES <- 6500
dataset_dssat$MANAGEMENT$INITIAL_CONDITIONS$ICREN <- 1.14
dataset_dssat$MANAGEMENT$INITIAL_CONDITIONS$ICREP <- 0
dataset_dssat$MANAGEMENT$INITIAL_CONDITIONS$ICRIP <- 100
dataset_dssat$MANAGEMENT$INITIAL_CONDITIONS$ICRID <- 15

# Assemble full input dataset
dataset_dssat_input <- compile_model_dataset(
  dataset = dataset_dssat,
  framework = "dssat",
  sol_append = FALSE,
  write = TRUE, write_in_dssat_dir = TRUE,
  args = list(RSEED = 4250, SMODEL = "WHAPS",  # general
              WATER = "Y", NITRO = "Y", TILL = "Y",  # options
              PHOTO = "C", MESEV = "S", # methods
              FERTI = "R", HARVS = "R",  # management
              GROUT = "Y", VBOSE = "Y")  # output
)


###----- Run DSSAT simulation ------------------------------------------------------

setwd(old_wd)
output_directory <- paste0(getwd(), "/simulations")

sims <- run_simulations(filex_path = "C:/DSSAT48/Wheat/HWOC2501.WHX",
                        treatments = 1:3,
                        framework = "dssat",
                        dssat_dir = NULL,
                        sim_dir = "./inst/extdata/test_fixtures")


###----- Plot output ---------------------------------------------------------------
plot_output(sims)


