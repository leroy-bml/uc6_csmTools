## -------------------------------------------------------------------
## Script name: example_pipeline_experiment.R
## Purpose of script: showcasing etl pipeline prototype of FAIRagro 
## UC6 as an example of application of FAIR RDM
##
## Author: Benjamin Leroy
## Date Created: 2024-01-31
## Copyright (c) Benjamin Leroy, 2024
## Email: benjamin.leroy@tum.de
## -------------------------------------------------------------------
## Notes: data are at least partially simulated and do not represent
## the actual experiment results
## 
## -------------------------------------------------------------------

###----- IoT weather data --------------------------------------------
## -------------------------------------------------------------------
## This section showcase the acquisition of IoT sensor data (weather
## time series) from a weather station operating on the study field
## in Triesdord and stored on FROST server (OGC SensorThings API).
## The steps include (1) pulling the data; (2) transform and format
## to DSSAT input format.
## The 'extract' routine requires FROST server credentials.
##
## -------------------------------------------------------------------

# Parameters for extraction
url = user_url
token = get_kc_token(base_url, client_id, client_secret, username, password)  # input here your SensorHub credentials
var = c("air_temperature","rainfall","solar_radiation")
lon = 11.7240  #TODO: change to Triesdorf!
lat = 48.4017  #TODO: change to Triesdorf!
from = "2023-01-01"
to = "2023-12-31"

# Extract weather data from SensorHub
weather_icasa <- extract_iot(url, token, var, lon, lat, from, to, format = "icasa")
#' NB Jan 2025: currently only works on LRZ network; eduVPN or LAN connexion required

# Map to DSSAT format
weather_dssat <- remap(weather_icasa, input_model = "icasa", output_model = "dssat") 


###----- Crop management/manually input data (template) --------------
## -------------------------------------------------------------------
## This section demonstrates ###
##
## -------------------------------------------------------------------

# Parameters
template_path <- "C:/Users/bmlle/Documents/0_DATA/TUM/HEF/FAIRagro/2-UseCases/UC6_IntegratedModeling/Workflows/csmInputTemplate/template_icasa_vba.xlsm"
### TODO: set up submodule and change to relative path

# Extract template data
template_icasa <- extract_template(template_path, headers = "short")
uc6_icasa <- template_icasa$HWTD2401  # Focal dataset --> TODO: solve data inconsistencies in template 

# Map to DSSAT format
uc6_dssat <- remap(uc6_icasa, input_model = "icasa", output_model = "dssat")
#uc6_dssat <- format_metadata(uc6_dssat, data_model = "dssat", section = "management")

# Write files
# format_metadata(uc6_dssat, data_model = "dssat")


###----- Soil profile data - external database -----------------------
## -------------------------------------------------------------------
## This section demonstrates the sourcing of soil profile data from a
## a third-party dataset (Global 10-km Soil Grids DSSAT profiles).
## The data is first downloaded with the Dataverse API, and the closest
## profile to the set of input coordinates extracted.
## As the dataset already provides ready-to-use DSSAT formats, no
## mapping step is necesary here.
## Data reference: https://doi.org/10.7910/DVN/1PEEY0
##
## -------------------------------------------------------------------

#####----- Extract profile data from SoilGrids -----------------------
soil_dssat <- get_soilGrids_profile(lon = lon, lat = lat, format = "icasa")
#TODO: change to Triesdorf!

write_sol(soil_dssat, "tmp.SOL")


###----- Data requirement control ------------------------------------
## -------------------------------------------------------------------
## xxxxxx
##
## -------------------------------------------------------------------

mngt <- uc6_dssat[1:11]
obs <- uc6_dssat[12]
wth <- weather_dssat
soil <- soil_dssat
framework <- "dssat"
std <- "minimum"

check_data_requirements()





# Reanalysis weather + estimate phenology

###----- Genotype parameter fitting ----------------------------------
## -------------------------------------------------------------------
## xxxxxx
##
## -------------------------------------------------------------------

# Parameters for simulation
options(DSSAT.CSM = "C:\\DSSAT48\\DSCSM048.EXE")  # Path to DSSAT-CSM executable
xfile <- "####"
# # xtables <- read_filex("C:/DSSAT48/Wheat/KSAS8101.WHX")
cultivar <- "Campesino"  # Winter wheat
model <- "WHAPS"
minbound <- list(P1 = 400, P5 = 700, PHINT = 110, STMMX = 1, SLAP1 = 300)  # Fixed pars
maxbound <- list(P1 = 400, P5 = 700, PHINT = 110, STMMX = 1, SLAP1 = 300)  # Fixed pars
reps = 3
cores = 6

# Calibrate parameters 
genpars <- calibrate(
  xfile, cultivar, model, minbound = minbound, maxbound = maxbound, reps = 3, cores = 6
)


###----- Perform simulations -----------------------------------------
## -------------------------------------------------------------------
## Compiles DSSAT input files from all the different data sections
## (weather, soil, crop management, cultivar parameters, measured data)
## and set controls for the simulations (DSSAT documentation ###)
## Simulation are performed using the written inputs
## 
## NB: this part require a local installation of DSSATCSM
## (###)
##
## -------------------------------------------------------------------

###----- Compile crop modeling data ----------------------------------
dataset_dssat <- compile_model_dataset(
  framework = "dssat",
  filex = uc6_dssat, filea = uc6_dssat, filet = NULL,
  file_cul = NULL, file_sol = soil_dssat, file_wth = weather_dssat,
  write = FALSE, path = "./data",
  args = list(RSEED = 4250, SMODEL = "WHAPS",  # general
              WATER = "Y", NITRO = "Y", TILL = "Y",  # options
              PHOTO = "C", MESEV = "S", # methods
              FERTI = "R", HARVS = "R",  # management
              GROUT = "Y", VBOSE = "Y")  # output
)

###----- Run DSSAT simulation ----------------------------------------

output_directory <- paste0(getwd(), "/simulations")

sims <- run_simulations(dataset = dataset_dssat,  #options handling?
                        framework = "dssat",
                        dssat_dir = NULL,
                        sim_dir = output_directory)


