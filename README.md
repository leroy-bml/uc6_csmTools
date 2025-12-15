# csmTools
This R package provides a robust Extract, Transform, Load (ETL) pipeline prototype for agricultural research data, designed to align with FAIR (Findable, Accessible, Interoperable, Reusable) Research Data Management (RDM) principles. Developed as part of FAIRagro Use Case 6 (UC6), it offers a set of functions to integrate diverse data sources for crop modeling, specifically showcasing integration with DSSAT.

## Background
Crop experiment datasets are typically published as interlinked tables describing experimental design, environment, management events, and measured variables. These tables follow a relational structure, connected via identifier variables (primary/foreign keys) akin to a database schema.
Many crop simulation frameworks, such as DSSAT and APSIM, use standardized annotation conventions (variable names, units, relationships) that can be automatically mapped to a unified reference: the [ICASA data dictionary](https://github.com/DSSAT/ICASA-Dictionary). This dictionary enables cross-model comparability by providing a consistent vocabulary for crop experiment variables across diverse cropping systems.
The ICASA dictionary covers an extensive range of crop experiment variables for a variety of cropping systems. It follows a category structure analogous to common data structures of crop experiment datasets. However, because the relationships between different tables can be defined in multiple ways, crop experiment datasets must be reshaped to the standard input format to become usable as model inputs. ICASA is specifically designed for crop modelers, whose knowledge representations of agricultural data may subtly yet meaningfully differ from those of agronomists. These discrepancies make annotating field data to the ICASA format a complex and time-consuming process.
Furthermore, agricultural experiment datasets often contain critical gaps for process-based crop modeling. In practice, researchers frequently supplement missing data from alternative sources, and there is growing interest in integrating high-resolution field and remote sensing data streams into crop modeling workflows. However, each additional data source introduces new processing requirements, further increasing the burden of data preparation and harmonization.

## Purpose
csmTools aims to facilitate the ETL process for crop modelers by offering functions to source various data components, identify and reshape datasets, generate model inputs, perform simulations, and evaluate the quality of model inputs and outputs.

## Key Features & Functions
The package provides a suite of functions categorized by their role in the ETL pipeline:

### 1. Data Extraction & Acquisition
*   `get_field_data()`: Imports ICASA-compliant field (meta)data from an [ICASA data entry template](https://github.com/leroy-bml/csmInputTemplate).
*   `get_sensor_data()`: Fetches weather time series data from IoT sensor stations operating on OGC SensorThings API endpoints (e.g., [FROST server](https://github.com/FraunhoferIOSB/FROST-Server)). Requires API credentials.
*   `get_weather_data()`: Downloads complementary weather data from third-party databases (e.g., [NASA POWER](https://power.larc.nasa.gov/docs/)) to fill data gaps.
*   `get_soil_data()`: Extracts soil profile information from the global 10-km soil profile dataset for DSSAT crop models (https://doi.org/10.1016/j.envsoft.2019.05.012).

### 2. Data Transformation & Harmonization
*   `convert_dataset()`: Transforms datasets between different input and output models (e.g., `user` to `icasa`, `nasa-power` to `icasa`, `icasa` to `dssat`). The function uses a declarative approach, where mapping rules (defined in YAML configuration files) specify transformation sequences as a series of basic operations (e.g., joins, pivots, string manipulations, unit conversions). In addition to built-in mappings for popular data models (e.g., BonaRes LTE, ICASA, DSSAT, NASAPOWER, OGC SensorThings API), users can define custom YAML maps for their own data structures.
*   `normalize_soil_profile()`: Standardizes soil profile data by normalizing soil layers to a specified depth sequence, which can increase the performance of some models.
*   `estimate_initial_layers()`: Calculates initial soil conditions (e.g., percent available water, total N) for crop models based on whole-profile values for water and nitrogen; this function implements a sub-module from the XBuild module of DSSAT.

### 3. Data Integration & Assembly
*   `assemble_dataset()`: Merges and combines various data components into a unified dataset in various ways: append columns, aggregates rows, replace whole data section; this is the core function used to quickly integrate edits made to model input files and build an integrated dataset from different extracted data components.
*   `build_simulation_files()`: Generates adequate file formats compatible with the [R DSSAT parser](https://github.com/palderman/DSSAT) which is used to write DSSAT input files.

### 4. Crop Simulation
*   `run_simulations()`: Executes crop simulations using specified crop modeling frameworks (e.g., DSSAT) based on the generated input files. This is a wrapper around the 'run_dssat()' function from the [DSSAT package](https://github.com/palderman/DSSAT) that allows running the DSSAT CSM executable from within an R session. The function offers additional convenience by streamlining the creation of batch files, executing simulations, and loading the output into the R environment
The DSSAT program and all the associated documentation can be downloaded [here](https://dssat.net). It is recommended to use the default installation path (C:/DSSAT48/) as using custom paths may cause issues with running DSSAT from within R.

## Current version
The current version allows integrating data from the following sources:
*   OGC SensorThings API endpoints (e.g., [FROST server](https://github.com/FraunhoferIOSB/FROST-Server))
*   BonaRes long-term field experiments published on the BonaRes repository (German language only)
*   Soil profile from the global [10-km soil profile dataset for DSSAT](https://doi.org/10.1016/j.envsoft.2019.05.012) crop models.
*   Modeled weather time-series from [NASA POWER](https://power.larc.nasa.gov/docs/).
*   Any manually input agricultural field data using the [ICASA-Agro data entry template](https://github.com/leroy-bml/csmInputTemplate).
These data can then be modified, integrated, transformed into DSSAT inputs, and used in crop modeling applications.
All functions were designed for seamless integration into workflow managers such as [SciWIn](https://github.com/fairagro/sciwin), and can hence be integrated with scripts developed in different programming language as part of complex and fully reproducible workflows. See an example application [here](https://github.com/joemureithi/fairagroUC6-workflows). 

## Upcoming features
*   Integration with advanced weather data extraction interface [ClimData](https://github.com/Kaushikreddym/climdata) to provide access to diverse weather and climate data sources.
*   GLUE genotype parameter calibration wrapper to design custom calibration workflows (i.e., set calibration sequences and constraints) and streamline parameter fitting with [GLUE](https://github.com/DSSAT/glue).
*   Support for creating and editing DSSAT cultivar files (.CUL)
*   Crop modeling fitness-for-use scoring functions, based on [existing tools and community standards](https://doi.org/10.1016/j.envsoft.2015.05.009).
*   Plotting wrapper for rapid simulation diagnostics
*   Mapping of data products to publication-ready FAIR digital objects ([Annotated Research Context](https://arc-rdm.org))
*   APSIM support

## Installation
You can install the development version of `csmTools` directly from GitHub using `devtools`:

```R
# Install devtools if you haven't already
install.packages("devtools")
# Install the package
devtools::install_github("leroy-bml/csmTools")
```
Linux users may need to execute `install_requirements.sh` to install required apt packages. If you need to install DSSAT there is a `install_dssat.sh` to clone and build the package on Linux. Windows Users can just use DSSATs installer. 

### System Dependencies
*   **R Environment:** This package requires a functioning R installation.
*   **R Packages:** The package itself will list its R dependencies in the `DESCRIPTION` file. Additionally, for plotting results, `DSSAT`, `dplyr`, and `ggplot2` are typically used.
*   **DSSAT CSM:** To utilize the DSSAT simulation functionalities, a local installation of DSSAT CSM (e.g., `C:/DSSAT48` on Windows) is required.
*   **API Credentials:** For functions interacting with external APIs (e.g., `get_sensor_data` for FROST server), you will need to set specific environment variables for authentication. Examples include:
    *   `FROST_CLIENT_ID`
    *   `FROST_CLIENT_SECRET`
    *   `FROST_USERNAME`
    *   `FROST_PASSWORD`
    *   `FROST_USER_URL`
    It is recommended to store these securely (e.g., in an `.Renviron` file).

## Usage Example
A comprehensive example of how to use the package's functions in an end-to-end ETL and simulation pipeline is provided in `inst/examples/sciwin/workflow_experiment_ochsenwasen_sciwin.R`. This script demonstrates:
1.  **Extracting** crop management, sensor weather, complementary weather, soil, and phenology data.
2.  **Transforming** and mapping data to ICASA and then to DSSAT models.
3.  **Assembling** the final DSSAT input dataset.
4.  **Running** a DSSAT simulation.
5.  **Visualizing** the simulation results against observed data.
You can run this example after installing the package and setting up your environment variables:

```R
# After installing the package and setting up credentials
library(fairagroUC6.RDM) # Replace with your actual package name

# Locate the example script (adjust path if needed)
example_script_path <- system.file("examples", "sciwin", "example_pipeline_experiment.R", package = "fairagroUC6.RDM")
source(example_script_path)
```

You can also run the pipeline using Docker and the provided Dockerfile and later on copy files with `docker cp`.
```bash
docker build . -t uc6_image
docker run uc6_image --name uc6_container
docker start uc6_container
docker cp uc6_container:/uc6_csmTools/Rplots.pdf ./Rplots.pdf
```

## License
This project is open-sourced under the MIT License. See the `LICENSE` file for details.

## Contact
For questions, bug reports, or feedback, please contact Benjamin Leroy at [benjamin.leroy@tum.de](mailto:benjamin.leroy@tum.de).
