#' DSSAT table templates
#' 
#' @importFrom dplyr rename
#' 
#' 

# File X template ---------------------------------------------------------


# General
GENERAL_template <-
  data.frame(
    PEOPLE = NA_character_, ADDRESS = NA_character_,
    SITE = NA_character_,
    PAREA = NA_real_, PRNO = NA_real_, PLEN = NA_real_,
    PLDR = NA_real_, PLSP = NA_real_,
    PLAY = NA_character_, HAREA = NA_real_,
    HRNO = NA_real_, HLEN = NA_real_,
    HARM = NA_character_)

GENERAL_template$NOTES <- list(NA_character_)


# Treatments
TREATMENTS_template <-
  data.frame(
    N = 1, R = 1, O = 0, C = 0, TNAME = "",
    CU = 1, FL = 1, SA = 0, IC = 0, MP = 1,
    MI = 0, MF = 0, MR = 0, MC = 0, MT = 0,
    ME = 0, MH = 0, SM = 1
  )


# Cultivars
CULTIVARS_template <-
  data.frame(
    C = 1, CR = "FA", INGENO = "IB0001", CNAME = ""
  )


# Fields
FIELDS_template <-
  data.frame(
    L = 1,
    ID_FIELD = NA_character_, WSTA = NA_character_, FLSA = NA_character_,
    FLOB = NA_real_, FLDT = NA_character_, FLDD = NA_real_,
    FLDS = NA_real_, FLST = NA_character_,
    SLTX = NA_character_, SLDP = NA_real_, ID_SOIL = NA_character_,
    FLNAME = NA_character_,
    XCRD = NA_real_, YCRD = NA_real_, ELEV = NA_real_,
    AREA = NA_real_, SLEN = NA_real_, FLWR = NA_real_,
    SLAS = NA_real_, FLHST = NA_character_,
    FHDUR = NA_real_
  )


# Soil analysis
SOIL_ANALYSIS_template <-
  data.frame(
    A = 1, SADAT = NA_character_, SMHB = NA_character_, SMPX = NA_character_, SMKE = NA_character_,
    SANAME = NA_character_
  )

SOIL_ANALYSIS_template$SABL <- list(NA_real_)
SOIL_ANALYSIS_template$SADM <- list(NA_real_)
SOIL_ANALYSIS_template$SAOC <- list(NA_real_)
SOIL_ANALYSIS_template$SANI <- list(NA_real_)
SOIL_ANALYSIS_template$SAPHW <- list(NA_real_)
SOIL_ANALYSIS_template$SAPHB <- list(NA_real_)
SOIL_ANALYSIS_template$SAPX <- list(NA_real_)
SOIL_ANALYSIS_template$SAKE <- list(NA_real_)
SOIL_ANALYSIS_template$SASC <- list(NA_real_)


# Initial conditions
INITIAL_CONDITIONS_template <-
  data.frame(
    C = 1, PCR = NA_character_, ICDAT = NA_character_, ICRT = NA_real_, ICND = NA_real_,
    ICRN = NA_real_, ICRE = NA_real_, ICWD = NA_real_, ICRES = NA_real_,
    ICREN = NA_real_, ICREP = NA_real_, ICRIP = NA_real_, ICRID = NA_real_,
    ICNAME = NA_character_
  )

INITIAL_CONDITIONS_template$ICBL <- list(NA_real_)
INITIAL_CONDITIONS_template$SH2O <- list(NA_real_)
INITIAL_CONDITIONS_template$SNH4 <- list(NA_real_)
INITIAL_CONDITIONS_template$SNO3 <- list(NA_real_)


# Planting details
PLANTING_DETAILS_template <- data.frame(P = 1,
                                        PDATE = NA_character_,
                                        EDATE = NA_character_,
                                        PPOP = NA_real_,
                                        PPOE = NA_real_,
                                        PLME = NA_character_,
                                        PLDS = NA_character_,
                                        PLRS = NA_real_,
                                        PLRD = NA_real_,
                                        PLDP = NA_real_,
                                        PLWT = NA_real_,
                                        PAGE = NA_real_,
                                        PENV = NA_real_,
                                        PLPH = NA_real_,
                                        SPRL = NA_real_,
                                        PLNAME = NA_character_)


# Tillage
TILLAGE_template <-
  data.frame(
    T = 1, TDATE = NA_character_, TIMPL = NA_character_, TDEP = NA_real_,
    TNAME = NA_character_
  )


# Irrigation
IRRIGATION_template <-
  data.frame(
    I = 1, EFIR = NA_real_, IDEP = NA_real_, ITHR = NA_real_,
    IEPT = NA_real_, IOFF = NA_character_, IAME = NA_character_, IAMT = NA_real_,
    IRNAME = NA_character_
  )

IRRIGATION_template$IDATE <- list(NA_character_)
IRRIGATION_template$IROP <- list(NA_character_)
IRRIGATION_template$IRVAL <- list(NA_real_)


# Fertilizers
FERTILIZERS_template <-
  data.frame(
    F = 1, FDATE = NA_character_, FMCD = NA_character_, FACD = NA_character_,
    FDEP = NA_real_, FAMN = NA_real_, FAMP = NA_real_, FAMK = NA_real_,
    FAMC = NA_real_, FAMO = NA_real_, FOCD = NA_character_,
    FERNAME = NA_character_
  )


# Residues
RESIDUES_template <-
  data.frame(
    R = 1, RDATE = NA_character_, RCOD = NA_character_, RAMT = NA_real_,
    RESN = NA_real_, RESP = NA_real_, RESK = NA_real_, RINP = NA_real_,
    RDEP = NA_real_, RMET = NA_character_, RENAME = NA_character_
  )


# Chemicals
CHEMICALS_template <-
  data.frame(
    C = 1, CDATE = NA_character_, CHCOD = NA_character_, CHAMT = NA_real_,
    CHME = NA_character_, CHDEP = NA_real_, CHT = NA_character_,
    CHNAME = NA_character_
  )


# Environment modificatuions
ENVIRONMENT_MODIFICATIONS_template <-
  data.frame(
    E = 1, ODATE = NA_character_, EDAY = NA_real_, ERAD = NA_real_,
    EMAX = NA_real_, EMIN = NA_real_, ERAIN = NA_real_, ECO2 = NA_real_,
    EDEW = NA_real_, EWIND = NA_real_,
    ENVNAME = NA_character_
  )


# Harvest
HARVEST_template <-
  data.frame(
    H = 1, HDATE = NA_character_, HSTG = NA_character_, HCOM = NA_character_,
    HSIZE = NA_character_, HPC = NA_real_, HBPC = NA_real_,
    HNAME = NA_character_
  )


# Simulation controls
SIMULATION_CONTROLS_template <-
  data.frame(
    N = 1,
    GENERAL = "GE",
    NYERS = 1, NREPS = 1, START = "S",
    SDATE = NA_character_, RSEED = 2150,
    SNAME = NA_character_, SMODEL = NA_character_,
    OPTIONS = "OP",
    WATER = "N", NITRO = "N", SYMBI = "N", PHOSP = "N",
    POTAS = "N", DISES = "N", CHEM = "N", TILL = "N",
    CO2 = "M",
    METHODS = "ME",
    WTHER = "M", INCON = "M", LIGHT = "E", EVAPO = "R",
    INFIL = "S", PHOTO = "L", HYDRO = "R", NSWIT = 1,
    MESOM = "G", MESEV = "R", MESOL = 2,
    MANAGEMENT = "MA",
    PLANT = "R", IRRIG = "N", FERTI = "N", RESID = "N",
    HARVS = "M",
    OUTPUTS = "OU",
    FNAME = "N", OVVEW = "Y", SUMRY = "Y", FROPT = 1,
    GROUT = "N", CAOUT = "N", WAOUT = "N", NIOUT = "N",
    MIOUT = "N", DIOUT = "N", VBOSE = "N", CHOUT = "N",
    OPOUT = "N", FMOPT = "A",
    PLANTING = "PL",
    PFRST = NA_character_, PLAST = NA_character_,
    PH2OL = NA_real_, PH2OU = NA_real_,
    PH2OD = NA_real_, PSTMX = NA_real_, PSTMN = NA_real_,
    IRRIGATION = "IR",
    IMDEP = NA_real_, ITHRL = NA_real_,
    ITHRU = NA_real_, IROFF = NA_character_, IMETH = NA_character_,
    IRAMT = NA_real_, IREFF = NA_real_,
    NITROGEN = "NI",
    NMDEP = NA_real_, NMTHR = NA_real_, NAMNT = NA_real_,
    NCODE = NA_character_, NAOFF = NA_character_,
    RESIDUES = "RE",
    RIPCN = NA_real_, RTIME = NA_real_,
    RIDEP = NA_real_,
    HARVEST = "HA",
    HFRST = NA_character_, HLAST = NA_character_,
    HPCNP = NA_real_, HPCNR = NA_real_,
    SIMDATES = "SI",
    ENDAT = NA_character_, SDUR = NA_real_, FODAT = NA_character_,
    FSTRYR = NA_real_, FENDYR = NA_real_, FWFILE = NA_character_,
    FONAME = NA_character_
  )


# Assemble all sections
# FILEX_template <- list(
#   GENERAL = GENERAL_template,
#   TREATMENTS = TREATMENTS_template,
#   CULTIVARS = CULTIVARS_template,
#   FIELDS = FIELDS_template,
#   SOIL_ANALYSIS = SOIL_ANALYSIS_template,
#   INITIAL_CONDITIONS = INITIAL_CONDITIONS_template,
#   PLANTING_DETAILS = PLANTING_DETAILS_template,
#   TILLAGE = TILLAGE_template,
#   IRRIGATION = IRRIGATION_template,
#   FERTILIZERS = FERTILIZERS_template,
#   RESIDUES = RESIDUES_template,
#   CHEMICALS = CHEMICALS_template,
#   ENVIRONMENT_MODIFICATIONS = ENVIRONMENT_MODIFICATIONS_template,
#   HARVEST = HARVEST_template,
#   SIMULATION_CONTROLS = SIMULATION_CONTROLS_template
# )

FILEX_template <- list(
  GENERAL = GENERAL_template,
  TREATMENTS = TREATMENTS_template,
  CULTIVARS = CULTIVARS_template,
  FIELDS = FIELDS_template,
  SOIL_ANALYSIS = SOIL_ANALYSIS_template,
  INITIAL_CONDITIONS = INITIAL_CONDITIONS_template,
  PLANTING_DETAILS = PLANTING_DETAILS_template,
  TILLAGE = TILLAGE_template,
  IRRIGATIONS = IRRIGATION_template,
  FERTILIZERS = FERTILIZERS_template,
  RESIDUES = RESIDUES_template,
  CHEMICALS = CHEMICALS_template,
  ENVIRONMENT_MODIFICATIONS = ENVIRONMENT_MODIFICATIONS_template,
  HARVEST = HARVEST_template,
  SIMULATION_CONTROLS = SIMULATION_CONTROLS_template
)


# SOL file template -------------------------------------------------------

SOIL_template <- 
  data.frame(
    PEDON = NA_character_, SOURCE = NA_character_, TEXTURE = NA_character_, DEPTH = NA_real_,
    DESCRIPTION = NA_character_, SITE = NA_character_, COUNTRY = NA_character_, LAT = NA_real_, LONG = NA_real_,
    `SCS FAMILY` = NA_character_, SCOM = NA_character_, SALB = NA_real_, SLU1 = NA_real_, SLDR = NA_real_, SLRO = NA_real_,
    SLNF = NA_real_, SLPF = NA_real_, SMHB = NA_character_, SMPX = NA_character_, SMKE = NA_character_
  )

SOIL_template$SLB <- list(NA_real_)
SOIL_template$SLMH <- list(NA_character_)
SOIL_template$SLLL <- list(NA_real_)
SOIL_template$SDUL <- list(NA_real_)
SOIL_template$SSAT <- list(NA_real_)
SOIL_template$SRGF <- list(NA_real_)
SOIL_template$SSKS <- list(NA_real_)
SOIL_template$SBDM <- list(NA_real_)
SOIL_template$SLOC <- list(NA_real_)
SOIL_template$SLCL <- list(NA_real_)
SOIL_template$SLSI <- list(NA_real_)
SOIL_template$SLCF <- list(NA_real_)
SOIL_template$SLNI <- list(NA_real_)
SOIL_template$SLHW <- list(NA_real_)
SOIL_template$SLHB <- list(NA_real_)
SOIL_template$SCEC <- list(NA_real_)
SOIL_template$SADC <- list(NA_real_)

SOIL_template <- rename(SOIL_template, `SCS FAMILY` = SCS.FAMILY)


# WTH file template -------------------------------------------------------

WEATHER_header_template <-
  data.frame(
    INSI = NA_character_, LAT = NA_real_, LONG = NA_real_, ELEV = NA_real_,
    TAV = NA_real_, AMP = NA_real_, REFHT = NA_real_, WNDHT = NA_real_
  )
# 
# WEATHER_template$DATE <- list(NA_character_)
# WEATHER_template$SRAD <- list(NA_real_)
# WEATHER_template$TMAX <- list(NA_real_)
# WEATHER_template$TMIN <- list(NA_real_)
# WEATHER_template$RAIN <- list(NA_real_)
# WEATHER_template$DEWP <- list(NA_real_)
# WEATHER_template$WIND <- list(NA_real_)
# WEATHER_template$PAR <- list(NA_real_)
# WEATHER_template$EVAP <- list(NA_real_)
# WEATHER_template$RHUM <- list(NA_real_)


WEATHER_template <-
  data.frame(
    DATE = NA_character_, SRAD = NA_real_, TMAX = NA_real_, TMIN = NA_real_,
    RAIN = NA_real_, DEWP = NA_real_, WIND = NA_real_,
    PAR = NA_real_, EVAP = NA_real_, RHUM = NA_real_
  )



# File A template ---------------------------------------------------------

# v_fmt_filea <- c(
#   TRNO = "%6.0f", ADAT = "%6s", BWAH = "%6.0f",
#   #CHTA = "##%6.0f", CNAA = "%##6.0f",
#   CNAM = "%6.1f",
#   #CWAA = "%##6.0f",
#   CWAH = "%6.0f",  # to confirm
#   CWAM = "%6.0f", `GN%M` = "%6.2f", GNAM = "%6.1f", `H#AM` = "%6.0f", `H#UM` = "%6.0f", HDAT = "%6s",
#   HWAHF = "%6.0f",  # to confirm
#   #HIAM = "%##6.0f",
#   HWAH = "%6.0f", HWAM = "%6.0f", HWUM = "%6.1f",  # to confirm
#   #`L#SM` = "%##6.0f",
#   LAIX = "%6.2f",
#   MDAT = "%6s", EDAT = "%6s",
#   #PWAM = "%##6.0f", RNAH = "%##6.0f",
#   SNAM = "%6.1f",
#   #`T#AM` = "%##6.0f",
#   TDAT = "%6s"
#   #TNAH = "%##6.0f", TWAH = "%##6.0f", `UN%H` = "%##6.0f", UNAM = "%##6.0f", UWAH = "%##6.0f", UYAH = "%##6.0f"
# )

SUMMARY_template <-
  data.frame(
    TRNO = NA_integer_
  )

v_fmt_filea <- c(
  TRNO = "%6.0f", ADAT = "%6s", BWAH = "%6.0f",
  CHTA = "##%6.0f", CNAA = "%6.0f",
  CNAM = "%6.1f",
  CWAA = "%6.0f",
  CWAH = "%6.0f",  # to confirm
  CWAM = "%6.0f", GWAM = "%6.0f", `GN%M` = "%6.2f", GNAM = "%6.1f", `H#AM` = "%6.0f", `H#UM` = "%6.0f", HDAT = "%6s",
  HWAHF = "%6.0f",  # to confirm
  HIAM = "%6.0f",
  HWAH = "%6.0f", HWAM = "%6.0f", HWUM = "%6.1f",  # to confirm
  `L#SM` = "%6.0f",
  LAIX = "%6.2f",
  MDAT = "%6s", EDAT = "%6s",
  PWAM = "%6.0f", RNAH = "%6.0f",
  SNAM = "%6.1f",
  `T#AM` = "%6.0f",
  TDAT = "%6s",
  TNAH = "%6.0f", TWAH = "%6.0f", `UN%H` = "%6.0f", UNAM = "%6.0f", UWAH = "%6.0f", UYAH = "%6.0f"
)



# File A template ---------------------------------------------------------

# v_fmt_filet <- c(
#   TRNO = "%6.0f", DATE = "%6s",
#   CDAD = "%6.0f",
#   #CHTD = "%##6.0f", `CN%D` = "%##6.0f", CNAD = "%##6.0f", CWAD = "%##6.0f", CWID = "%##6.0f",
#   #`G#AD` = "##%6.0f", `GN%D` = "%##6.0f", GNAD = "%##6.0f", `GP%D` = "%##6.0f", GPAD = "%##6.0f", GWAD = "%##6.0f",
#   #GWGD = "%##6.0f",
#   #HIAD = "%6.0f", HIPD = "%6.2f",
#   #`L#SD` = "%6.1f", `LN%D` = "%6s", `LP%D` = "%6.0f", LAID = "%6.0f", 
#   #LDAD = "%6.0f", LNAD = "%6.0f", LPAD = "%6.0f", LWAD = "%6.0f",
#   #NUPC = "%6.0f",
#   #`P#AD` = "%6.0f", PWAD = "%6.0f", PWDD = "%6.0f",
#   #RDPD = "%6.0f", `RN%D` = "%6.0f", RNAD = "%6.1f", RWAD = "%6.0f",
#   #SDAD = "##%6.0f", SHAD = "%##6.0f", SLAD = "%##6.0f", SNAD = "%##6.0f", SPAD = "%6.2f", SWAD = "%6.0f",
#   #`T#AD` = "%##6.0f", TWAD = "%##6.0f",
#   #UNAD = "%6s", UWAD = "%6s", UYAD = "%6s",
#   `VN%D` = "%##6.0f", VNAD = "%##6.0f"
# )

TIME_SERIES_template <-
  data.frame(
    TRNO = NA_integer_,
    DATE = NA_character_
  )

v_fmt_filet <- c(
  TRNO = "%6.0f", DATE = "%6s",
  CDAD = "%6.0f",
  CHTD = "%6.0f", `CN%D` = "%6.0f", CNAD = "%6.0f", CWAD = "%6.0f", CWID = "%6.0f",
  `G#AD` = "%6.0f", `GN%D` = "%6.0f", GNAD = "%6.0f", `GP%D` = "%6.0f", GPAD = "%6.0f", GWAD = "%6.0f",
  GWGD = "%6.0f",
  HIAD = "%6.0f", HIPD = "%6.2f", HWAD = "%6.0f",
  `L#SD` = "%6.1f", `LN%D` = "%6s", `LP%D` = "%6.0f", LAID = "%6.0f", 
  LDAD = "%6.0f", LNAD = "%6.0f", LPAD = "%6.0f", LWAD = "%6.0f",
  NUPC = "%6.0f",
  `P#AD` = "%6.0f", PWAD = "%6.0f", PWDD = "%6.0f",
  RDPD = "%6.0f", `RN%D` = "%6.0f", RNAD = "%6.1f", RWAD = "%6.0f",
  SDAD = "%6.0f", SHAD = "%6.0f", SLAD = "%6.0f", SNAD = "%6.0f", SPAD = "%6.2f", SWAD = "%6.0f",
  `T#AD` = "%6.0f", TWAD = "%6.0f",
  UNAD = "%6s", UWAD = "%6s", UYAD = "%6s",
  `VN%D` = "%6.0f", VNAD = "%6.0f"
)


# File X sections ---------------------------------------------------------


filex_sections <- c("GENERAL",
                    "TREATMENTS",
                    "CULTIVARS",
                    "FIELDS",
                    "SOIL_ANALYSES",
                    "INITIAL_CONDITIONS",
                    "PLANTING_DETAILS",
                    "TILLAGE",
                    "IRRIGATION",
                    "FERTILIZERS",
                    "RESIDUES",
                    "CHEMICALS",
                    "ENVIRONMENT_MODIFICATIONS",
                    "HARVEST",
                    "SIMULATION_CONTROLS")

