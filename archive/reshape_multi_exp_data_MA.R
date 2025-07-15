
#---------------------------------------------
# 0 ·  Setup
#---------------------------------------------
key_map <- c(
  "Versuchsjahr"  = "Year",
  "Parzelle_ID"   = "Plot_id",
  "Wiederholung"  = "Rep_no",
  "Pruefglied_ID" = "Treatment_id"
)

rename_keys <- function(df, map) {
  hits <- intersect(names(map), names(df))
  if (length(hits))
    names(df)[match(hits, names(df))] <- map[hits]
  df
}

distinct_per_year <- function(col, df, yr = "Year") {
  tapply(df[[col]], df[[yr]], function(x) length(unique(x)))
}

## ---- 1. harmonise column names ----------------------------------------
db_h    <- lapply(mun_list, rename_keys, key_map)      # “h” = harmonised
DESIGN  <- rename_keys(mun_db$VERSUCHSAUFBAU, key_map)

cat("✓ names harmonised\n")
str(head(DESIGN))

## ─── Use the harmonised copy from now on ────────────────────────────
db        <- db_h
mother_tbl <- db[["VERSUCHSAUFBAU"]]

## ─────────────────── 1 · harmonise column names ────────────────────
##  (run this once, *before* you start working with DESIGN, DATA_tbls …)
## -------------------------------------------------------------------

key_map <- c(
  Versuchsjahr  = "Year",          # year of the trial
  Parzelle_ID   = "Plot_id",       # unique plot code
  Wiederholung  = "Rep_no",        # replicate number
  Pruefglied_ID = "Treatment_id"   # treatment code
)

rename_keys <- function(df, map = key_map) {
  # rename only the keys that really exist in the data-frame
  old <- intersect(names(map), names(df))
  if (length(old))
    names(df)[match(old, names(df))] <- map[old]
  df
}

distinct_per_year <- function(col, df, yr = "Year") {
  tapply(df[[col]], df[[yr]], function(x) length(unique(x)))
}

## ---- harmonise every table in the database ------------------------
db_h    <- lapply(mun_db, rename_keys)          # “h” = harmonised

## ---- and the mother table you pass separately ---------------------
DESIGN  <- rename_keys(mun_db$VERSUCHSAUFBAU)

cat("✓ names harmonised\n")
str(head(DESIGN))

## ---- 2. DESIGN basics --------------------------------------------------
YEARS_id <- "Year"
PLOTS_id <- "Plot_id"

if (!PLOTS_id %in% names(DESIGN)) {
  cand <- setdiff(names(DESIGN), YEARS_id)
  is_ok <- function(cl)
    all(tapply(DESIGN[[cl]], DESIGN[[YEARS_id]], anyDuplicated) == 0)
  hits <- cand[vapply(cand, is_ok, logical(1))]
  if (!length(hits))
    stop("No duplicate-free plot column found – set PLOTS_id manually.")
  PLOTS_id <- hits[1]
  cat("Auto-detected plot column →", PLOTS_id, "\n")
} else {
  cat("Plot column already present →", PLOTS_id, "\n")
}

plots_per_year <- distinct_per_year(PLOTS_id, DESIGN, YEARS_id)
PLOTS_n        <- max(plots_per_year)

print(plots_per_year)           # see the counts
cat("Max plots in any year :", PLOTS_n, "\n")

## ──────────────────────────────── STEP 3 ────────────────────────────────
##  Treatment information (column + source table)
## -----------------------------------------------------------------------
cat("\n── STEP 3 : treatment key ──────────────────────────────────────────\n")

TREATMENTS_id  <- "Treatment_id"
TREATMENTS_tbl <- DESIGN                                   # already present
cat("✓ found column", TREATMENTS_id, "in DESIGN (n =", nrow(TREATMENTS_tbl), "rows)\n")

## ---- 4. add Rep_no from PARZELLE  (fixed) -----------------------------
if ("PARZELLE" %in% names(db_h) &&
    all(c(PLOTS_id, "Rep_no") %in% names(db_h$PARZELLE))) {
  
  DESIGN <- DESIGN %>%
    # drop *any* existing Rep_no variants, not just the exact name
    dplyr::select(-dplyr::starts_with("Rep_no")) %>%
    dplyr::left_join(
      dplyr::select(db_h$PARZELLE,
                    !!rlang::sym(PLOTS_id),  # e.g. "Plot_id"
                    Rep_no),
      by = PLOTS_id
    )
  cat("✓ Rep_no merged from PARZELLE\n")
} else {
  cat("• No PARZELLE / Rep_no merge performed\n")
}

## quick check
cat("Columns now in DESIGN:\n")
print(names(DESIGN))

table_head <- head(DESIGN[, c(PLOTS_id, "Year", "Rep_no")], 8)
print(table_head)

## ---- 5. replicate summary ---------------------------------------------
have_rep <- "Rep_no" %in% names(DESIGN)
REPS_id  <- if (have_rep) "Rep_no" else NA_character_
REPS_n   <- if (have_rep) {
  max(table(DESIGN$Rep_no, DESIGN$Year))
} else if (!is.na(metadata$replication)) {
  as.numeric(metadata$replication)
} else {
  NA_real_
}

cat("Rep column :", REPS_id, "\n")
cat("Rep number :", REPS_n,  "\n")

## ──────────────────────────────── STEP 4 ────────────────────────────────
##  Build DESIGN_str  (DESIGN + plot + treatment, inc. Faktor columns)
## -----------------------------------------------------------------------
cat("\n── STEP 4 : DESIGN_str construction ───────────────────────────────\n")

DESIGN_str <- merge_tbls(
  list(DESIGN     = DESIGN),
  list(TREATMENTS = TREATMENTS_tbl,
       PLOTS      = PLOTS_tbl),
  type      = "child-parent",
  drop_keys = FALSE
)[[1]]

faktor_col <- grep("Faktor.*_ID$", names(DESIGN_str), value = TRUE)
std_keys   <- c("Year", "Plot_id", "Treatment_id", "Rep_no")
DESIGN_str <- dplyr::select(DESIGN_str, dplyr::any_of(c(std_keys, faktor_col)))

cat("✓ DESIGN_str ready  – rows:", nrow(DESIGN_str), "| cols:", ncol(DESIGN_str), "\n")

## ──────────────────────────────── STEP 5 ────────────────────────────────
##  Split remaining tables into DATA vs. ATTR
## -----------------------------------------------------------------------
cat("\n── STEP 5 : DATA / ATTR split ─────────────────────────────────────\n")

DESIGN_tbls_nms <- unlist(lapply(
  list(DESIGN, PLOTS_tbl, TREATMENTS_tbl),
  \(df) get_df_name(db, df)
))
other_tbls <- db[!names(db) %in% DESIGN_tbls_nms]
other_tbls <- other_tbls[order(names(other_tbls))]

has_design_link <- sapply(other_tbls,
                          \(df) has_link(df, DESIGN_str,
                                         subset = c("Year", "Plot_id", "Treatment_id")
                          )
)
DATA_tbls <- other_tbls[has_design_link]
ATTR_tbls <- other_tbls[!has_design_link]

cat("• DATA tables :", paste(names(DATA_tbls), collapse = ", "), "\n")
cat("• ATTR tables :", paste(names(ATTR_tbls), collapse = ", "), "\n")

## ── drop any DESIGN tables from the “other_tbls” list ─────────────────
design_tbls <- c(
  get_df_name(db_ipt, DESIGN_tbl),
  get_df_name(db_ipt, PLOTS_tbl),
  get_df_name(db_ipt, TREATMENTS_tbl)
)
other_tbls <- other_tbls[!names(other_tbls) %in% design_tbls]

## ── now split into DATA vs ATTR tables ────────────────────────────────
has_design_link <- sapply(other_tbls, function(df) {
  has_link(df, DESIGN_str, subset = c("Year", "Plot_id", "Treatment_id"))
})
DATA_tbls <- other_tbls[has_design_link]
ATTR_tbls <- other_tbls[!has_design_link]

## ── strip out Rep_no from every ATTR table so it never gets duplicated ─
ATTR_tbls <- lapply(ATTR_tbls, function(df) {
  if ("Rep_no" %in% names(df))
    df[, setdiff(names(df), "Rep_no"), drop = FALSE]
  else
    df
})

## ── STEP 5·1 : flatten attribute hierarchy ─────────────────────────────
repeat {
  # find “leaves” (tables with no children)
  ATTR_parents <- lapply(ATTR_tbls, get_parent, ATTR_tbls)
  ATTR_leaves  <- ATTR_tbls[sapply(ATTR_parents, is.null)]
  if (length(ATTR_leaves) == 0) break
  
  # merge each leaf into its parent set
  ATTR_children <- ATTR_tbls[!names(ATTR_tbls) %in% names(ATTR_leaves)]
  ATTR_merged   <- merge_tbls(
    ATTR_children, ATTR_leaves,
    type      = "child-parent",
    drop_keys = TRUE
  )
  
  # only overwrite the parents that already existed
  idx <- which(names(ATTR_tbls) %in% names(ATTR_merged))
  ATTR_tbls[idx] <- ATTR_merged
  
  # stop once nothing new can be merged
  ATTR_children <- ATTR_tbls[!names(ATTR_tbls) %in% names(ATTR_leaves)]
  if (length(ATTR_children) == 0) break
}
cat("✓ ATTR hierarchy flattened, tables remaining:", length(ATTR_tbls), "\n")

## 5·2  attach ATTR to DATA
DATA_tbls <- merge_tbls(
  DATA_tbls, ATTR_tbls,
  type = "bidirectional",
  drop_keys = TRUE
)
cat("✓ ATTR attached to DATA tables\n")

## ──────────────────────────────── STEP 6 ────────────────────────────────
##  Classify DATA tables (management / observed / other)
## -----------------------------------------------------------------------
cat("\n── STEP 6 : classify DATA tables ──────────────────────────────────\n")

DATA_tbls_ident <- tag_data_type(
  db         = DATA_tbls,
  years_col  = "Year",
  plots_col  = "Plot_id",
  plots_len  = PLOTS_n,
  max_events = 8
)

MNGT_ipt      <- DATA_tbls_ident[["management"]]
DOBS_suma_ipt <- DATA_tbls_ident[["observed_summary"]]
DOBS_tser_ipt <- DATA_tbls_ident[["observed_timeseries"]]
ATTR_ipt      <- DATA_tbls_ident[["other"]]

cat("• management      :", paste(names(MNGT_ipt),      collapse = ", "), "\n")
cat("• obs summary     :", paste(names(DOBS_suma_ipt), collapse = ", "), "\n")
cat("• obs timeseries  :", paste(names(DOBS_tser_ipt), collapse = ", "), "\n")
cat("• other           :", paste(names(ATTR_ipt),      collapse = ", "), "\n")

## ──────────────────────────────── STEP 7 ────────────────────────────────
##  Field table (FL_ID)
## -----------------------------------------------------------------------
cat("\n── STEP 7 : field table (FL_ID) ───────────────────────────────────\n")

PLOTS_all_ids <- get_pkeys(PLOTS_tbl, alternates = TRUE)
FIELDS_cols   <- setdiff(
  names(PLOTS_tbl),
  c(PLOTS_all_ids, std_keys)
)

FIELDS_tbl <- PLOTS_tbl %>%
  dplyr::group_by_at(FIELDS_cols) %>%
  dplyr::mutate(FL_ID = dplyr::cur_group_id()) %>%
  dplyr::ungroup() %>%
  dplyr::relocate(FL_ID)

FIELDS <- dplyr::distinct(
  dplyr::select(FIELDS_tbl, FL_ID, dplyr::all_of(FIELDS_cols))
)
if (!"FL_LON" %in% names(FIELDS)) FIELDS$FL_LON <- metadata$longitude
if (!"FL_LAT" %in% names(FIELDS)) FIELDS$FL_LAT <- metadata$latitude

cat("✓ FIELDS table built – rows:", nrow(FIELDS), "\n")

stopifnot("Year"    %in% names(DESIGN_str))
stopifnot("Plot_id" %in% names(DESIGN_str))

str(MNGT_ipt[[1]])
# look for Year, Plot_id, and Treatment_id

is_treatment <- function(df, years_col, plots_col) {
  # 1) if no plot-column supplied, default to a dummy
  if (is.null(plots_col)) {
    df$plots_id <- 1
    plots_col   <- names(df)[ncol(df)]
  }
  
  # 2) find the primary key(s) of this management table
  mngt_id <- get_pkeys(df, alternates = FALSE)
  
  # 3) try to pick out a real Date/POSIXt column
  date_cols_real <- names(df)[
    vapply(df, function(x) inherits(x, "Date") || inherits(x, "POSIXt"), logical(1))
  ]
  date_col <- date_cols_real[1]
  
  # 4) if none found, fall back to character–columns matching "YYYY-MM-DD"
  if (is.na(date_col) || date_col == "") {
    char_iso <- names(df)[
      vapply(df, is.character, logical(1)) &
        vapply(df, function(x) any(grepl("^\\d{4}-\\d{2}-\\d{2}", x)), logical(1))
    ]
    if (length(char_iso)) {
      date_col <- char_iso[1]
      # convert in-place
      df[[date_col]] <- as.Date(sub("T.*", "", df[[date_col]]))
    } else {
      stop("`is_treatment()`: no Date/POSIX nor ISO-style char date column found in management table.")
    }
  }
  
  # 5) everything else is identical to your prior logic
  management_cols <- setdiff(names(df), c(mngt_id, years_col, plots_col))
  
  out <- df %>%
    group_by_at(c(years_col, plots_col)) %>%
    arrange(.data[[date_col]]) %>%
    mutate(events_count = row_number()) %>%
    mutate(across(all_of(management_cols), as.character)) %>%
    rowwise() %>%
    mutate(mngt_full = paste(c_across(all_of(management_cols)), collapse = "|")) %>%
    ungroup() %>%
    select(all_of(c(years_col, plots_col)), events_count, mngt_full) %>%
    pivot_wider(
      names_from  = all_of(plots_col),
      values_from = mngt_full
    ) %>%
    rowwise() %>%
    mutate(unique_count = n_distinct(c_across(-all_of(c(years_col, "events_count"))))) %>%
    group_by_at(years_col) %>%
    summarise(is_trt = any(unique_count > 1), .groups = "drop") %>%
    as.data.frame()
  
  return(out)
}

cat("\n── STEP 7 : build management IsTreatment flags ──────────────────\n")
MNGT_is_trt <- lapply(
  MNGT_ipt,
  is_treatment,
  years_col = "Year",
  plots_col = "Plot_id"
)

## ──────────────────────────────── STEP 8 ────────────────────────────────
##  Management tables  (IDs + reduced forms)
## -----------------------------------------------------------------------
rlang::last_trace()
# Inspect the first few years / the flag
head(MNGT_is_trt[[1]])

MNGT_ipt <- mapply(
  \(df, tag) dplyr::left_join(df, tag, by = "Year"),
  MNGT_ipt, MNGT_is_trt,
  SIMPLIFY = FALSE
)

MNGT_fmt <- lapply(names(MNGT_ipt), function(nm) {
  df <- MNGT_ipt[[nm]]
  id_nm <- paste0(toupper(substr(nm, 1, 2)), "_ID")
  df %>%
    dplyr::left_join(DESIGN_str, by = c("Year", "Plot_id")) %>%
    dplyr::group_by(Year,
                    dplyr::across(dplyr::ends_with("Faktor_ID"))) %>%
    dplyr::mutate(tmp = dplyr::cur_group_id()) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(!!id_nm := tmp) %>%
    dplyr::select(-tmp, -dplyr::starts_with("Faktor")) %>%
    dplyr::relocate(!!id_nm)
})
names(MNGT_fmt) <- names(MNGT_ipt)

MNGT_out <- lapply(
  MNGT_fmt,
  \(df) dplyr::distinct(df[!names(df) %in% "Plot_id"])
)
names(MNGT_out) <- names(MNGT_fmt)

MNGT_ids <- lapply(MNGT_fmt, function(df) {
  df[names(df) %in% c(names(df)[1], "Year", "Plot_id")]
})

cat("✓ management IDs produced for", length(MNGT_ids), "tables\n")

## ──────────────────────────────── STEP 9 ────────────────────────────────
##  Treatment matrix
## -----------------------------------------------------------------------
cat("\n── STEP 9 : treatment matrix ──────────────────────────────────────\n")

join_col <- if ("Plot_id" %in% names(FIELDS_tbl)) "Plot_id" else PLOTS_id

TREATMENTS_matrix <-
  Reduce(
    \(x, y) merge(x, y,
                  by    = intersect(names(x), names(y)),
                  all.x = TRUE),
    MNGT_ids,
    init = DESIGN_str
  ) %>%
  dplyr::select(-dplyr::any_of("Rep_no")) %>%
  dplyr::distinct() %>%
  fix_plot_column(join_col, PLOTS_id) %>%
  dplyr::mutate(
    !!join_col := as.character(.data[[join_col]])
  ) %>%
  dplyr::left_join(
    FIELDS_tbl %>%
      dplyr::mutate(
        !!join_col := as.character(.data[[join_col]])
      ) %>%
      dplyr::select(FL_ID, !!rlang::sym(join_col)),
    by = join_col
  ) %>%
  dplyr::mutate(
    dplyr::across(
      dplyr::ends_with("_ID"),
      ~ ifelse(is.na(.x), 0, .x)
    )
  ) %>%
  dplyr::group_by(Treatment_id) %>%
  dplyr::mutate(TRTNO = dplyr::cur_group_id()) %>%
  dplyr::ungroup() %>%
  dplyr::relocate(TRTNO)

cat("✓ TREATMENTS matrix done  – rows:", nrow(TREATMENTS_matrix), "\n")

## ──────────────────────────────── STEP 10 ───────────────────────────────
##  Observed data  (summary + time-series)
## -----------------------------------------------------------------------
cat("\n── STEP 10 : observed data blocks ─────────────────────────────────\n")

drop_keys_tag <- \(df) dplyr::select(df, -dplyr::any_of(
  c(get_pkeys(df, FALSE), "tag")
))
DOB_suma_ipt <- lapply(DOBS_suma_ipt, drop_keys_tag)
DOB_tser_ipt <- lapply(DOBS_tser_ipt, drop_keys_tag)

base_keys  <- c(
  "Year", join_col, "Treatment_id",
  if ("Rep_no" %in% names(DESIGN_str)) "Rep_no"
)
init_frame <- dplyr::select(DESIGN_str, dplyr::all_of(base_keys))

library(tibble)   # for as_tibble()

merge_obs <- function(lst) {
  # 1) do the old Reduce(merge) to get a single data.frame
  df <- Reduce(
    function(x, y) merge(x, y,
                         by = c("Year", join_col),
                         all = TRUE),
    lst,
    init = init_frame
  )
  
  # 2) now convert to a tibble, letting it uniquify any duplicate names
  df <- as_tibble(df, .name_repair = "unique")
  
  # 3) carry on with your existing dplyr pipeline
  df %>%
    group_by(Treatment_id) %>%
    mutate(TRTNO = cur_group_id()) %>%
    ungroup() %>%
    relocate(TRTNO) %>%
    arrange(Year, TRTNO) %>%
    distinct()
}

DOB_suma_out <- merge_obs(DOBS_suma_ipt)
DOB_tser_out <- merge_obs(DOBS_tser_ipt)

cat("✓ Observed-summary rows :", nrow(DOB_suma_out),
    "| Observed-timeseries rows :", nrow(DOB_tser_out), "\n")

## ──────────────────────────────── STEP 11 ───────────────────────────────
##  Provenance / metadata  (GENERAL)
## -----------------------------------------------------------------------
cat("\n── STEP 11 : metadata & provenance ────────────────────────────────\n")

GENERAL <- data.frame(
  PERSONS = metadata$contact_name,
  EMAIL   = metadata$contact_email,
  ADDRESS = metadata$trial_institution,
  SITE    = metadata$site,
  COUNTRY = metadata$country,
  PLTA    = metadata$size_plots,
  DOI     = metadata$doi,
  NOTES   = paste0(
    "Mapped ", Sys.Date(),
    " with csmTools – Source DOI: ", metadata$doi
  ),
  stringsAsFactors = FALSE
)

cat("✓ GENERAL block created\n")

## ──────────────────────────────── STEP 12 ───────────────────────────────
##  Final assembly
## -----------------------------------------------------------------------
cat("\n── STEP 12 : assembling final ICASA list ──────────────────────────\n")

TREATMENTS_matrix <- dplyr::distinct(
  dplyr::select(
    TREATMENTS_matrix,
    -dplyr::any_of(c("Plot_id", "Treatment_id"))
  )
)

MANAGEMENT <- append(
  MNGT_out,
  list(GENERAL, FIELDS, TREATMENTS_matrix),
  after = 0
)
names(MANAGEMENT)[1:3] <- c("GENERAL", "FIELDS", "TREATMENTS")

OBSERVED <- list(
  Summary      = DOB_suma_out,
  Time_series  = DOB_tser_out
)

OTHER <- ATTR_ipt
names(OTHER) <- paste0("OTHER_", names(ATTR_ipt))

DATA_out <- append(
  MANAGEMENT,
  c(
    list(OBSERVED_Summary   = DOB_suma_out),
    list(OBSERVED_TimeSeries = DOB_tser_out),
    OTHER
  ),
  after = length(MANAGEMENT)
)

attr(DATA_out, "EXP_DETAILS") <- metadata$name
attr(DATA_out, "SITE_CODE")   <- toupper(
  paste0(substr(metadata$site, 1, 2),
         countrycode(metadata$country,
                     origin = "country.name",
                     destination = "iso2c"))
)

cat("✓ ICASA list completed – elements:", length(DATA_out), "\n")

## Return ------------------------------------------------------------------
DATA_out

# assume your final object is called DATA_out
cat("=== Sections in DATA_out ===\n")
print(names(DATA_out))
cat("\n")

summarize_section <- function(obj, name) {
  cat("----", name, "----\n")
  if (is.data.frame(obj)) {
    cat("  data.frame with", nrow(obj), "rows and", ncol(obj), "columns\n\n")
    cat("  head:\n")
    print(utils::head(obj, 3))
    cat("\n  summary:\n")
    print(summary(obj))
  } else {
    cat("  object of class:", class(obj), "\n")
    print(obj)
  }
  cat("\n")
}

invisible(
  lapply(names(DATA_out), function(nm) {
    summarize_section(DATA_out[[nm]], nm)
  })
)




