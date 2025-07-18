% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/get_dssat_keys.R
\name{get_icasa}
\alias{get_icasa}
\title{Download and Load the ICASA Data Dictionary}
\usage{
get_icasa()
}
\value{
A named list of data frames, each corresponding to a sheet in the ICASA Data Dictionary (excluding the first sheet).
}
\description{
Downloads the official ICASA Data Dictionary Excel file from the DSSAT GitHub repository and loads its sheets as data frames.
}
\details{
The function downloads the ICASA Data Dictionary from the official DSSAT GitHub repository and loads each worksheet (except the first) into a list of data frames. The list is named according to the sheet names in the workbook.

The function uses \code{wb_load} and \code{wb_to_df} for reading the Excel file and converting sheets to data frames.
}
\examples{
\dontrun{
icasa_dict <- get_icasa()
names(icasa_dict)
}

}
