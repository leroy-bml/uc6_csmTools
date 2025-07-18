% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/map_data.R
\name{map_data}
\alias{map_data}
\title{Map a Data Table to a Standard Format}
\usage{
map_data(
  df,
  input_model,
  output_model,
  map,
  keep_unmapped = TRUE,
  col_exempt = NULL
)
}
\arguments{
\item{df}{A data frame to be mapped.}

\item{input_model}{Character. Name of the input data model.}

\item{output_model}{Character. Name of the output (target) data model.}

\item{map}{A lookup table detailing input and target headers, units, and codes.}

\item{keep_unmapped}{Logical. Whether to keep unmapped variables in the output.
Defaults to \code{TRUE}.}

\item{col_exempt}{Character vector of column names to keep in the output if
\code{keep_unmapped = FALSE}.}
}
\value{
A data frame with headers, units, and codes mapped to the format specified in the map.
}
\description{
Maps a data frame's headers, units, and codes to a standard format using a mapping table.
Currently supports only exact matches for header names, units, and codes.
}
