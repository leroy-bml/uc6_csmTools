% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/map_data.R
\name{map_codes}
\alias{map_codes}
\title{Map Categorical Data to Standard Codes}
\usage{
map_codes(df, map, direction, ...)
}
\arguments{
\item{df}{A data frame containing the data to be mapped.}

\item{map}{A lookup table of categories to be mapped to standard codes,
as generated by \code{make_code_lookup}.}

\item{direction}{Character. Direction of the mapping:
\code{"to_icasa"} (to standard ICASA codes) or
\code{"from_icasa"} (from standard ICASA codes).}

\item{...}{Additional arguments (currently unused).}
}
\value{
A data frame with codes mapped to the format specified in the supplied map.
}
\description{
Maps categorical variables in a data frame to standard codes using a lookup table.
Currently supports lookup tables as data frames; support for JSON SC2 files may be added in the future.
}
