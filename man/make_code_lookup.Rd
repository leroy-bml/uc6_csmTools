% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/map_data.R
\name{make_code_lookup}
\alias{make_code_lookup}
\title{Convert a Mapping Code List String to a Lookup Table}
\usage{
make_code_lookup(vec)
}
\arguments{
\item{vec}{A code mapping list stored as a string.
Format: \code{list('source1: target1', 'source2: target2')}}
}
\value{
A data frame with two columns:
\describe{
\item{source}{Original data values}
\item{target}{Standardized data values}
}
}
\description{
Converts a code mapping list, stored as a string, into a lookup table.
The string format is analogous to the \code{code_mapping} field in standard
ARDN SC2 JSON files. See:
\url{https://agmip.github.io/ARDN/Annotation_SC2.html}
}
