% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/format_dssat.R
\name{split_by_year}
\alias{split_by_year}
\title{Split a multiyear crop experiment dataset by Year and Reset Management IDs}
\usage{
split_by_year(ls)
}
\arguments{
\item{ls}{A named list of data frames, typically representing sections of an experimental dataset.}
}
\value{
A named list of lists, each corresponding to a year (e.g., "Y2020"), containing the split and cleaned data frames for that year.
}
\description{
Splits each data frame in a list by the "Year" column, removes empty and all-NA columns, and resets management IDs for each year.
}
\details{
The function:
\itemize{
\item Removes metadata sections common to all years.
\item Splits each remaining data frame by the "Year" column.
\item Removes empty data frames and columns with only NA values.
\item Removes summary or time series files with only minimal columns.
\item Resets management IDs in the treatment matrix and management data frames for each year, using \code{reset_id}.
}
The result is a list of lists, one per year, each containing the relevant data frames for that year with cleaned and reset management IDs.

The function uses the helper functions \code{revert_list_str} and \code{reset_id}.
}
\examples{
\dontrun{
yearly_data <- split_by_year(ls)
names(yearly_data)
}

}
