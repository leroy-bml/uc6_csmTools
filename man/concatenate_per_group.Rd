% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/extract_transform_template.R
\name{concatenate_per_group}
\alias{concatenate_per_group}
\title{Concatenate Non-Constant Columns by Group in a Data Frame}
\usage{
concatenate_per_group(df)
}
\arguments{
\item{df}{A data frame to process.}
}
\value{
A data frame grouped by the constant columns, with other columns concatenated as comma-separated strings.
}
\description{
Groups a data frame by columns with constant values and concatenates the remaining columns within each group.
}
\details{
The function identifies columns in \code{df} that have the same value in every row (constant columns). It then groups the data frame by these constant columns and, for each group, concatenates the values of the remaining columns into comma-separated strings.

This is useful for summarizing data where some columns are invariant within groups, and you want to aggregate the other columns as concatenated lists.

The function uses the \strong{dplyr} package for data manipulation.
}
\examples{
library(dplyr)
df <- data.frame(
  id = c(1, 1, 2, 2),
  value1 = c('a', 'b', 'c', 'd'),
  value2 = c('x', 'y', 'z', 'w')
)
concatenate_per_group(df)
# Returns a data frame with one row per id, and value1/value2 as comma-separated strings

}
