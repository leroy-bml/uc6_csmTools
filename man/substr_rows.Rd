% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/etl_utils.R
\name{substr_rows}
\alias{substr_rows}
\title{Find Rows Present in Only One of Two Data Frames}
\usage{
substr_rows(df1, df2)
}
\arguments{
\item{df1}{A data frame.}

\item{df2}{A data frame to compare with \code{df1}.}
}
\value{
A data frame containing the rows that are present in only one of the two input data frames.
}
\description{
Returns the rows that are present in either \code{df1} or \code{df2}, but not in both. This is equivalent to the symmetric difference of the two data frames by rows.
}
\details{
The function uses \code{anti_join} from the \strong{dplyr} package to find rows in \code{df1} that are not in \code{df2} and vice versa. It then combines these unique rows from both data frames using \code{bind_rows}. The result is a data frame containing all rows that are unique to either \code{df1} or \code{df2}.

Both data frames should have the same column structure for meaningful comparison.
}
\examples{
library(dplyr)
df1 <- data.frame(a = 1:3, b = c("x", "y", "z"))
df2 <- data.frame(a = 2:4, b = c("y", "z", "w"))
substr_rows(df1, df2)
# Returns rows with a = 1, b = "x" and a = 4, b = "w"

}
