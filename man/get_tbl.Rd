% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/identify_exp_design.R
\name{get_tbl}
\alias{get_tbl}
\title{Retrieve a Table from a List by Primary Key}
\usage{
get_tbl(db, pkey)
}
\arguments{
\item{db}{A list of data frames.}

\item{pkey}{The name of the primary key to match (as a string).}
}
\value{
The data frame from \code{db} whose primary key matches \code{pkey}.
}
\description{
Retrieves a data frame (table) from a list of data frames by matching its primary key.
}
\details{
The function uses \code{\link{get_pkeys}} (with \code{alternates = FALSE}) to determine the primary key of each data frame in the list, and returns the first data frame whose primary key matches \code{pkey}.
}
\examples{
# Example usage (assuming get_pkeys is defined):
# db <- list(
#   df1 = data.frame(ID = 1:3, Value = c('A', 'B', 'C')),
#   df2 = data.frame(Key = 1:3, Data = c('X', 'Y', 'Z'))
# )
# get_tbl(db, pkey = "ID")

}
\seealso{
\code{\link{get_pkeys}}
}
