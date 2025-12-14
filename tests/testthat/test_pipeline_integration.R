file <- system.file("examples/sciwin/PlantGro.OUT", package = "csmTools")

# we do expect the pipeline to run before the tests
# reason: the pipeline runs ~20min and writes multiple files
# devtools::check() or R CMD check . would trigger the pipeline each build...

source("../helpers.R")

test_that("pipeline_has_run", {
    expect_true(file.exists(file))
})

test_that("pipeline_file_is_valid", {
    output <- DSSAT::read_output(file_name = file)
    message(readLines(file)[3])
    expect_no_error(output)
})

# test expect_equal_by_last_digit helper function
test_that("Values differ only in the last digit", {
    expect_equal_by_last_digit(7446, 7445)
    expect_equal_by_last_digit(530.9, 531.0)
    expect_equal_by_last_digit(1.934, 1.933)
    expect_equal_by_last_digit(1.0001, 1.0002)
    expect_equal_by_last_digit(1.000000001, 1.000000002)
    expect_equal_by_last_digit(0.018, 0.019)
})

test_that("PlantGro.OUT is plausible", {
    valid <- system.file("extdata/test_fixtures/PlantGro.OUT", package = "csmTools")

    expected <- DSSAT::read_output(file_name = valid)
    actual <- DSSAT::read_output(file_name = file)

    for (i in 1:nrow(expected)) {
        for (j in 1:ncol(expected)) {
            if (is_numeric(expected[i, j]) && is_numeric(actual[i, j])) {
                expect_equal_by_last_digit(as.numeric(actual[i, j]), as.numeric(expected[i, j]))
            } else {
                expect_equal(expected[i, j], actual[i, j])
            }
        }
    }
})
