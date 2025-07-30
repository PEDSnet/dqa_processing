#!/usr/bin/env Rscript

#' PEDSnet data request framework
#'
#' This file contains the request-specific configuration, and the starting point
#' for execution, of a standard PEDSnet data request.
#'
#' In order to execute this data request, please do the following:
#'
#'  1. Make sure your database and R environments meet the
#'     requirements for executing this code, as described in the
#'     `README.pdf` file distributed with the package.
#'  2. Insure the appropriate information for your site is in the
#'     `site_info.R` file designated below. You can do so by editing
#'     the distributed file directly, or by copying over it a `site_info.R`
#'     file from a prior request executed against the same CDM version;
#'     there is no data specific to this request in `site_info.R`.
#'  3. Edit the variables below to provide information about your
#'     execution of this request.
#'  4. Execute this file.
#'  5. Return the contents of the `results` directory to the PEDSnet
#'     DCC.
#'
#' Please note that in order to serve as a portable starting point for request
#' execution, this file contains a mix of configuration settings and executable
#' code relying on global varibles.  Please do not edit the content outside the
#' designated configuration settings unless you are familiar with the internals
#' of the standard request framework, and the potential for distant effects.
#' @md


#' Please edit: optional basename of the directory for this request package
#'
#' This basename should point to the top-level directory for the request
#' package, located under whereever you have set `PEDSNET_DATA_REQUEST_ROOT`.
#' At present, it is only used if you leave `base_dir` at the auto-detect
#' setting, which is STRONGLY recommended.
#'
#' For example, if you keep your data request work in a subdirectory named
#' "data_science" under your home directory, and now you're starting in on the
#' MAGICAL trial's feasibility data request, you would
#'
#'   1. Set PEDSNET_DATA_REQUEST_ROOT = "$HOME/data_science" in .Renviron
#'   2. Define req_basename <- 'magical_feasibility'
#'
#' If you always rely on running the package with the working directory set to
#' the package directory, you can skip this, at the cost of portability.
#' @md
req_basename <- 'dqa_processing'

#' Please don't edit: path to the top-level directory of this request package
#'
#' This path should point to the top-level directory of the unzipped
#' data request package.  It will be the parent directory of the
#' directory containing this file.
#'
#' By default, the program will attempt to determine where the package is
#' located, based on your setting of the environment variable
#' PEDSNET_DATA_REQUEST_ROOT (assuming you tend to keep your request work in a
#' common place). As a convenience, the root defaults to your home directory.
#'
#' For portability, it is STRONGLY recommended that you use the auto-detect
#' mechanism, but in cases of absolute need, you can replace the value below
#' with a simple path string.
#' @md
base_dir <- 'auto-detect'

if (base_dir == 'auto-detect')
    base_dir <-
      {
       preferred <- Sys.getenv('PEDSNET_DATA_REQUEST_ROOT',
                                unset = Sys.getenv('HOME', unset = NA))
        if (! is.na(preferred) && nchar(preferred) > 0 && ! is.na(req_basename))
          preferred <- file.path(preferred, req_basename)
        if (file.exists(file.path(preferred, 'site', 'run.R'))) {
          preferred
        }
        else {
            if (any(file.exists(c('run.R',
                                  file.path('..', 'site', 'run.R'))))) {
              file.path(getwd(), '..')
            } else {
              if (file.exists(file.path('site', 'run.R'))) {
                getwd()
              } else {
                stop("Can't find package base directory; ",
                     "set base_dir in site/run.R")
              }
            }
          }
      }

source(file.path(base_dir, 'code', 'config.R'))
config('base_dir', base_dir)

#' Please edit: local database configuration information
#'
#' The contents of this file describe how to connect to the CDM database to be
#' used for this data request.  By default, it will look for a file named
#' `site_info.R` in the same directory as this file.  Since the information
#' doesn't change for a given version of the CDM, you may elect to use a single
#' copy located elsewhere for multiple data requests.  You can also choose to
#' edit or copy over the version provided with the request package.
#' @md
config('site_info', file.path(base_dir, 'site', 'site_info.R'))

#' Please edit: Request-specific retention of intermediate tables
#'
#' This Boolean value can be used to override the `default_retain_intermediates`
#' setting from site_info.R.  If it is NA, the default is not overridden.
#' @md
config('retain_intermediates', NA)

#' Please edit: Request-specific results schema
#'
#' Specific schema to which you want intermediate tables to be written
#' during processing of the request.
#' If it is `NA`, the default value from site_info.R is used.
#' @md
config('results_schema', 'dqa_rox')


#' when processing a new site
#' set new_site_pp to TRUE if adding a new site to the results, FALSE if running against all sites
#' set results_schema_other to (string) name of schema containing additional site's data (only required if new_site_pp=TRUE)
config('new_site_pp',FALSE)
config('results_schema_other', NA)

#################################################################
#
# End of site-specific code.  Please do not edit below this point
#
#################################################################

#' Request-specific suffix for output
#'
#' Unique tag for non-temporary table names created during execution
#' of this query.  Replace default with ticket number or similar for a
#' more readable result.  Keep the tag short, given Oracle limits on
#' table name length.
#' @md
config('results_name_tag', NA)
config('local_name_tag', '_op_1510')

#' Execution mode
#'
#' The intended execution mode for the reuqest.  Recognized values are:
#'
#' * `development` - Development and testing still in progress
#' * `production` - Intended for results generation
#' * `distribution` - Production version for distribution to remote sites
#' @md
config('execution_mode', 'development')
#' Place to write results
#'
#' * `file` - Write output to CSV files in `results` directory
#' * TRUE - Write output to config('db_src')
#' * DBI data source - Write output to database
config('results_target',
       if (config('execution_mode') == 'distribution') 'file' else TRUE)
#' current_version should match the most recent db version
config('current_version','v58')
config('previous_version','v57')
#' Request-specific debug output for database operations
#'
#' This Boolean value specifies whether the query log should include
#' detailed information about execution of SQL queries in the database
#' @md
config('db_trace', TRUE)

#' Request-specific caching of loaded codesets
#'
#' This Boolean value specifies whether repeated attempts to load the same
#' codeset should use a cached value rather than reloading.  This yields a
#' potentially significant savings in memory and time.
#' @md
config('cache_enabled', config('execution_mode') != 'development')

source(file.path(base_dir, 'code', 'driver.R'))

if ( is.na(Sys.getenv('PEDSNET_SKIP_REQUEST_EXECUTION', unset = NA)) &&
     (config('execution_mode') != 'development' ||
      ! is.na(Sys.getenv('PEDSNET_FORCE_REQUEST_EXECUTION', unset = NA)))) {
  run_request(base_dir)
} else {
  .load(base_dir)
}
