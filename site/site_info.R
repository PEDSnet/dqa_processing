#!/usr/bin/env Rscript

#' Site-specific information for data request execution.
#'
#' Please edit information as noted below.
#'
#' The information in this file describes site-specific practices for managing
#' data requests in general, such as connection information and organiation of
#' databases, and defaults for output handling.  These settings are not
#' typically request-specific, and will likely remain unchanged across multiple
#' requests targeting the same version of data.  As a result, you can often copy
#' this file from one request to another, or point multiple requests to a single
#' site_info.R file.
#'
#' @md
"site_info.R"

#' Your site's name.
#' For technical reasons, it should be lowercase.
#' @md
config('site', 'cc')

#' Code to establish a database connection at your site.
#'
#' The connection must be able to reach CDM data, the vocabularies,
#' and any result schemata needed.  The connection may be either a
#' dplyr-style src_foo() object or a DBI-style dbConnect()
#' object.
#'
#' A few notes:
#'
#' * You may find it convenient to use the
#'   (srcr)[https://cran.r-project.org/web/packages/srcr/index.html] package
#'   to abstract database connection information such as credentials and server
#'   names out of this file.
#' * If using Oracle, the following are required before loading ROracle, if
#'   these are not set in the global environment:
#'     * `Sys.setenv(TZ=Sys.timezone())`
#'     * `Sys.setenv(ORA_SDTZ=Sys.timezone())`
#'
#' @md
config('db_src', {
  require(srcr);
  default <- Sys.getenv('PEDSNET_DB_SRC_CONFIG_BASE', unset = NA)
  if (is.na(default) || nchar(default) == 0) default <- 'argos_pedsnet_current'
  srcr(default)
})
#' set up connection to database from previous cycle
#' if environmental variable PEDSNET_DB_SRC_CONFIG_BASE_PREV is set (either manually or in .Rprofile), calls the file as named, and if not, defaults to the name `config_dqa_prev`
config('db_src_prev', {
  require(srcr);
  default <- Sys.getenv('PEDSNET_DB_SRC_CONFIG_BASE_PREV', unset = NA)
  if (is.na(default) || nchar(default) == 0) default <- 'config_dqa_prev'
  srcr(default)
})

#' Name of the schema, if any, to be prepended to CDM fact table names.
#'
#' @details
#' If `NA`, no schema qualifier is added.
#' @md
config('cdm_schema', 'dcc_pedsnet')

#' Name of the schema, if any, to be prepended to vocabulary tables.
#'
#' @details
#' If `NA`, no schema qualifier is added.
#' @md
config('vocabulary_schema', 'vocabulary')

#' Name of the schema in which to create intermediate and results tables
#'
#' This value determines whether a schema name is added to names of
#' tables holding intermediate or final results.  If it is `NA`, no
#' explicit schema is used, and tables are created wherever the DBMS
#' places them.  It can be overridden by the request-specific setting in `run.R`.
#' @md
config('default_results_schema', NA)

#' Whether or not to keep intermediate tables
#'
#' This Boolean value determines whether tables holding codesets
#' or intermediate steps are retained after execution completes.  If
#' `FALSE`, they are created as temporary tables.  It can be overridden by the
#' request-specific setting in `run.R`.
#' @md
config('default_retain_intermediates', FALSE)

#' Names of standard tables used in queries.
#'
#' This list defines a simple mapping between the names that will be used in the
#' code for the request (the left-hand side of each assignment) and the actual
#' names of the tables in the database.  It is intended to allow for different
#' alphabetic casing, use of version-specific names, etc.
#'
#' Please edit only the right-hand side of each assignment.
#' Table names on the left must be lower-case; those on the right
#' must reflect naming conventions in the database.
#' @md
config('table_names',
       list(adt_occurrence = 'adt_occurrence',
            care_site = 'care_site',
            condition_era = 'condition_era',
            condition_occurrence = 'condition_occurrence',
            death = 'death',
            device_exposure = 'device_exposure',
            dose_era = 'dose_era',
            drug_era = 'drug_era',
            drug_exposure = 'drug_exposure',
            fact_relationship = 'fact_relationship',
            immunization = 'immunization',
            location = 'location',
            measurement = 'measurement',
            measurement_anthro = 'measurement_anthro',
            measurement_labs = 'measurement_labs',
            measurement_vitals = 'measurement_vitals',
            measurement_organism = 'measurement_organism',
            observation = 'observation',
            observation_period = 'observation_period',
            person = 'person',
            procedure_occurrence = 'procedure_occurrence',
            provider = 'provider',
            visit_occurrence = 'visit_occurrence',
            visit_payer = 'visit_payer',
            concept = 'concept',
            concept_ancestor = 'concept_ancestor',
            concept_relationship = 'concept_relationship'))

#> ##################### End of site-specific configuration
