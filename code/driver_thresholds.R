# Top-level code for execution of data request

suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(rlang))
suppressPackageStartupMessages(library(tibble))
suppressPackageStartupMessages(library(readr))
suppressPackageStartupMessages(library(stringr))
suppressPackageStartupMessages(library(tidyr))
suppressPackageStartupMessages(library(purrr))

# Need to do this for assignInNamespace to work
suppressPackageStartupMessages(library(dbplyr))

# Required for execution using Rscript
suppressPackageStartupMessages(library(methods))

#' Set up the execution environment
#'
#' The .load() function sources the R files needed to execute the query
#' and sets up the execution environment.  In particular, all of the base
#' framework files, as well as files inthe code_dir with names matching
#' `cohort_*.R` or `analyze_*.R` will be sourced.
#'
#' This function is usually run automatically when the `run.R` file is sourced
#' to execute the request.  It may also be executed manually during an
#' interactive session to re-source changed code or to re-establish a connection
#' to the database.
#'
#' **N.B.** You will almost never have to edit this function.
#'
#' @param here The name of the top-level directory for the request.  The default
#'   is `config('base_dir')` if the config function has been set up, or the
#'   global variable `base_dir` if not.
#'
#' @return The value of `here`.
#' @md
.load <- function(here = ifelse(typeof(get('config')) == 'closure',
                                config('base_dir'), base_dir)) {
  source(file.path(here, 'code', 'config.R'))
  source(file.path(here, 'code', 'req_info.R'))
  source(config('site_info'))
  source(file.path(here, config('subdirs')$code_dir, 'setup.R'))
  source(file.path(here, config('subdirs')$code_dir, 'codesets.R'))
  for (fn in list.files(file.path(here, config('subdirs')$code_dir),
                        'util_.+\\.R', full.names = TRUE))
    source(fn)
  for (fn in list.files(file.path(here, config('subdirs')$code_dir),
                        'cohort_.+\\.R', full.names = TRUE))
    source(fn)
  for (fn in list.files(file.path(here, config('subdirs')$code_dir),
                        'analyze_.+\\.R', full.names = TRUE))
    source(fn)
  source(file.path(here, config('subdirs')$code_dir, 'cohorts.R'))

  .env_setup()

  for (def in c('retain_intermediates', 'results_schema')) {
    if (is.na(config(def)))
      config(def, config(paste0('default_', def)))
  }

  here
}

#' Execute the request
#'
#' This function presumes the environment has been set up, and executes the
#' steps of the request.
#'
#' In addition to performing queries and analyses, the execution path in this
#' function should include periodic progress messages to the user, and logging
#' of intermediate totals and timing data through [append_sum()].
#'
#' This function is also typically executed automatically, but is separated from
#' the setup done in [.load()] to facilitate direct invocation during
#' development and debugging.
#'
#' @param base_dir The name of the top-level directory for the request.  The default
#'   is `config('base_dir')`, which should always be valid after execution of
#'   [.load()].
#'
#' @return The return value is dependent on the content of the request, but is
#'   typically a structure pointing to some or all of the retrieved data or
#'   analysis results.  The value is not used by the framework itself.
#' @md
.run  <- function(base_dir = config('base_dir')) {

  message('Starting execution with framework version ',
          config('framework_version'))

  rslt <- list()

  # Anomaly detection for thresholds ----
  # BMC ----
  message('BMC anomaly thresholds')
  rslt$bmc_thresh<-results_tbl('bmc_anom_pp')%>%distinct(check_type, check_name, site,
                                               lower_tail, upper_tail)%>%
    collect()%>%
    pivot_longer(cols=c(lower_tail, upper_tail),
                 names_to="threshold_operator",
                 values_to="threshold",
                 values_drop_na=TRUE)%>%
    mutate(threshold_operator=case_when(threshold_operator=='lower_tail'~'lt',
                                        threshold_operator=='upper_tail'~'gt'),
           check_name_app=paste0(check_name, '_rows'),
           application='rows')%>%
    filter(threshold_operator=='lt')

  # ECP ----
  message("ECP anomaly thresholds")
  rslt$ecp_thresh<-results_tbl('ecp_output_pp')%>%distinct(check_type, check_name, site,
                                               lower_tail, upper_tail)%>%
    pivot_longer(cols=c(lower_tail, upper_tail),
                 names_to="threshold_operator",
                 values_to="threshold",
                 values_drop_na=TRUE)%>%
    mutate(threshold_operator=case_when(threshold_operator=='lower_tail'~'lt',
                                        threshold_operator=='upper_tail'~'gt'),
           check_name_app=paste0(check_name, '_person'),
           application='person')%>%
    filter(threshold_operator=='lt')%>%
    collect()

  # default thresholds -----
  rslt$thresholds_standard<-format_default_thresholds(std_thresholds=read_codeset('threshold_limits','ccdcc'),
                                                      anom_thresholds=bind_rows(rslt$bmc_thresh,
                                                                                rslt$ecp_thresh))

  message('Finding previous thresholds')
  # Find n-1 thresholds for those that should be re-set or stop flagging
  rslt$redcap_prev <- .qual_tbl(name='dqa_issues_redcap',
                                schema='dqa_rox',
                                db=config('db_src_prev'))%>%
    mutate(threshold_operator=case_when(threshop=='greater than'~'gt',
                                        threshop=='less than'~'lt'),
           rc_finalflag=case_when(finalflag=='Continue to flag'~1L,
                                  finalflag=='Stop flagging'~2L,
                                  finalflag=='Continue flagging with new threshold'~3L,
                                  finalflag=='Other'~4L))%>%
    filter(rc_finalflag%in%c(2L,3L))%>%collect()

  # this is the table in the v55 schema, but starting in v57, point to thresholds_history instead
  rslt$thresholds_history<-.qual_tbl(name='thresholds_history_new',
                                     schema='dqa_rox',
                                     db=config('db_src_prev'))%>%collect()
  rslt$thresholds_this_version<-determine_thresholds(default_thresholds=rslt$thresholds_standard,
                                                     newset_thresholds=rslt$redcap_prev,
                                                     history_thresholds=rslt$thresholds_history)
  message('Creating table to track threshold versions')
  rslt$thresholds_history_new <- bind_rows(rslt$thresholds_this_version,
                                           rslt$thresholds_history)
  output_tbl(rslt$thresholds_history_new,
             name='thresholds_history')

  message('Create threshold table')
  rslt$thresholds_applied <- apply_thresholds(check_app_tbl=read_codeset('check_apps', col_types = 'cccccc'),
                                              threshold_tbl = rslt$thresholds_this_version)


  output_list_to_db(rslt$thresholds_applied,
                    append=FALSE)
  rslt$threshold_violations <- reduce(.x=rslt$thresholds_applied,
                                      .f=dplyr::bind_rows)%>%
    filter(violation&(is.na(rc_finalflag)|rc_finalflag!=2))

  copy_to_new(df=rslt$threshold_violations,
              name='threshold_tbl_violations',
              temporary=FALSE)

  message('Done.')

  invisible(rslt)

}

#' Set up and execute a data request
#'
#' This function encapsulates a "production" run of the data request.  It sets
#' up the environment, executes the request, and cleans up the environment.
#'
#' Typically, the `run.R` file calls run_request() when in a production mode.
#'
#' @param base_dir Path to the top of the data request files.  This is
#'   typically specified in `run.R`.
#'
#' @return The result of [.run()].
#' @md
run_request <- function(base_dir) {
  base_dir <- .load(base_dir)
  on.exit(.env_cleanup())
  .run(base_dir)
}
