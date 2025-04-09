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
  # DC ------
  message('Changes between data cycles processing')
  rslt$dc_preprocess <- dc_preprocess(results='dc_output')

  # flag anomalies to limit the range of output values for visualization
  # NOT used as thresholds
  rslt$dc_anom<-compute_dist_anomalies(df_tbl=filter(rslt$dc_preprocess,site!='total'),
                                        grp_vars=c('check_type', 'application'),
                                        var_col='prop_total_change')
  rslt$dc_anom_pp<-detect_outliers(df_tbl=rslt$dc_anom,
                                    tail_input = 'both',
                                    p_input = 0.9,
                                    column_analysis = 'prop_total_change',
                                    column_eligible = 'analysis_eligible',
                                    column_variable = 'application')

  # for plotting
  rslt$dc_pp_plot<-dc_suppress_outlier(bind_rows(rslt$dc_anom_pp,
                                                 filter(rslt$dc_preprocess,site=='total')))

  copy_to_new(df=rslt$dc_pp_plot,
              name='dc_output_pp',
              temporary = FALSE)
  # VS ----
  message('Value set processing')
  # by vocabulary_id
  rslt$vs_pp<-vs_process('vs_output')
  output_tbl(rslt$vs_pp,
             name='vs_output_pp')
  # by check_name_app
  rslt$vs_violations_pp<-vc_vs_rollup(rslt$vs_pp)
  output_tbl(rslt$vs_violations_pp,
             name='vs_violations_pp')

  # VC ----
  message('Vocabulary conformance')
  rslt$vc_pp<-vc_process('vc_output')
  output_tbl(rslt$vc_pp,
             name='vc_output_pp')

  rslt$vc_violations_pp<-vc_vs_rollup(rslt$vc_pp)
  output_tbl(rslt$vc_violations_pp,
             name='vc_violations_pp')

  # UC ------
  message('Unmapped concepts processing')
  rslt$uc_preprocess <- uc_process(results='uc_output')
  copy_to_new(df=rslt$uc_preprocess,
              name='uc_output_pp',
              temporary = FALSE)

  rslt$uc_by_year_preprocess <- uc_by_year_preprocess(results='uc_by_year')
  copy_to_new(df=rslt$uc_by_year_preprocess,
              name='uc_by_year_pp',
              temporary = FALSE)
  rslt$uc_grpd_process <- results_tbl('uc_grpd')
  copy_to_new(df=rslt$uc_grpd_process,
              name='uc_grpd_pp',
              temporary=FALSE)

  # MF ------
  message('Missing field: visit id processing')
  rslt$mf_visitid_preprocess <- mf_visitid_preprocess(results='mf_visitid_output')
  copy_to_new(df=rslt$mf_visitid_preprocess,
              name='mf_visitid_pp',
              temporary = FALSE)

  # PF ------
  message('Person facts processing')
  rslt$pf_output_preprocess <- pf_output_preprocess(results='pf_output')
  copy_to_new(df=rslt$pf_output_preprocess,
              name='pf_output_pp',
              temporary = FALSE)

  # FOT ------
  message('Facts over time processing')
  rslt$fot_map <- read_codeset('fot_map','cc')
  output_tbl(rslt$fot_map,
             'fot_map',
             indexes=list('domain'))
  rslt$input_tbl <- results_tbl('fot_output') %>% inner_join(results_tbl('fot_map'),by='check_name')

  fot_list <- fot_check('row_cts',tblx=rslt$input_tbl)
  output_list_to_db(fot_list, append=FALSE)

  rslt$fot_output_distance <- check_fot_all_dist(fot_list$fot_heuristic_pp)
  output_tbl(rslt$fot_output_distance,
             'fot_output_distance_pp',
             indexes=list('check_name'))

  rslt$fot_output_ratios<-add_fot_ratios(fot_lib_output=results_tbl('fot_output'),
                                         fot_map=rslt$fot_map,
                                         denom_mult=10000L)
  output_tbl(rslt$fot_output_ratios,
             name='fot_output_mnth_ratio_pp')
  # BMC ----
  message('Best mapped concepts processing')
  # sending the set of best/not best mapped concepts to the schema
  rslt$bmc_conceptset<-load_codeset('bmc_conceptset', col_types='cci', indexes=list('check_name')) %>%
    inner_join(select(results_tbl('bmc_gen_output'), check_name, check_desc)%>%distinct(),
               by = 'check_name')
  copy_to_new(df=rslt$bmc_conceptset,
              name='bmc_conceptset',
              temporary = FALSE)
  # row-level assignment
  rslt$bmc_concepts <-bmc_assign(bmc_output=results_tbl('bmc_gen_output'),
                                 conceptset=load_codeset('bmc_conceptset', col_types='cci', indexes=list('check_name')))

  output_tbl(rslt$bmc_concepts,
             name='bmc_gen_output_concepts_pp')
  # computing proportions of best mapped per site/check
  rslt$bmc_pp <- bmc_rollup(rslt$bmc_concepts)
  output_tbl(rslt$bmc_pp,
             name='bmc_gen_output_pp')
  ## flag anomalies (to be used in driver_thresholds)
  rslt$bmc_anom<-compute_dist_anomalies(df_tbl=rslt$bmc_pp%>%filter(include_new==1L),
                                        grp_vars=c('check_name', 'check_desc', 'check_type'),
                                        var_col='best_row_prop')
  rslt$bmc_anom_pp<-detect_outliers(df_tbl=rslt$bmc_anom,
                                    tail_input = 'both',
                                    p_input = 0.9,
                                    column_analysis = 'best_row_prop',
                                    column_eligible = 'analysis_eligible',
                                    column_variable = 'check_name')
  output_tbl(rslt$bmc_anom_pp,
             name='bmc_anom_pp')

  # DCON ----
  message('Domain concordance processing')
  # overall
  rslt$dcon_output_pp <- apply_dcon_pp(dcon_tbl=results_tbl('dcon_output'),
                                            byyr=FALSE)
  output_tbl(rslt$dcon_output_pp,
             name='dcon_output_pp')

  # ECP ----
  message("ECP processing")
  rslt$ecp_process <- results_tbl('ecp_output') %>%
    mutate(check_name_app=paste0(check_name, '_person'))%>%
    collect()%>%
    left_join(read_codeset('ecp_cat', col_types='cc'))%>%
    mutate(check_cat=case_when(is.na(check_cat)~'other',
                               TRUE~check_cat))

  # flag anomalies (to be used in driver_thresholds)
  rslt$ecp_anom<-compute_dist_anomalies(df_tbl=rslt$ecp_process,
                                        grp_vars=c('check_name', 'check_cat'),
                                        var_col='prop_with_concept')
  rslt$ecp_anom_pp<-detect_outliers(df_tbl=rslt$ecp_anom,
                                    tail_input = 'both',
                                    p_input = 0.9,
                                    column_analysis = 'prop_with_concept',
                                    column_eligible = 'analysis_eligible',
                                    column_variable = 'check_name')
  output_tbl(rslt$ecp_anom_pp,
             name='ecp_output_pp')

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
