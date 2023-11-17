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

  message('Determining thresholds')
  thresholds <- load_codeset('threshold_limits','ccdcc',
                             indexes=list('check_name'))
  copy_to_new(df=thresholds,
              name='thresholds_pedsnet_standard',
              temporary=FALSE)

  message('Finding previous thresholds')
  redcap_prev <- .qual_tbl(name='dqa_issues_redcap_op_1510',
                           schema='dqa_rox',
                           db=config('db_src_prev'))
  thresholds_prev <- .qual_tbl(name='thresholds_op_1510',
                               schema='dqa_rox',
                               db=config('db_src_prev'))

  message('Assigning thresholds for current cycle')
  thresholds_this_version <-
    compute_new_thresholds(redcap_tbl=redcap_prev,
                           previous_thresholds=thresholds_prev,
                           threshold_tbl=thresholds)


  copy_to_new(df=thresholds_this_version,
             name='thresholds',
             temporary = FALSE)

  # -- in new version, skip this part --

  if(config('new_site_pp')) {

    thresholds_tbls <- create_thresholds_full(schema_name_input=config('results_schema_other'),
                                         append_tbl=TRUE,#config('append_to_existing'),
                                         db=config('db_src'))

    dq_names <- pull_dqa_table_names(schema_name = config('results_schema_other'))
    dq_names_vector <- dq_names[['table']]

    tbl_names <-
      config('db_src') %>%
      DBI::dbListObjects(DBI::Id(schema= config('results_schema_other'))) %>%
      dplyr::pull(table) %>%
      purrr::map(~slot(.x, 'name')) %>%
      dplyr::bind_rows() %>%
      # select(! matches('*_pp_*'))
      filter(! str_detect(table,'pp')) %>%
      #filter(str_detect(table,'output|violations')) %>%
      #filter(! str_detect(table, 'fot')) %>%
      filter(! str_detect(table, 'thrshld')) %>%
      filter(! str_detect(table, 'old'))

    tbl_names_short <-
      tbl_names %>%
      mutate_all(~gsub(paste0(config('results_name_tag')),'',.))

    tbl_names_short_vector <- tbl_names_short[['table']]

    differences <- setdiff(tbl_names_short_vector,dq_names_vector)

    for(i in differences) {

      output <- create_combined_data(i)

      output
    }

  } else {
    thresholds_tbls <- create_thresholds_full(schema_name_input=config('results_schema'),
                                         append_tbl=FALSE)
    output_list_to_db(thresholds_tbls,
                      append=FALSE)
  }




  # rslt$dqa_tbl_names <- pull_dqa_table_names()
  # rslt$dqa_tbls <- pull_dqa_tables(rslt$dqa_tbl_names)
  # rslt$check_names <- get_check_names(rslt$dqa_tbl_names)
  # rslt$thresholds <- set_broad_thresholds(rslt$check_names)
  # thresholds_attached <- attach_thresholds(tbls_all=rslt$dqa_tbls,
  #                                          thresholds=rslt$thresholds)
  #  output_list_to_db(thresholds_attached,
  #                    append=FALSE)

  ## -- and come back here --
  message('Changes between data cycles processing')
  #rslt$dc_preprocess <- dc_preprocess(results='dc_output')
  # change this to dc_output for running on dqa_library output without thresholds attached
  rslt$dc_preprocess <- dc_preprocess(results='dc_output')

  copy_to_new(df=rslt$dc_preprocess,
              name='dc_output_pp',
              temporary = FALSE)

  message('Value set and vocabulary violations processing')
  # by vocabulary_id
  rslt$vc_vs_violations_preprocess <- vc_vs_violations_preprocess(results='vc_vs_violations')
  copy_to_new(df=rslt$vc_vs_violations_preprocess,
              name='vc_vs_violations_pp',
              temporary = FALSE)
  # by check_name_app
  rslt$vc_vs_output_pp <- vc_vs_rollup(pp_output = rslt$vc_vs_violations_preprocess)
  copy_to_new(df=rslt$vc_vs_output_pp,
              name='vc_vs_output_pp',
              temporary=FALSE)

  message('Unmapped concepts processing')
  # note: this part is new and check_name_app is the only thing added
  rslt$uc_preprocess <- uc_process(results='uc_output')
  copy_to_new(df=rslt$uc_preprocess,
              name='uc_output_pp',
              temporary = FALSE)

  rslt$uc_by_year_preprocess <- uc_by_year_preprocess(results='uc_by_year')
  copy_to_new(df=rslt$uc_by_year_preprocess,
              name='uc_by_year_pp',
              temporary = FALSE)

  message('Missing field: visit id processing')
  rslt$mf_visitid_preprocess <- mf_visitid_preprocess(results='mf_visitid_output',
                                                      results_dc=results_tbl('dc_output'),
                                                      db_version=config('current_version'))
  copy_to_new(df=rslt$mf_visitid_preprocess,
              name='mf_visitid_pp',
              temporary = FALSE)

  message('Person facts processing')
  rslt$pf_output_preprocess <- pf_output_preprocess(results='pf_output')
  copy_to_new(df=rslt$pf_output_preprocess,
              name='pf_output_pp',
              temporary = FALSE)

  message('Facts over time processing')
  rslt$fot_map <- read_codeset('fot_map','cc')
  output_tbl(rslt$fot_map,
             'fot_map',
             indexes=list('domain'))
  rslt$input_tbl <- results_tbl('fot_output') %>% inner_join(results_tbl('fot_map'),by='check_name')

  fot_list <- fot_check('row_cts',tblx=rslt$input_tbl)
  output_list_to_db(fot_list)

  rslt$fot_output_distance <- check_fot_all_dist(fot_list$fot_heuristic)
  output_tbl(rslt$fot_output_distance,
             'fot_output_distance',
             indexes=list('check_name'))

  message('Best mapped concepts processing')
  # NOTE: test this part
  # sending the set of best/not best mapped concepts to the schema
  rslt$bmc_conceptset<-load_codeset('bmc_conceptset', col_types='cci', indexes=list('check_name')) %>%
    inner_join(select(results_tbl('bmc_gen_output'), check_name, check_desc)%>%distinct(),
                      by = 'check_name')
  copy_to_new(df=rslt$bmc_conceptset,
              name='bmc_conceptset',
              temporary = FALSE)
  # row-level assignment
  #rslt$bmc_concepts <- bmc_assign_old(bmc_output=results_tbl('bmc_gen_output'))
  rslt$bmc_concepts <-bmc_assign(bmc_output=results_tbl('bmc_gen_output'),
                                     conceptset=load_codeset('bmc_conceptset', col_types='cci', indexes=list('check_name')))

  output_tbl(rslt$bmc_concepts,
                name='bmc_gen_output_concepts_pp')
  # computing proportions of best mapped per site/check
  #rslt$bmc_pp <- bmc_rollup_old(rslt$bmc_concepts)
  rslt$bmc_pp <- bmc_rollup(rslt$bmc_concepts)
  output_tbl(rslt$bmc_pp,
              name='bmc_gen_output_pp')

  message('Domain concordance processing')
  # by year --> don't currently have overlap by year
  # rslt$dcon_output_byyr_pp <- apply_dcon_pp(dcon_tbl=results_tbl('dcon_by_yr'),
  #                                           byyr=TRUE)
  # output_tbl(rslt$dcon_output_byyr_pp,
  #            name='dcon_output_pp_byyr')

  # overall
  rslt$dcon_output_pp <- apply_dcon_pp(dcon_tbl=results_tbl('dcon_output'),
                                            byyr=FALSE)
  output_tbl(rslt$dcon_output_pp,
             name='dcon_output_pp')


  message('Create threshold table')
  # NEW!
  # change this to check_apps when all of the tables have been added
  # need to test whether violations are flagged appropriately based on col_name when thresholds are NOT in the pp tables
  rslt$thresholds_applied <- apply_thresholds(check_app_tbl=read_codeset('check_apps', col_types = 'cccccc'),
                                              threshold_tbl = results_tbl('thresholds'))


  output_list_to_db(rslt$thresholds_applied,
                    append=FALSE)
  rslt$threshold_violations <- reduce(.x=rslt$thresholds_applied,
                                      .f=dplyr::bind_rows)%>%
    filter(violation)

  copy_to_new(df=rslt$threshold_violations,
              name='threshold_tbl_violations',
              temporary=FALSE)


  # remove this part----
  threshold_list <- create_threshold_tbl_post()
  rslt$threshold_list <- reduce(.x=threshold_list,
                                .f=dplyr::union) %>%
    mutate(version=config('current_version')) %>%
    group_by(site,threshold,threshold_operator,
             check_name,check_name_app,version) %>%
    summarise(value_output=sum(value_output)) %>%
    ungroup()



  output_tbl(rslt$threshold_list,
             'threshold_tbl_output')

  rslt$threshold_list_violations <-
    rslt$threshold_list %>%
    mutate(violation=case_when(threshold_operator == 'gt' & value_output > threshold ~ 1,
                               threshold_operator == 'lt' & value_output < threshold ~ 1,
                               TRUE ~ 0)) %>% filter(violation==1)

  output_tbl(rslt$threshold_list_violations,
             'threshold_tbl_violations')

  # rslt$threshold_list_wide <-
  #   rslt$threshold_list %>%
  #   #select(check_name_app,value_output) %>%
  #   pivot_wider(id_cols=site,
  #               names_from = check_name_app,
  #                values_from = value_output)
  # Up to here ----

  message('Generate and add masked site identifiers to all tables with "site" column')
  rslt$pp_tbl_names <- pull_site_tables()
  rslt$tbls_anon <- attach_anon_id(all_sites_tbl=results_tbl('dc_output'),
                                   tbls_to_anon=rslt$pp_tbl_names)
  output_list_to_db_collect(rslt$tbls_anon,
                            append=FALSE)
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
