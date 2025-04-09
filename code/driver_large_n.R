

.run  <- function(base_dir = config('base_dir')) {

  ## Data Cycle Changes ----
  rslt <- list()
  rslt$dc_ln <- summarize_large_n(dq_output = results_tbl('dc_output_pp') %>%
                                    filter(site != 'total'),
                                  check_string = 'dc',
                                  num_col = 'prop_total_change',
                                  grp_vars = c('domain', 'check_name', 'application', 'check_type', 'check_name_app'),
                                  shape='wide')

  output_tbl(rslt$dc_ln %>% bind_rows(results_tbl('dc_output_pp') %>%
                                    filter(site == 'total')%>%
                                      select(-c(max_val, min_val, mean_val, median_val))%>% collect()), 'dc_output_ln')

  ## Vocabulary & Valueset Conformance ----

  rslt$vs_ln <- summarize_large_n(dq_output = results_tbl('vs_output_pp') %>%
                                      filter(site != 'total'),
                                    check_string = 'vs',
                                    num_col = 'prop_viol',
                                    grp_vars = c('table_application', 'measurement_column',
                                                 'check_type', 'check_name'),
                                    shape="wide")
  output_tbl(rslt$vs_ln,
             'vs_output_ln')

  rslt$vc_ln <- summarize_large_n(dq_output = results_tbl('vc_output_pp') %>%
                                    filter(site != 'total'),
                                  check_string = 'vc',
                                  num_col = 'prop_viol',
                                  grp_vars = c('table_application', 'measurement_column',
                                               'check_type', 'check_name'),
                                  shape="wide")

  output_tbl(rslt$vc_ln,
             'vc_output_ln')

  ## Unmapped Concepts ----

  rslt$uc_ln <- summarize_large_n(dq_output = results_tbl('uc_output_pp') %>%
                                    filter(site != 'total'),
                                  check_string = 'uc',
                                  num_col = 'unmapped_prop',
                                  grp_vars = c('measure', 'check_name', 'check_type'),
                                  shape="wide")

  output_tbl(rslt$uc_ln %>% bind_rows(results_tbl('uc_output_pp') %>%
                                      filter(site == 'total') %>% collect()), 'uc_output_ln')

  rslt$uc_by_yr_ln <- summarize_large_n(dq_output = results_tbl('uc_by_year_pp') %>%
                                          filter(site != 'total'),
                                        check_string = 'uc',
                                        num_col = 'prop_total',
                                        grp_vars = c('check_type', 'database_version',  'year_date', 'unmapped_description',
                                                     'check_name'),
                                        shape="wide")

  output_tbl(rslt$uc_by_yr_ln %>% bind_rows(results_tbl('uc_by_year_pp') %>%
                                      filter(site == 'total') %>% collect()), 'uc_by_year_ln')

  ## Person Facts ----

  ### person level
  rslt$pf_person_ln <- summarize_large_n(dq_output = results_tbl('pf_output_pp') %>%
                                           filter(site != 'total'),
                                         check_string = 'pf',
                                         num_col = 'fact_pts_prop',
                                         grp_vars = c('check_description', 'check_name',
                                                      'visit_type', 'check_desc_neat'),
                                         shape="wide")%>%
    rename_with(~paste0(.x,"_pts"), c(min_val, max_val,
                                        q1, q3,
                                        median_val, mean_val))

  ### visit level
  rslt$pf_visit_ln <- summarize_large_n(dq_output = results_tbl('pf_output_pp') %>%
                                           filter(site != 'total'),
                                        check_string = 'pf',
                                         num_col = 'fact_visits_prop',
                                         grp_vars = c('check_description', 'check_name',
                                                      'visit_type', 'check_desc_neat'),
                                        shape="wide")%>%
    rename_with(~paste0(.x,"_visits"), c(min_val, max_val,
                                      q1, q3,
                                      median_val, mean_val))

  ### stitch together and bring in the total rows
  rslt$pf_final_ln <- rslt$pf_person_ln %>% full_join(rslt$pf_visit_ln) %>%
    bind_rows(results_tbl('pf_output_pp') %>% filter(site == 'total') %>% collect())

  output_tbl(rslt$pf_final_ln, 'pf_output_ln')

  ## Best Mapped Concepts ----

  rslt$bmc_ln <- summarize_large_n(dq_output = results_tbl('bmc_gen_output_pp') %>%
                                     filter(site != 'total'),
                                   check_string = 'bmc',
                                   num_col = 'best_row_prop',
                                   grp_vars = c('check_name', 'check_desc', 'include_new'),
                                   shape="wide")

  output_tbl(rslt$bmc_ln%>% bind_rows(results_tbl('bmc_gen_output_pp') %>%
                                          filter(site == 'total') %>% collect()),
             'bmc_gen_output_ln')

  ## Domain Concordance ----

  rslt$dcon_ln <- summarize_large_n(dq_output = results_tbl('dcon_output_pp') %>%
                                            filter(site != 'total'),
                                    check_string = 'dcon',
                                          num_col = 'prop',
                                          grp_vars = c('cohort', 'check_name','check_desc',
                                                       'check_type'),
                                    shape="wide")

  output_tbl(rslt$dcon_ln %>% bind_rows(results_tbl('dcon_output_pp') %>%
                                    filter(site == 'total') %>% collect()), 'dcon_output_ln')

  ## MF VisitID ----

  rslt$mf_visitid_ln <- summarize_large_n(dq_output = results_tbl('mf_visitid_pp') %>%
                                            filter(site != 'total'),
                                          check_string = 'mf',
                                          num_col = 'prop_missing_visits_total',
                                          grp_vars = c('measure', 'domain', 'check_name', 'check_type', 'check_name_app'),
                                          shape="wide")

  output_tbl(rslt$mf_visitid_ln %>% bind_rows(results_tbl('mf_visitid_pp') %>%
                                      filter(site == 'total') %>% collect()), 'mf_visitid_ln')

  ## Expected Concepts Present ----

  rslt$ecp_ln <- summarize_large_n(dq_output = results_tbl('ecp_output_pp') %>%
                                     filter(site != 'total'),
                                   check_string = 'ecp',
                                   num_col = 'prop_with_concept',
                                   grp_vars = c('concept_group', 'check_name', 'check_name_app','cohort_denominator'),
                                   shape="wide")

  output_tbl(rslt$ecp_ln %>% bind_rows(results_tbl('ecp_output_pp') %>%
                                            filter(site == 'total') %>% collect()), 'ecp_output_ln')


}
