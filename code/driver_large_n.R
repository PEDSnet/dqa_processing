

.run  <- function(base_dir = config('base_dir')) {
  
  ## Data Cycle Changes
  
  rslt$dc_ln <- summarize_large_n(dq_output = results_tbl('dc_output_pp') %>%
                                    filter(site != 'total'),
                                  num_col = 'plot_prop',
                                  grp_vars = c('domain', 'check_name', 'application'))
  
  output_tbl(rslt$dc_ln %>% union(results_tbl('dc_output_pp') %>%
                                    filter(site == 'total')), 'dc_output_ln')
  
  ## Vocabulary & Valueset Conformance
  
  rslt$vcvs_ln <- summarize_large_n(dq_output = results_tbl('vc_vs_output_pp') %>%
                                      filter(site != 'total'),
                                    num_col = 'tot_prop',
                                    grp_vars = c('table_application', 'measurement_column',
                                                 'check_type', 'check_name'))
  
  output_tbl(rslt$vcvs_ln %>% union(results_tbl('vc_vs_output_pp') %>%
                                    filter(site == 'total')), 'vc_vs_output_ln')
  
  ## Unmapped Concepts
  
  rslt$uc_ln <- summarize_large_n(dq_output = results_tbl('uc_output_pp') %>% 
                                    filter(site != 'total'),
                                  num_col = 'unmapped_prop',
                                  grp_vars = c('measure', 'check_name'))
  
  output_tbl(rslt$uc_ln %>% union(results_tbl('uc_output_pp') %>%
                                      filter(site == 'total')), 'uc_output_ln')
  
  rslt$uc_by_yr_ln <- summarize_large_n(dq_output = results_tbl('uc_by_year_pp') %>%
                                          filter(site != 'total'),
                                        num_col = 'prop_total',
                                        grp_vars = c('year_date', 'unmapped_description',
                                                     'check_name'),
                                        time = TRUE)
  
  output_tbl(rslt$uc_by_yr_ln %>% union(results_tbl('uc_by_year_pp') %>%
                                      filter(site == 'total')), 'uc_by_year_ln')
  
  ## Person Facts
  
  rslt$pf_person_ln <- summarize_large_n(dq_output = results_tbl('pf_output_pp') %>%
                                           filter(site != 'total'),
                                         num_col = 'fact_pts_prop',
                                         grp_vars = c('check_description', 'check_name',
                                                      'visit_type'))
  
  rslt$pf_visit_ln <- summarize_large_n(dq_output = results_tbl('pf_output_pp') %>%
                                           filter(site != 'total'),
                                         num_col = 'fact_visits_prop',
                                         grp_vars = c('check_description', 'check_name',
                                                      'visit_type'))
  
  joincols <- colnames(rslt$pf_person_ln)
  joincols <- joincols[!joincols %in% c('max_val', 'min_val', 'q1', 'q3', 
                                        'mean_val', 'median_val')]
  
  rslt$pf_final_ln <- rslt$pf_person_ln %>% left_join(rslt$pf_visit_ln,
                                                      by = joincols,
                                                      suffix = c('_pt', '_visits')) %>%
    union(results_tbl('pf_output_pp') %>% filter(site == 'total'))
  
  output_tbl(rslt$pf_final_ln, 'pf_output_ln')
  
  ## Best Mapped Concepts
  
  rslt$bmc_ln <- summarize_large_n(dq_output = results_tbl('bmc_gen_output_pp') %>%
                                     filter(site != 'total'),
                                   num_col = 'best_row_prop',
                                   grp_vars = c('check_name', 'check_desc'))
  
  output_tbl(rslt$bmc_ln%>% union(results_tbl('bmc_gen_output_pp') %>%
                                          filter(site == 'total')), 'bmc_gen_output_ln')
  
  ## Domain Concordance
  
  rslt$dcon_ln <- summarize_large_n(dq_output = results_tbl('dcon_output_pp') %>%
                                            filter(site != 'total'),
                                          num_col = 'prop',
                                          grp_vars = c('cohort', 'check_name', 
                                                       'check_type'))
  
  output_tbl(rslt$dcon_ln %>% union(results_tbl('dcon_output_pp') %>%
                                    filter(site == 'total')), 'dcon_output_ln')
  
  ## MF VisitID
  
  rslt$mf_visitid_ln <- summarize_large_n(dq_output = results_tbl('mf_visitid_pp') %>%
                                            filter(site != 'total'),
                                          num_col = 'prop_missing_visits_total',
                                          grp_vars = c('measure', 'domain', 'check_name'))
  
  output_tbl(rslt$mf_visitid_ln %>% union(results_tbl('mf_visitid_pp') %>%
                                      filter(site == 'total')), 'mf_visitid_ln')
  
  ## Expected Concepts Present
  
  rslt$ecp_ln <- summarize_large_n(dq_output = results_tbl('ecp_output_pp') %>%
                                     filter(site != 'total'),
                                   num_col = 'prop_with_concept',
                                   grp_vars = c('concept_group', 'check_name'))
  
  output_tbl(rslt$ecp_ln %>% union(results_tbl('ecp_output_pp') %>%
                                            filter(site == 'total')), 'ecp_output_ln')
  
  ## Facts Over Time
  
}
