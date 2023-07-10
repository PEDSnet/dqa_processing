


#' combine new data together
#'
#' @param tbl_name the tbl name to
#' grab from another schema; should
#' match the destination tbl_name
#'
#' @return a database tbl with the union
#' of the existing with the new site
#'

create_combined_data <- function(tbl_name) {

  newdata_tbl <-
    results_tbl_other(tbl_name) %>%
    collect()

  output_tbl_append(newdata_tbl,
                    name=tbl_name)

}

# create_combined_data('bmc_rxnorm_output')
# create_combined_data('bmc_rxnorm_output_pp')
# create_combined_data('dcon_meta')
# create_combined_data('dcon_output')
# create_combined_data('dcon_output_pp')
# create_combined_data('fot_heuristic')
# create_combined_data('fot_heuristic_summary')
# create_combined_data('fot_map')
# create_combined_data('fot_output_distance')
# create_combined_data('fot_output') # run this one
# create_combined_data('mf_visitid_output')
# create_combined_data('pf_mappings')
# create_combined_data('pf_output')
# create_combined_data('pf_output_pp')
# create_combined_data('threshold_tbl_output')
# create_combined_data('threshold_tbl_violations')
# create_combined_data('thresholds')
# create_combined_data('uc_by_year') # run this one
# create_combined_data('uc_by_year_pp')
# create_combined_data('uc_grpd') # run this one
# create_combined_data('uc_output')
# create_combined_data('vc_vs_violations')
# create_combined_data('vc_vs_violations_pp')
