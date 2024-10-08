#'
#' This file contains functions to identify cohorts in a study.  Its
#' contents, as well as the contents of any R files whose name begins
#' with "cohort_", will be automatically sourced when the request is
#' executed.
#'
#' For simpler requests, it will often be possible to do all cohort
#' manipulation in functions within this file.  For more complex
#' requests, it may be more readable to separate different cohorts or
#' operations on cohorts into logical groups in different files.
#'

#' function to add proportions,thresholds, and totals to dc_output
#'
#' @param results dc_output results tbl
#'
#' @return dc_output tbl with totals and additional change in proportion
#' and threshold columns

dc_preprocess <- function(results) {

  dc_wider <- results_tbl(results) %>%
    pivot_wider(names_from=database_version,
                values_from=c(total_ct, total_pt_ct))%>%
    collect()

  prev_v <- paste0('total_ct_',config('previous_version'))
  current_v <- paste0('total_ct_',config('current_version'))

  prev_v_pt <- paste0('total_pt_ct_',config('previous_version'))
  current_v_pt <- paste0('total_pt_ct_',config('current_version'))


   dc_totals_person <- dc_wider %>%
     group_by(domain, check_name, check_type) %>%
     summarise({{prev_v}} := sum(.data[[prev_v_pt]]),
               {{current_v}} := sum(.data[[current_v_pt]])) %>%
     mutate(site='total', application='person')%>%
     ungroup()

   dc_totals_rows <- dc_wider %>%
     group_by(domain, check_name, check_type) %>%
     summarise({{prev_v}} := sum(.data[[prev_v]]),
               {{current_v}} := sum(.data[[current_v]])) %>%
     mutate(site='total', application='rows')%>%
     ungroup()

   dc_site_pats <- dc_wider %>% select(-c({{prev_v}}, {{current_v}})) %>%
     rename({{prev_v}}:=all_of(prev_v_pt),
            {{current_v}}:=all_of(current_v_pt))%>%
     mutate(application='person')

   dc_site_rows <- dc_wider %>% select(-c({{prev_v_pt}}, {{current_v_pt}})) %>%
     mutate(application='rows')

  all_dat<-bind_rows(dc_totals_person, dc_totals_rows) %>%
    bind_rows(., dc_site_pats) %>%
    bind_rows(., dc_site_rows) %>%
    mutate(prop_total_change=
             case_when(!!sym(prev_v) == 0 ~ NA_real_,
                       TRUE ~ round((.data[[current_v]]-.data[[prev_v]])/.data[[prev_v]],2))) %>%
    mutate(check_name_app=paste0(check_name, "_", application))

  # adding in a scaled proportion
  max_val<-all_dat%>%summarise(m=max(abs(prop_total_change), na.rm=TRUE))%>%pull()

  all_dat %>%
    mutate(prop_total_change=case_when(is.na(prop_total_change)~0,
                                       TRUE~prop_total_change),
           abs_prop=abs(prop_total_change),
           newval=1+((exp(100)-1)/(max_val)*(abs_prop)),
           plot_prop=case_when(prop_total_change<0~-1*log(newval),
                                TRUE~log(newval)))%>%
    select(-c(abs_prop, newval))


}

#' Function to compute proportions of valueset values from vs check
#' @param results name of the vs library output in results schema
#' @return table with:
#'            site
#'            table_application
#'            measurement_column
#'            vocabulary_id
#'            check_type
#'            check_name
#'            total_denom_ct
#'            accepted_value
#'            tot_ct: sum of total rows for given value in vs
#'            tot_prop: sum of proportion of all values for that vs
vs_process<-function(results){
  results_tbl(results) %>%
    mutate(prop_total_viol=as.numeric(total_viol_ct/total_denom_ct),
           prop_total_pt_viol=as.numeric(total_viol_pt_ct/total_pt_ct)) %>%
    group_by(site, table_application, measurement_column, vocabulary_id, check_type, check_name, total_denom_ct, accepted_value) %>%
    summarise(tot_ct = sum(total_viol_ct),
              tot_prop = sum(prop_total_viol)) %>%
    ungroup()
}

#' Function to compute proportions of vocabulary values from vc check
#' @param results name of the vc library output in results schema
#' @return table with:
#'            site
#'            table_application
#'            measurement_column
#'            vocabulary_id
#'            check_type
#'            check_name
#'            total_denom_ct
#'            accepted_value
#'            tot_ct: sum of total rows for given value in vs
#'            tot_prop: sum of proportion of all values for that vs
vc_process<-function(results){
  results_tbl(results) %>%
    mutate(prop_total_viol=as.numeric(total_viol_ct/total_denom_ct),
           prop_total_pt_viol=as.numeric(total_viol_pt_ct/total_pt_ct)) %>%
    group_by(site, table_application, measurement_column, vocabulary_id, check_type, check_name, total_denom_ct, accepted_value) %>%
    summarise(tot_ct = sum(total_viol_ct),
              tot_prop = sum(prop_total_viol)) %>%
    ungroup()
}


#' function to sum total counts by site, and check_name
#' and calculate proportions
#'
#' @param pp_output vs_vc_output results tbl
#'
#' @return vc_vs_output tbl with summed total counts and proportions per check_name, only for violations

vc_vs_rollup <- function(pp_output){
  pp_output %>%
    filter(!accepted_value)%>%
    group_by(site, table_application, measurement_column, check_type, check_name, total_denom_ct) %>%
    summarise(tot_ct=sum(tot_ct),
              tot_prop=sum(tot_prop))%>%
    ungroup()%>%
    mutate(check_name_app=paste0(check_name, "_rows"))

}


#' function to add proportions to uc_by_year
#'
#' @param results uc_by_year results tbl
#'
#' @return uc_by_year tbl with additional unmapped concept proportions column


uc_by_year_preprocess <- function(results) {

  results_tbl(results) %>%
    mutate(prop_total=total_unmapped_row_ct/total_row_ct)
}

#' Function to add post-processed columns to the unmapped concepts dqa_library output
#'             and to add a `total` row with for each of the check applications
#'             for the overall number and proportion of unmapped rows
#' @param results name of output table for the uc output from dqa_library
#' @return table with additional columns/etc needed for pp output
uc_process <- function(results){
  total_uc<-results_tbl(results) %>%
    group_by(measure, check_type, database_version, check_name) %>%
    summarise(total_rows=sum(total_rows),
              unmapped_rows=sum(unmapped_rows))%>%
    ungroup() %>%
    mutate(site='total',
           unmapped_prop=unmapped_rows/total_rows)

  total_uc %>%
    dplyr::union_all(results_tbl(results))%>%
    mutate(check_name_app=paste0(check_name,"_rows"))
}


#' function to add proportions to mf results
#'
#' @param results mf_visitid results tbl
#' @param results_dc output from the changes between data cycles check from dqa_library
#' @param db_version current database version
#'
#' @return mf_visitid tbl with additional domain, total_ct, and proportion
#' column

mf_visitid_preprocess <- function(results) {

  test_mf <- results_tbl(results) %>%
    mutate(
      domain = case_when(
        measure == "conditions excluding problem list" ~ "condition_occurrence",
        measure == "all drugs" ~ "drug_exposure",
        measure == "prescription or inpatient drugs" ~ "drug_exposure",
        measure == "all procedures" ~ "procedures",
        measure == "all labs" ~ "measurement_labs",
        measure == "all immunizations" ~ "immunization"))
  # compute overall counts for mf_visitid check
  test_mf_overall <- test_mf %>%
    group_by(domain, measure, check_type, database_version, check_name) %>%
    summarise(total_visits=sum(total_visits),
              missing_visits_total=sum(missing_visits_total),
              missing_visits_distinct=sum(missing_visits_distinct),
              visit_na=sum(visit_na),
              total_id=sum(total_id),
              total_ct=sum(total_ct)) %>%
    ungroup()%>%
    mutate(site = 'total')

  # compute proportions
  test_mf %>%
    dplyr::union_all(test_mf_overall)%>%
   # left_join(dc_overall, by = c("site", "domain")) %>%
    filter(total_ct!=0)%>%
    mutate(prop_total_visits = round(total_visits/total_ct, 2),
           prop_missing_visits_total = round(missing_visits_total/total_ct,2))%>%
   # distinct()%>%
    mutate(check_name_app=paste0(check_name, "_rows"))

}


#' function to add total counts by check_description
#'
#' @param results pf_output results tbl
#'
#' @return pf_output tbl with additional total counts

pf_output_preprocess <- function(results) {

  rslt_collect<-results_tbl(results)%>%collect()
  # %>%
  #   mutate(check_name=case_when(check_name=='pf_dr'~'pf_visits_dr',
  #                               TRUE~check_name))

  db_version<-config('current_version')

  pf_totals <- results_tbl(results) %>%
    # mutate(check_name=case_when(check_name=='pf_dr'~'pf_visits_dr',
    #                             TRUE~check_name))%>%
    group_by(check_description, check_name) %>%
    summarise(no_fact_visits=sum(no_fact_visits),
              no_fact_pts=sum(no_fact_pts),
              total_visits=sum(total_visits),
              total_pts=sum(total_pts),
              fact_visits=sum(fact_visits),
              fact_pts=sum(fact_pts)) %>%
    ungroup()%>%
    mutate(site='total',
           check_type='pf',
           database_version=db_version)%>%
    mutate(no_fact_visits_prop=round(no_fact_visits/total_visits,2),
           no_fact_pts_prop=round(no_fact_pts/total_pts,2),
           fact_visits_prop=1-no_fact_visits_prop,
           fact_pts_prop=1-no_fact_pts_prop) %>%
    collect()

  # have to collect to bind rows since total columns may be missing site-specific things (e.g. thresholds)
   bind_rows(rslt_collect,pf_totals)%>%
     mutate(visit_type = case_when(str_detect(check_description, "^long_ip")~'long_inpatient',
                                   str_detect(check_description, "^ip")~ 'inpatient',
                                   str_detect(check_description, "^all")~'all',
                                   str_detect(check_description, "^op")~'outpatient',
                                   str_detect(check_description, "^ed")~'emergency'),
            check_description=str_remove(check_description, "^long_ip_|^ip_|^all_|^op_|^ed_")) %>%
     mutate(check_description= case_when(check_description=='all_visits_with_procs_drugs_labs' ~ 'visits_with_procs_drugs_labs',
                                         TRUE ~ check_description))%>%
     mutate(check_name_app=paste0(check_name, "_visits"),
            check_desc_neat=str_remove(check_description, "visits_with_|_visits"))

}

#' This function calculates the heuristic used in the FOT dq check
#' The heuristic is:
#' month / ((month-1)*.25 +
#'          (month+1)*.25 +
#'          (month-12)*.5)
#'In plain words, its the current month divided by the weighted average of the
#'previous month, the next month, and the value from the current month in the
#'previous year
fot_check_calc <- function(tblx, site_col,time_col, target_col) {
  tblx %>%
    window_order(!!sym(site_col),!!sym(time_col)) %>%
    mutate(
      lag_1 = lag(!!sym(target_col)),
      lag_1_plus = lag(!!sym(target_col),-1),
      lag_12 = lag(!!sym(target_col),12),
      check_denom_stupid = (lag(!!sym(target_col))*.25 +
                              lag(!!sym(target_col),-1)*.25 +
                              lag(!!sym(target_col),12)*.5)) %>%
    filter(check_denom_stupid!=0) %>%
    mutate(check = !!sym(target_col)/check_denom_stupid-1)
}

#' Main function that does the FOT checks
fot_check <- function(target_col,
                      tblx=results_tbl('fot_output'),
                      check_col='check_name',
                      check_desc='check_desc',
                      site_col='site',
                      time_col='month_end') {

  cols_to_keep <- c('domain',eval(site_col),eval(check_col),eval(check_desc),eval(time_col),'check')

  rv <- FALSE
  rv_agg <- FALSE
  #base tbl to make a network wide version of the check
  agg_check <- tblx %>% group_by(domain, !!sym(time_col),!!sym(check_col), !!sym(check_desc)) %>%
    summarise({{target_col}} := sum(!!sym(target_col))) %>%
    ungroup() %>%
    mutate({{site_col}}:='all')

  for (target_check in tblx %>% select(!!sym(check_col)) %>% distinct() %>% pull()) {
    for (target_site in tblx %>% select(!!sym(site_col)) %>% distinct() %>% pull()) {
      foo <- fot_check_calc(tblx %>%
                              filter(check_name==target_check & site==target_site),
                            site_col='site',
                            time_col,
                            target_col) %>% collect()
      if(!is.logical(rv)){
        rv <- union(rv, foo)
      } else {
        rv <- foo
      }
    }
    bar <- fot_check_calc(agg_check %>% filter(check_name==target_check),
                          site_col,time_col,target_col) %>%
      select(cols_to_keep) %>% collect()
    if(!is.logical(rv_agg)){
      rv_agg <- union(rv_agg, bar)
    } else {
      rv_agg <- bar
    }
  }

  #summarise the checks across sites
  rv_summary <- rv %>% group_by(domain, !!sym(check_col), !!sym(site_col)) %>%
    summarise(std_dev = sd(check,na.rm=TRUE),
              pct_25 = quantile(check,.25),
              pct_75 = quantile(check,.75),
              med = median(check),
              m = mean(check)) %>% ungroup() %>% collect()

  rv_summary_allsites <- rv_agg %>%
    filter(site=='all') %>% group_by(domain, !!sym(check_col), !!sym(site_col)) %>%
    summarise(std_dev = sd(check,na.rm=TRUE),
              pct_25 = quantile(check,.25),
              pct_75 = quantile(check,.75),
              med = median(check),
              m = mean(check)) %>% ungroup() %>% collect() %>%
    mutate(site='all')


  return(list(fot_heuristic_pp= dplyr::union(rv %>% select(cols_to_keep),
                                          rv_agg),
              fot_heuristic_summary_pp=dplyr::union(rv_summary,
                                                 rv_summary_allsites)))
}

#' fot table computing distance from "all" check
#'
#' @param fot_check_output first element of list output from `fot_check`
#'
#' @return tbl with the following columns:
#' domain | check_name | month_end | centroid | site | check | distance
#'
#' The `distance` column measures, for each site/domain/check/month combination,
#' the distance between the site's normalized `check` output compared to
#' all sites combined.
#'

check_fot_all_dist <- function(fot_check_output) {

  just_all <-
    fot_check_output %>%
    filter(site=='all') %>%
    rename(centroid=check) %>%
    select(-c(site))

  combined <-
    just_all %>%
    inner_join(
      fot_check_output,
      by=c('domain',
           'check_name',
           'month_end',
           'check_desc')
    ) %>% mutate(
      distance=round(check,3)-round(centroid,3)
    )
}



#' output table to database if it does not exist, or
#' append it to an existing table with the same name if it does
#'
#' @param data the data to output
#' @param name the name of the table to output
#'
#' Parameters are the same as `output_tbl`
#'
#' @return The table as it exists on the databse, with the new data
#' appended, if the table already existts.
#'

output_tbl_append <- function(data, name = NA, local = FALSE,
                              file = ifelse(config('execution_mode') !=
                                              'development', TRUE, FALSE),
                              db = ifelse(config('execution_mode') !=
                                            'distribution', TRUE, FALSE),
                              results_tag = TRUE, ...) {

  if (is.na(name)) name <- quo_name(enquo(data))

  if(db_exists_table(config('db_src'),intermed_name(name,temporary=FALSE))) {

    tmp <- results_tbl(name) %>% collect_new
    new_tbl <-
      bind_rows(tmp,
                data) %>% distinct()
    output_tbl(data=new_tbl,
               name=name,
               local=local,
               file=file,
               db=db,
               results_tag = TRUE, ...)
  } else {
    output_tbl(data=data,
               name=name,
               local=local,
               file=file,
               db=db,
               results_tag = TRUE, ...)
  }


}


#' output a list of tables to the database
#'
#' @param output_list list of tables to output
#' @param append logical to determine if you want to append if the table exists
#'
#' @return tables output to the database; if
#' table already exists, it will be appended
#'

output_list_to_db <- function(output_list,
                              append=TRUE) {


  if(append) {

    for(i in 1:length(output_list)) {

      output_tbl_append(data=output_list[[i]],
                        name=names(output_list[i]))

    }

  } else {

    for(i in 1:length(output_list)) {

      output_tbl(data=output_list[[i]],
                 name=names(output_list[i]))

    }

  }

}

#' Function to group together RxNorm levels
#' and sum the number and proportion of records
#' within that group
#' @param drug_tbl table containing DQ output from the BMC RxNorm check
#' @param des_rollup character vector of values that the `rxnorm_level` can take on
#' @return table containing
#'                the columns (from the original `drug_tbl`):
#'                        site, database_version, check_type, check_desc, check_desc_short
#'                and the new colums:
#'                        rollup_value: `des_rollup` list
#'                        rollup_value_ct: sum of records meeting desired level
#'                        rollup_value_prop: proportion of records meeting desired level
rollup_rxnorm_level <- function(drug_tbl,
                                des_rollup){
  drug_tbl %>%
    filter(rxnorm_level%in%!!des_rollup)%>%
    mutate(rollup_value=as.character(des_rollup)) %>%
    group_by(site, database_version, check_type, check_name, check_desc, check_desc_short,threshold,threshold_operator,
             rollup_value)%>%
    summarise(rollup_value_ct=sum(rxnorm_rows),
              rollup_value_prop=sum(row_proportions))%>%
    ungroup()
}

#' Function to add a proportion column
#'     when counts are computed for each domain and for combined
#'     and to add total as a site summarizing cohorts from all sites
#' @param dcon_tbl output from the dcon check, expected to have the cols:
#'     check_name, check_desc, site, check_type, database_version, yr, cohort, value
#' @param byyr boolean indicator of whether the dcon_tbl output is by year or not
#' @return dcon_tbl with additional columns with totals and proportions for the checks
apply_dcon_pp <- function(dcon_tbl,
                          byyr){
  dcon_tbl <- collect_new(dcon_tbl)
  if(byyr){
    dcon_overall <- dcon_tbl %>%
      group_by(check_type, database_version, check_name, check_desc, yr, cohort)%>%
      summarise(value_pts=sum(value_pts,na.rm=TRUE),
                value_visits=sum(value_visits, na.rm=TRUE))%>%
      ungroup()%>%
      mutate(site='total')

    dcon_tbl_pp<- bind_rows(dcon_tbl, dcon_overall) %>%
      group_by(site, yr, check_name, check_type, check_desc) %>%
      mutate(tot_pats=sum(value_pts, na.rm = TRUE),
             tot_vis=sum(value_visits, na.rm=TRUE)) %>%
      ungroup()%>%
      mutate(pats_prop=value_pts/tot_pats,
             visits_prop=value_visits/tot_vis)
  }else{
  dcon_overall <- dcon_tbl %>%
    group_by(check_type, database_version, check_name, check_desc, cohort) %>%
    summarise(value=sum(value,na.rm=TRUE))%>%
    ungroup()%>%
    mutate(site='total')

  dcon_tbl_pp<-dcon_tbl %>%
    bind_rows(dcon_overall) %>%
    pivot_wider(values_from = value,
                names_from=cohort)%>%
    mutate(tot_pats=cohort_1+cohort_2-combined,
           cohort_1_only=cohort_1-combined,
           cohort_2_only=cohort_2-combined,
           cohort_1_in_2=combined,
           cohort_2_in_1=combined,
           # duplicative, but keeping around for proportion of 2 in 1 and vice versa
           cohort_1_denom=cohort_1-combined,
           cohort_2_denom=cohort_2-combined)%>%
    pivot_longer(cols=c(cohort_1_only, cohort_2_only, combined, cohort_2_in_1, cohort_1_in_2, cohort_1_denom, cohort_2_denom),
                 names_to="cohort",
                 values_to="value")%>%
    # cohort_1_only: overall, patients in just cohort 1
    # cohort_2_only: overall, patients in just cohort 2
    # combined: overall, patients in both 1 and 2
    # cohort_1_denom: patients in cohort 1 not cohort 2
    # cohort_2_denom: patients in cohort 2 not cohort 1
    # cohort_1_in_2: patients in cohort 2 who are also in 1 (use cohort 2 in denom)
    # cohort_2_in_1: patients in cohort 1 who are also in 2 (use cohort 1 in denom)
    # note that cohort_1_in_2 and cohort_2_in_1 will have the same raw number, but different proportions since the denominator is different
     mutate(prop=case_when(cohort%in%c('cohort_1_only', 'cohort_2_only', 'combined')~value/tot_pats,
                           cohort=='cohort_1_in_2'~value/cohort_2,
                           cohort=='cohort_2_in_1'~value/cohort_1,
                           cohort=='cohort_1_denom'~value/cohort_1,
                           cohort=='cohort_2_denom'~value/cohort_2))%>%
    # in case one of the cohort denominators is 0
    mutate(prop=case_when(is.na(prop)~0,
                          TRUE~prop))
  }
  return(dcon_tbl_pp%>%
           mutate(check_name_app=paste0(check_name,"_concordance")))
}

#' Function to assign a "best" indicator and to add a site='total' count per concept
#' @param bmc_output output table from dqa_library for the bmc check
#' @param conceptset .csv file in specs directory with cols:
#'                      check_name: name of check to match values in bmc_output
#'                      concept: concept to match values in bmc_output
#'                      include: 0 if check should count up the "best" as any concept NOT assigned a 0 or 1 if check should count up the "best" as any concept that IS assigned a 1
#' @return table with column `include_new` to indicate whether concept should be counted as a "best" concept for the given check_name
bmc_assign_old <- function(bmc_output,
                       conceptset=load_codeset('bmc_conceptset', col_types='cci', indexes=list('check_name'))){
  # if check_name+include indicates ones that should not be counted as "best", assign count_best=0
  # if check_name+include indicates ones that should be counted as "best", assign count_best=1
  best_designation <- conceptset %>%
    filter(!is.na(include))%>%
    select(check_name, include)%>%
    distinct()%>%
    rename(count_best=include) # probably can remove this in the future if the concept set only has ones we want to count/not count

  bmc_w_best <- bmc_output %>%
    inner_join(best_designation, by = 'check_name')%>%
    left_join(conceptset, by=c('check_name', 'concept')) %>%
    mutate(include_new=case_when(is.na(include)&count_best==0L~1L, # insert opposite of what's already there for the ones that weren't in the set as either "best" or "not best"
                             is.na(include)&count_best==1L~0L,
                             !is.na(include)~include)) %>%
    collect()
  # compute totals across sites
  site_row_counts <- bmc_w_best%>%
    select(total_rows,total_pts, site, check_name)%>%
    distinct() %>%
    group_by(check_name)%>%
    summarise(total_rows=sum(total_rows),
              total_pts=sum(total_pts)) %>%
    ungroup()

  bmc_w_best_overall <- bmc_w_best%>%
    group_by(concept, check_type, database_version, check_name, check_desc, #check_name_app,
             check_desc_short,
             count_best, include, include_new)%>%
    summarise(concept_rows=sum(concept_rows),
              concept_pts=sum(concept_pts)) %>%
    ungroup()%>%
    inner_join(site_row_counts, by = 'check_name')%>%
    mutate(site='total',
           row_proportions=concept_rows/total_rows,
           person_proportions=concept_pts/total_pts)

  bind_rows(bmc_w_best, bmc_w_best_overall)

}

bmc_assign <- function(bmc_output,
                           conceptset=load_codeset('bmc_conceptset', col_types='cci', indexes=list('check_name'))){
  # if check_name+include indicates ones that should not be counted as "best", assign count_best=0
  # if check_name+include indicates ones that should be counted as "best", assign count_best=1
  best_designation <- conceptset %>%
    filter(!is.na(include))%>%
    select(check_name, include)%>%
    distinct()%>%
    rename(count_best=include) # probably can remove this in the future if the concept set only has ones we want to count/not count

  bmc_w_best <- bmc_output %>%
    inner_join(best_designation, by = 'check_name')%>%
    left_join(conceptset, by=c('check_name', 'concept')) %>%
    mutate(include_new=case_when(is.na(include)&count_best==0L~1L, # insert opposite of what's already there for the ones that weren't in the set as either "best" or "not best"
                                 is.na(include)&count_best==1L~0L,
                                 !is.na(include)~include)) %>%
    collect()

  return(bmc_w_best)

}

#' Function to compute proportion of "best" based on output from bmc check
#' @param bmc_output_pp table output from the bmc_assign function, which has all the columns output from the bmc check + an indicator column for whether the concept should be in the "best" category
#' @return table with the cols: site, check_type, database_version, check_name, check_desc, check_desc_short, count_best, include, total_rows, total_pts, best_row_prop, best_pts_prop
bmc_rollup_old <- function(bmc_output_pp){
  bmc_output_pp %>%
    group_by(across(c(site, check_type, database_version, starts_with("check_name"), check_desc, check_desc_short, count_best, include_new, total_rows, total_pts, starts_with("threshold"))))%>%
    summarise(best_rows=sum(concept_rows),
              best_pts=sum(concept_pts)) %>%
    ungroup()%>%
    mutate(best_row_prop=best_rows/total_rows,
           best_pts_prop=best_pts/total_pts) # if we just want to look at the ones that are ranked as best, limit to where include_new=1

}

#' Function to compute proportion of "best" based on output from bmc check
#' @param bmc_output_pp table output from the bmc_assign function, which has all the columns output from the bmc check + an indicator column for whether the concept should be in the "best" category
#' @return table with the cols: site, check_type, database_version, check_name, check_desc, check_desc_short, count_best, include, total_rows, total_pts, best_row_prop, best_pts_prop
bmc_rollup <- function(bmc_output_pp,
                           check_domains=read_codeset('check_domains', col_types='ccccc')){
  # find proportions of best mapped for each site
  bmc_sites <- bmc_output_pp %>%
    #filter(include_new==1L)%>%
    group_by(across(c(site, include_new, check_type, database_version, starts_with("check_name"), check_desc, check_desc_short, total_rows, starts_with("threshold"))))%>%
    summarise(best_rows=sum(concept_rows)) %>%
    ungroup()%>%
    mutate(best_row_prop=best_rows/total_rows)

  # add up site counts to get overall proportions
  bmc_overall <- bmc_sites %>%
    filter(include_new==1L)%>%
    group_by(check_type, database_version, check_name, check_desc, check_desc_short)%>%
    summarise(best_rows=sum(best_rows),
              total_rows=sum(total_rows))%>%
    ungroup()%>%
    mutate(best_row_prop=best_rows/total_rows,
           site='total')

  # finding instances where no best mapped rows in a table
  sites <- bmc_sites %>% distinct(site)
  check_domains <- bmc_sites %>% distinct(check_type, check_name, check_desc, check_desc_short)
  sites_and_checks <- sites %>% cross_join(check_domains)

  site_no_bmc <- sites_and_checks%>%
    anti_join(bmc_sites, by = c('site', 'check_name'))%>%
    mutate(best_rows=0,
           best_row_prop=0,
           database_version=config('current_version'),
           include_new=1L)


  bind_rows(bmc_sites, bmc_overall)%>%
    bind_rows(., site_no_bmc)%>%
    mutate(check_name_app=paste0(check_name, "_rows"))


}

add_fot_ratios<-function(fot_lib_output,
                         fot_map,
                         denom_mult){
  fot_input_tbl<-fot_lib_output %>%
    mutate(row_ratio=case_when(row_pts==0|total_pt==0~0,
                               TRUE~row_pts/(total_pt)*denom_mult))%>%collect()

  fot_input_tbl_allsite_med<-fot_input_tbl%>%
    group_by(check_type, check_name, check_desc, database_version, month_end) %>%
    summarise(row_ratio=median(row_ratio, na.rm=TRUE))%>%
    ungroup()%>%
    mutate(site='allsite_median')

  fot_input_tbl_allsite_mean<-fot_input_tbl%>%
    group_by(check_type, check_name, check_desc, database_version, month_end) %>%
    summarise(row_ratio=mean(row_ratio, na.rm=TRUE))%>%
    ungroup()%>%
    mutate(site='allsite_mean')

  bind_rows(fot_input_tbl,
            fot_input_tbl_allsite_med)%>%
    bind_rows(fot_input_tbl_allsite_mean)%>%
    left_join(fot_map, by = 'check_name')

}

#' @param df_tbl output from the computation of a particular function for anomaly detection
#' @param grp_vars the columns to group by to compute the summary statistics for
#' @param prop_concept column to perform summary statistics for, to detect an anomaly
#'
#' @return the `df_tbl` with the following computed:
#'  `mean_val`, `median_val`, `sd_val`, `mad_val`, `cov_val`, `max_val`,
#'  `min_val`, `range_val`, `total_ct`, `analysis_eligible`
#'  the `analysis_eligible` will indicate whether the group for which the user
#'  wishes to detect an anomaly for is eligible for analysis.
#'
#'  The following conditions will disqualify a group from the anomaly detection analysis:
#'  (1) Sample size < 5 in group
#'  (2) Mean < 0.02 or Median < 0.01
#'  (3) Mean value < 0.05 and range <0.01
#'  (4) Coefficient of variance <0.01 and sample size <11
#'


compute_dist_anomalies <- function(df_tbl,
                                   grp_vars,
                                   var_col){

  site_rows <-
    df_tbl %>% ungroup() %>% select(site) %>% distinct()
  grpd_vars_tbl <- df_tbl %>% ungroup() %>% select(!!!syms(grp_vars)) %>% distinct()

  tbl_new <-
    cross_join(site_rows,
               grpd_vars_tbl) %>%
    left_join(df_tbl) %>%
    mutate(across(where(is.numeric), ~replace_na(.x,0)))


  stats <- tbl_new %>%
    group_by(!!!syms(grp_vars))%>%
    summarise(mean_val=mean(!!!syms(var_col)),
              median_val=median(!!!syms(var_col)),
              sd_val=sd(!!!syms(var_col), na.rm=TRUE),
              mad_val=mad(!!!syms(var_col)),
              cov_val=sd(!!!syms(var_col),na.rm=TRUE)/mean(!!!syms(var_col)),
              max_val=max(!!!syms(var_col)),
              min_val=min(!!!syms(var_col)),
              range_val=max_val-min_val,
              total_ct=n()) %>% ungroup() %>%
    ungroup() %>% mutate(analysis_eligible =
                           case_when(mean_val < 0.02 | median_val < 0.01 |
                                       (mean_val < 0.05 & range_val < 0.1) |
                                       (cov_val < 0.1 & total_ct < 11) ~ 'no',
                                     TRUE ~ 'yes'))
  final <- tbl_new %>% left_join(stats,
                                 by=c(grp_vars))

  return(final)


}

#' Computes anomaly detection for a group (e.g., multi-site analysis)
#' Assumes: (1) No time component; (2) Table has a column indicating
#' whether a particular group or row is eligible for analysis; (3) column
#' variable exists for which to compute the anomaly
#'
#' @param df_tbl tbl for analysis; usually output from `compute_dist_anomalies`
#' @param tail_input whether to detect anomaly on right, left, or both sides; defaults to `both`
#' @param p_input the threshold for anomaly; defaults to 0.9
#' @param column_analysis a string, which the name of the column for which to compute anomaly detection;
#' @param column_variable a string, which is the name of the variable to compute summary statistics for;
#' @param column_eligible a string, which is the name of the column that indicates eligibility for analysis
#'

detect_outliers <- function(df_tbl,
                            tail_input = 'both',
                            p_input = 0.9,
                            column_analysis = 'prop_concept',
                            column_eligible = 'analysis_eligible',
                            column_variable = 'concept_id') {

  final <- list()

  eligible_outliers <-
    df_tbl %>% filter(!! sym(column_eligible) == 'yes')

  if(nrow(eligible_outliers) == 0){

    output_final_all <- df_tbl %>% mutate(anomaly_yn = 'no outlier in group')

    cli::cli_warn('No variables were eligible for anomaly detection analysis')

  }else{

    groups_analysis <- group_split(eligible_outliers %>% unite(facet_col, !!!syms(column_variable), sep = '_', remove = FALSE) %>%
                                     group_by(facet_col))

    for(i in 1:length(groups_analysis)) {

      # filtered <-
      #   eligible_outliers %>% filter(!!! syms(column_variable) == i)

      vector_outliers <-
        groups_analysis[[i]] %>% select(!! sym(column_analysis)) %>% pull()

      outliers_test <-
        hotspots::outliers(x=vector_outliers, p=p_input, tail= tail_input)

      output <- groups_analysis[[i]] %>% mutate(
        lower_tail = outliers_test[[10]],
        upper_tail = outliers_test[[9]]
      ) %>% mutate(anomaly_yn = case_when(!! sym(column_analysis) < lower_tail |
                                            !! sym(column_analysis) > upper_tail ~ 'outlier',
                                          TRUE ~ 'not outlier'))

      final[[i]] <- output


    }

    final

    output_final_anomaly <- purrr::reduce(.x=final,
                                          .f=dplyr::union)

    output_final_all <- df_tbl %>% left_join(output_final_anomaly) %>%
      mutate(anomaly_yn=case_when(
        is.na(anomaly_yn) ~ 'no outlier in group',
        TRUE ~ anomaly_yn
      )) %>% select(-facet_col)
  }

  return(output_final_all)
}
