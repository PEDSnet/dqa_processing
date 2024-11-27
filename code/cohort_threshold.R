#' appends check_name and check_type and site to table names
#'
#' @param tbl_names_df output from `pull_dqa_table_names`
#' @return A tibble with 3 columns: `site`,`check_type`,`check_name`
#'

get_check_names <- function(tbl_names) {


  final <- list()

  for(i in 1:nrow(tbl_names)) {

    string_name <-
      tbl_names[i,] %>%
      select(table) %>%
      pull()

  if(config('new_site_pp')) {

    if(any(colnames(results_tbl_other(paste0(string_name))) == 'check_type')) {
      tbl_current <- results_tbl_other(paste0(string_name))
    } else {tbl_current <- results_tbl_other(paste0(string_name)) %>%
      mutate(check_type = 'unknown')}

    if(any(colnames(results_tbl_other(paste0(string_name))) == 'check_name')) {
      tbl_current <- tbl_current
    } else {tbl_current <- tbl_current %>%
      mutate(check_name = 'unknown')}

    if(any(colnames(results_tbl_other(paste0(string_name))) == 'site')) {
      tbl_current <- tbl_current
    } else {tbl_current <- tbl_current %>%
      mutate(site = 'unknown')}

  } else {

    if(any(colnames(results_tbl(paste0(string_name))) == 'check_type')) {
      tbl_current <- results_tbl(paste0(string_name))
    } else {tbl_current <- results_tbl(paste0(string_name)) %>%
      mutate(check_type = 'unknown')}

    if(any(colnames(results_tbl(paste0(string_name))) == 'check_name')) {
      tbl_current <- tbl_current
    } else {tbl_current <- tbl_current %>%
      mutate(check_name = 'unknown')}

    if(any(colnames(results_tbl(paste0(string_name))) == 'site')) {
      tbl_current <- tbl_current
    } else {tbl_current <- tbl_current %>%
      mutate(site = 'unknown')}

  }




    check_name_tbl <-
      tbl_current %>%
      select(site,
             check_type,
             check_name) %>%
      distinct() %>% collect() %>% filter(! check_type == 'unknown',
                                          ! check_name == 'unknown',
                                          ! site == 'unknown')

    final[[i]] <- check_name_tbl

  }

  final_reduce <-
    reduce(.x=final,
           .f=dplyr::union)

  final_reduce
}

#' integrates redcap output with thresholds
#'
#' @param redcap_tbl the redcap tbl with new thresholds
#' @param previous_thresholds tbl with (site-specific) thresholds from previous data cycle
#' @param threshold_tbl the tbl with the PEDSnet thresholds; should be in `specs`
#' @param site_name_tbl a csv with all PEDSnet site names; should be in `specs`
#'
#' @return will output a tbl with `check_type`, `check_name`, `check_name_app`,
#' `threshold`, `threshold_operator`, `application`, `site`, `oldthreshold`, `newthreshold`
#'
#' The `oldthreshold` column provides the previous threshold, and the `newthreshold`
#' column shows the new threshold. The `rc_stop_flag` column indicates that
#' the threshold can be applied, but the issue should not be raised with the site (TRUE if should stop flagging)
#'

compute_new_thresholds <- function(redcap_tbl,
                                   previous_thresholds,
                                   threshold_tbl=read_codeset('threshold_limits','ccdcc'),
                                   site_name_tbl=read_codeset('site_names',col_types = 'c'),
                                   anomaly_tbl) {

  # apply standard thresholds to each of the sites
  thresholds_sites <-
    merge(threshold_tbl,site_name_tbl) %>% tibble::as_tibble(.) %>%
     unite('check_name_app',c(check_name,application),
           sep='_',remove=FALSE) %>%
    rename(threshold_pedsnet_default=threshold)

  version_num <- config('previous_version')
  version_num_current <- config('current_version')

  # format previous thresholds
  thresholds_previous <-
    previous_thresholds %>%
    select(-c(threshold_previous)) %>%
    rename(threshold_previous = threshold) %>%
    select(check_type,
           check_name_app,
           check_name,
           threshold_previous,
           threshold_version_global,
           application,
           site,
           threshold_version,
           threshold_operator)

  # thresholds from anomaly detection
  anomaly_thresholds<-anomaly_tbl%>%
    mutate(threshold_version=version_num_current,
           threshold_version_global=version_num_current)

  # bring in the prior redcap data and replace thresholds if they were adjusted in prior cycle
  thresholds_previous_merged <-
    thresholds_sites %>%
    left_join(
      thresholds_previous,
      copy=TRUE
    ) %>% distinct() %>%
    mutate(threshold_version_global =
             case_when(is.na(threshold_version_global) ~ 'standard',
                       TRUE ~ threshold_version_global)) %>%
    mutate(threshold_version =
             case_when(is.na(threshold_version) ~ version_num_current,
                       TRUE ~ threshold_version)) %>%
    bind_rows(anomaly_thresholds)


  redcap_new <-
    redcap_tbl %>%
    mutate(threshold_rc=
             case_when(!is.na(newthreshold) ~ as.numeric(newthreshold),
                       TRUE ~ NA_real_)) %>%
    rename(threshold_version_rc=version)%>%
    # PROBABLY WANT TO REMOVE AFTER V55
    #filter(version==version_num) %>%
    select(site,
           check_name,
           check_name_app,
           threshold_rc,
           threshold_operator,
           finalflag,
           threshold_version_rc)

  new_thresholds <-
    thresholds_previous_merged %>%
    left_join(
      redcap_new,
      by=c('site','check_name','check_name_app','threshold_operator'),
      copy=TRUE) %>%
    # if there was a threshold in redcap (could be newly assigned or not, use that)
    # if there was a threshold in the previous version but not in redcap (e.g. wasn't a violation), use that
    # mutate(threshold_previous=
    #          case_when(!is.na(threshold_rc) ~ threshold_rc,
    #                    TRUE ~ threshold_prev)) %>%
    mutate(threshold=case_when(!is.na(threshold_rc)~threshold_rc,
                               !is.na(threshold)~threshold,
                               # PROBABLY WANT TO CHANGE AFTER V55
                               TRUE~threshold_pedsnet_default))%>%
   # mutate(threshold_version = version_num_current) %>%
    # take version from RC if replaced, standard if same as before, name it after current version if changed in this version
    mutate(threshold_version=case_when(!is.na(threshold_version_rc)&!is.na(threshold_rc)~threshold_version_rc,
                                       threshold_previous==threshold&!is.na(threshold_previous)~'standard',
                                       TRUE~version_num_current))%>%

    select(-threshold_rc)%>%
    mutate(rc_stop_flag=case_when(finalflag=='Stop flagging'~TRUE,
                                  TRUE~FALSE))%>%
    select(-c(finalflag,threshold_version_rc))

}

#' attaches thresholds to tables
#'
#' @param tbls_all output of `pull_dqa_tbls`
#' @param threshods output of `set_broad_thresholds`
#' @return this function will output tables to the database:
#' 1) All tables with thresholds implemented (called *_thrshld)
#' 2) Current tables without thresholds renamed (called *_thrshldno)
#' 3) Tables with thresholds impelemtned (not suffixed with anything)
#'

attach_thresholds <- function(tbls_all,
                              thresholds,
                              append_tbl_existing) {

  #' attaches thresholds to tables

  tbls_with_threshold <- list()
  tbls_with_threshold_orig <- list()

  for(i in 1:length(tbls_all)) {

    if('threshold' %in% colnames(tbls_all[[i]])) {

      tbls_all[[i]] <- tbls_all[[i]] %>%
        select(-c(threshold,threshold_operator))

    }

    if(config('new_site_pp')){
      tbl_renamed <-
        tbls_all[[i]] %>%
        collect() %>%
        output_tbl_append(paste0(names(tbls_all[i]),'thrshldno'))
    } else {
      tbl_renamed <-
        tbls_all[[i]] %>%
        collect() %>%
        output_tbl(paste0(names(tbls_all[i]),'thrshldno'))
    }

    check_for_check <-
      tbls_all[[i]] %>%
      left_join(thresholds,
                 by=c('site','check_type','check_name'),
                 copy=TRUE) %>%
      select(-c(threshold, oldthreshold)) %>%
      rename(threshold=newthreshold)

    tbls_with_threshold[[paste0(names(tbls_all[i]),'_thrshld')]] <- check_for_check
    tbls_with_threshold_orig[[paste0(names(tbls_all[i]))]] <- check_for_check

  }

  output_list_to_db(tbls_with_threshold,
                    append=append_tbl_existing)

  tbls_with_threshold_orig


}


#' pulls the tables that have threshold values
#'
#' @return a two-column tibble: `schema`, `table`
#' will return with results schema removed
#'

pull_dqa_table_names_post <- function(schema_name=config('results_schema')) {



  #' pulls table names from database
  tbl_names <-
    config('db_src') %>%
    DBI::dbListObjects(DBI::Id(schema= schema_name)) %>%
    dplyr::pull(table) %>%
    purrr::map(~slot(.x, 'name')) %>%
    dplyr::bind_rows() %>%
    # select(! matches('*_pp_*'))
    filter(str_detect(table,'_pp_|uc_output')) %>%
    filter(! str_detect(table,'uc_by_year|thrshld|byyr|dcon')) %>%
    filter(! str_detect(table, 'old')) %>%
    filter(! str_detect(table, 'mistake')) %>%
    filter(! str_detect(table, 'concepts'))

  #' will remove the results name tag from table names
  tbl_names_short <-
    tbl_names %>%
    mutate_all(~gsub(paste0(config('results_name_tag')),'',.))



}


#' creating threshold
#'
#' @param check_apps_tbl A tbl from `specs` that specifies which
#' level the check was applied. Columns are:
#'   `tbl_name_pre` | `tbl_name_post` | `check_apps_no` | `check_app_first`
#'   `check_app_second` | `check_app_third` | `first_col` | `second_col` | `third_col`
#'

 create_threshold_tbl_post <- function(check_apps_tbl=
                                         read_codeset('check_apps','ccicccccc')) {

   tbls <- pull_dqa_table_names_post()

   check_apps <- check_apps_tbl

   thrshld_tbl <- list()

   for(i in 1:nrow(tbls)) {
     string_name <-
       tbls[i,] %>%
       select(table) %>%
       pull()

       check_apps_narrow <- check_apps %>% filter(tbl_name_post==string_name)
       check_apps_narrow_firstcol <- sym(paste0(check_apps_narrow$first_col))
       check_apps_narrow_secondcol <- sym(paste0(check_apps_narrow$second_col))
       check_apps_narrow_thirdcol <- sym(paste0(check_apps_narrow$third_col))
       thrshld <- results_tbl(paste0(string_name)) %>% collect() %>%
                  mutate(check_join=string_name) %>%
                  inner_join(check_apps_narrow,by=c('check_join'='tbl_name_post')) %>%
                  purrr::discard(~all(is.na(.)))

       thrshld_new_pp <- thrshld

       for(k in 1:(check_apps_narrow$check_apps_no)) {

         if(k==1) {thrshld_new <- thrshld %>% mutate(value_output1=!!check_apps_narrow_firstcol,
                                                    check_name_app1=paste0(check_name,'_',check_app_first))}
         if(k==2) {thrshld_new <- thrshld %>% mutate(value_output2=!!check_apps_narrow_secondcol,
                                                    check_name_app2=paste0(check_name,'_',check_app_second))}
         if(k==3) {thrshld_new <- thrshld %>% mutate(value_output2=!!check_apps_narrow_secondcol,
                                                    check_name_app2=paste0(check_name,'_',check_app_second))}

         thrshld_new_pp <- thrshld_new_pp %>% left_join(thrshld_new)


       }
       thrshld_new_pp_cols <- thrshld_new_pp %>%
          select(site,threshold,
                 threshold_operator,starts_with(c('check_name','value_output')))

       if('check_name_app' %in% names(thrshld_new_pp_cols)) {thrshld_new_pp_cols <- thrshld_new_pp_cols %>% select(- check_name_app)}

       final <- pivot_longer(thrshld_new_pp_cols,
                    cols=starts_with(c('check_name_app','value_output')),
                    names_pattern='(value_output|check_name_app)(.*)',
                    names_to=c('.value','rownum'),names_repair = 'unique')  %>% select(-rownum)

       thrshld_tbl[[i]] <- final

   }


   thrshld_tbl

 }


 create_thresholds_full <- function(schema_name_input,
                                    append_tbl,
                                    threshold_input=results_tbl('thresholds'),
                                    db_input=config('db_src'),
                                    threshold_apps=read_codeset('check_apps', col_types = 'cccc')) {


   otpt <- list()

   otpt$dqa_tbl_names <- pull_dqa_table_names(db=db_input,
                                              schema_name=schema_name_input)
   otpt$dqa_tbls <- pull_dqa_tables(otpt$dqa_tbl_names)
   otpt$check_names <- get_check_names(otpt$dqa_tbl_names)
   thresholds_attached <- attach_thresholds(tbls_all=otpt$dqa_tbls,
                                            thresholds=threshold_input,
                                            append_tbl_existing=append_tbl)

   thresholds_attached

 }

 #' Function to apply thresholds to a list of post-processed tables
 #' @param check_app_tbl table with (at least) the columns:
 #'                        tbl_name_post: name of post-processed table as it exists in the results schema. This table should contain a `check_name_app` column by which it will be joined to the thresholds
 #'                        col_filter: name of column to filter thresholding on (can be NULL)
 #'                        col_filter_value: value in the `col_filter` field on which to filter results
 #' @param threshold_tbl table with (at least) the columns:
 #'                        site
 #'                        check_name_app
 #'                        threshold
 #'                        threshold_operator
 #' @return named list (where names are the names of the post-processed tables) containing the post-processed tables with the thresholds attached
 apply_thresholds <- function(check_app_tbl,
                              threshold_tbl){
   threshold_tbl_limited <- threshold_tbl %>%
     select(site, check_name, check_name_app, threshold, threshold_operator, rc_finalflag)

   tbls_to_apply<-check_app_tbl %>%
     select(tbl_name_post) %>%
     pull()

   tbls_all <- list()

   for(i in 1:length(tbls_to_apply)) {

     string_name<-tbls_to_apply[i]
     app_name<-check_app_tbl$check_app[i]
     # find the output pp table
     if(config('new_site_pp')) {
       tbl_dq_post <-
         results_tbl_other(string_name)
     }else{
       tbl_dq_post <- results_tbl(string_name)
     }

     # apply filter if specified
     if(!is.na(check_app_tbl$col_filter[i])){
       filt_col<-check_app_tbl$col_filter[i]
       filt_string<-check_app_tbl$col_filter_value[i]
       tbl_dq_post<-tbl_dq_post%>%filter(as.character(!!sym(filt_col))==filt_string)
       # select(-!!sym(filt_col)) # could keep this in and it works, but not sure if needed
     }else{tbl_dq_post<-tbl_dq_post}

     # join final table to the thresholds
     tbl_dq <- tbl_dq_post %>%
       left_join(threshold_tbl_limited, by = c('site', 'check_name_app', 'check_name'), copy=TRUE)%>%
       rename(value_output:=!!sym(check_app_tbl$col_name[i]))%>%
       mutate(violation=case_when(threshold_operator=='gt'&
                                    value_output>threshold~TRUE,
                                  threshold_operator=='lt'&
                                    value_output<threshold~TRUE,
                                          TRUE~FALSE))%>%
       select(site, threshold, threshold_operator, check_name, check_name_app, value_output, violation, rc_finalflag)%>%
       collect()


     tbls_all[[paste0("thr_",string_name,"_",app_name)]] <- tbl_dq
   }
   tbls_all
 }

#' Function to format the `thresholds_limits` file to feed forward into tracking
#' @param std_thresholds standard thresholds file, with the cols:
#'          check_type
#'          check_name
#'          threshold
#'          threshold_operator
#'          application
#' @return table with the original columns +:
#'          check_name_app
#'          database_version
#'          site
format_default_thresholds<-function(std_thresholds,
                                    anom_thresholds){
  std_thresholds%>%
    unite('check_name_app',c(check_name,application), sep='_',remove=FALSE) %>%
    bind_rows(anom_thresholds%>%select(-site,-application)%>%distinct())%>%
    select(-application)%>%
    mutate(database_version=config('current_version'),
           site='default')
}

#' Function to determine the thresholds to be used in this version
#' @param default_thresholds thresholds that are set across all sites for all applicable checks, including those for anomaly detection
#' @param newset_thresholds thresholds that were set in the version 1 prior
#' @param history_thresholds table containing all the prior thresholds,
#'                in order to assess
determine_thresholds<-function(default_thresholds,
                               newset_thresholds,
                               history_thresholds,
                               site_name_tbl=read_codeset('site_names',col_types = 'c')){
  # apply standard thresholds to each of the sites
  default_thresholds_sites <-
    merge(default_thresholds%>%select(-site),site_name_tbl) %>% tibble::as_tibble()

  # check to see if a new threshold was set 1 version back (newset_thresholds)
  newset_thresholds_pp<-newset_thresholds%>%
    distinct(site, check_name_app, newthreshold, threshold_operator, rc_finalflag)%>%
    mutate(newthreshold=case_when(rc_finalflag==3L~as.numeric(newthreshold),
                                  TRUE~NA_real_))%>%
    rename(rc_finalflag_n1=rc_finalflag)

  # check to see if a threshold was assigned any number of versions ago
  # if threshold set more than one time, use the most recent
  history_thresholds_pp<-history_thresholds%>%
    group_by(site, check_name_app, threshold_operator)%>%
    filter(database_version==max(database_version, na.rm=TRUE))%>%
    ungroup()%>%
    select(site, check_name_app, threshold_operator,threshold,rc_finalflag)%>%
    rename(prevthreshold=threshold,
           rc_finalflag_prev=rc_finalflag)

  # put them together and chose one to use
  thresh_reset<-default_thresholds_sites%>%
    left_join(newset_thresholds_pp, by = c('site','check_name_app','threshold_operator'))%>%
    left_join(history_thresholds_pp, by = c('site', 'check_name_app','threshold_operator'))%>%
    mutate(nt=coalesce(newthreshold,prevthreshold,threshold),
           # take the most recent flag for whether to flag or reset threshold
           rc_finalflag=coalesce(rc_finalflag_n1, rc_finalflag_prev))%>%
    distinct(check_type, check_name_app, check_name, threshold_operator, database_version, site, nt, rc_finalflag)%>%
    rename(threshold=nt)%>%
    bind_rows(default_thresholds)
}

