


#' pull dqa table names
#'
#' @return a two-column tibble: `schema`, `table`
#' will return with results schema removed
#'

pull_dqa_table_names <- function(db=config('db_src'),
                                 schema_name=config('results_schema')) {



  #' pulls table names from database
  tbl_names <-
    db %>%
    DBI::dbListObjects(DBI::Id(schema= schema_name)) %>%
    dplyr::pull(table) %>%
    purrr::map(~slot(.x, 'name')) %>%
    dplyr::bind_rows() %>%
    # select(! matches('*_pp_*'))
    filter(! str_detect(table,'pp')) %>%
    filter(str_detect(table,'output|violations')) %>%
    filter(! str_detect(table, 'fot|dcon')) %>%
    filter(! str_detect(table, 'thrshld')) %>%
    filter(! str_detect(table, 'old'))


  #' will remove the results name tag from table names
  tbl_names_short <-
    tbl_names %>%
    mutate_all(~gsub(paste0(config('results_name_tag')),'',.))



}


#' pull dqa tables
#'
#' @param tbl_names output from `pull_dqa_table_names`
#' @return list of tables with dqa results for each element
#'

pull_dqa_tables <- function(tbl_names) {


  #' pulls all table results out
  tbls_all <- list()


  for(i in 1:nrow(tbl_names)) {

    string_name <-
      tbl_names[i,] %>%
      select(table) %>%
      pull()


    if(config('new_site_pp')) {
      tbl_dq <-
        results_tbl_other(string_name) %>% collect()
    } else {tbl_dq <-
      results_tbl(string_name) %>% collect()}


    tbls_all[[paste0(string_name)]] <- tbl_dq

  }

  tbls_all

}

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


#' reads in thresholds from the specs folder
#'
#' @param check_tbl output from `get_check_names`
#' @param threshold_tbl a csv file from the specs folder that
#' has all thresholds
#'
#' @return tbl with 5 columns: `site`, `check_type`, `check_name`, `threshold`, `threshold_operator`
#'
#'

set_broad_thresholds <- function(check_tbl,
                                 threshold_tbl=read_codeset('threshold_limits_test',
                                                            col_types='ccdc')) {

  checks <-
    check_tbl %>%
    inner_join(threshold_tbl,
               by=c('check_type','check_name'),
               multiple='all') %>%
    filter(! site == 'unknown',
           ! site == 'total')


}


#' integrates redcap output with thresholds
#'
#' @param redcap_tbl the redcap tbl with new thresholds
#' @param threshold_tbl the tbl with the PEDSnet thresholds; should be in `specs`
#' @param site_name_tbl a csv with all PEDSnet site names; should be in `specs`
#'
#' @return will output a tbl with `check_type`, `check_name`, `check_name_app`,
#' `threshold`, `threshold_operator`, `application`, `site`, `oldthreshold`, `newthreshold`
#'
#' The `oldthreshold` column provides the previous threshold, and the `newthreshold`
#' column shows the new threshold.
#'

compute_new_thresholds <- function(redcap_tbl,
                                   previous_thresholds,
                                   threshold_tbl=read_codeset('threshold_limits','ccdcc'),
                                   site_name_tbl=read_codeset('site_names',col_types = 'c')) {

  thresholds_sites <-
    merge(threshold_tbl,site_name_tbl) %>% tibble::as_tibble(.) %>%
     unite('check_name_app',c(check_name,application),
           sep='_',remove=FALSE)

  # thresholds_sites_db <-
  #  copy_to_new(df=thresholds_sites)

  version_num <- config('previous_version')
  version_num_current <- config('current_version')

  thresholds_previous <-
    previous_thresholds %>%
    select(-c(threshold_previous)) %>% 
    rename(threshold_previous = newthreshold) %>%
    select(check_type,
           check_name_app,
           check_name,
           threshold_previous,
           ### below is new
           threshold_version_global,
           application,
           site,
           threshold_version)

  thresholds_previous_merged <-
 #   thresholds_sites_db %>%
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
                       TRUE ~ threshold_version),
           threshold_previous = 
             case_when(is.na(threshold_previous) ~ threshold,
                       TRUE ~ threshold_previous))
  
  #%>% 
    # mutate(threshold_version_global = case_when(abs()))
    # mutate(
    #   threshold_version_global = case_when(abs(threshold-threshold_previous) > 0.01 ~ version_num,
    #                                        TRUE ~ threshold_version_global),
    #                                        #TRUE ~ 'standard'),
    #   threshold=case_when(abs(threshold-threshold_previous) > 0.01 ~ threshold_previous,
    #                       TRUE ~ threshold)
    # )

  #' TO DISCUSS WITH KIM: 
  #' - Should we call `threshold` something else?
  #' - What to do with the `finalflag` column when the value is `Stop flagging`
  #' - How to preserve a running copy of thresholds, and redcap responses?
  redcap_new <-
    redcap_tbl %>%
    mutate(newthreshold=
             case_when(is.na(newthreshold) ~ threshold,
                       TRUE ~ newthreshold)) %>%
    mutate(newthreshold=as.double(newthreshold),
           threshold=as.double(threshold)) %>%
    filter(version==version_num) %>%
    select(site,
           check_name,
           check_name_app,
           threshold,
           newthreshold) %>%
    rename(oldthreshold=threshold)

  new_thresholds <-
    thresholds_previous_merged %>%
    left_join(
      redcap_new,
      by=c('site','check_name','check_name_app'),
      copy=TRUE) %>% 
   #' NEW:  look to `threshold_previous` rather than just `threshold`
    mutate(oldthreshold=
             case_when(is.na(oldthreshold) ~ threshold_previous,
                       TRUE ~ oldthreshold)) %>%
    mutate(threshold_version_global=
             case_when(!is.na(newthreshold) & abs(newthreshold-oldthreshold) > 0.01 ~ version_num_current,
                       TRUE ~ threshold_version_global)) %>%
    mutate(newthreshold=
             case_when(is.na(newthreshold) ~ oldthreshold,
                       TRUE ~ newthreshold)) %>%
    mutate(threshold_version = version_num_current) %>% 
    # PROBABLY REMOVE THIS, but for now trying to just have one row per threshold (up to here there could be more than one newthreshold from the previous redcap)
    group_by(site, check_name_app)%>%filter(newthreshold==max(newthreshold))%>%ungroup()%>%distinct()

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

   tbls <- pull_dqa_table_names_post() #%>%
     #filter(!table == 'bmc_rxnorm_output_pp')

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
                                    #thresholds=results_tbl('thresholds'),
                                    db_input=config('db_src'),
                                    threshold_apps=read_codeset('check_apps', col_types = 'cccc')) {


   otpt <- list()

   otpt$dqa_tbl_names <- pull_dqa_table_names(db=db_input,
                                              schema_name=schema_name_input)
   otpt$dqa_tbls <- pull_dqa_tables(otpt$dqa_tbl_names)
   otpt$check_names <- get_check_names(otpt$dqa_tbl_names)
   #otpt$thresholds <- set_broad_thresholds(check_tbl=otpt$check_names)
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
     select(site, check_name, check_name_app, threshold, threshold_operator, threshold_version)

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
       select(site, threshold, threshold_operator, check_name, check_name_app, threshold_version, value_output, violation)%>%
       collect()


     tbls_all[[paste0("thr_",string_name,"_",app_name)]] <- tbl_dq
   }
   tbls_all
 }
