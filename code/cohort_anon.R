#' use output_list_to_db to append to each of the tables:
#' output_list_to_db(output_list) where output_list is a list of tables
#'

#' Function to add a column to each table in a list of tables
#'           with a masked site identifier
#' @param all_sites_tbl a tbl that contains a column called `site`
#'                        with each of the sites that you want to mask
#'                        in all the other tables
#' @param tbls_to_anon a list of all of the tables that you want to add the masked column to
#' @return each of the original tables in `tbls_to_anon` with the original columns
#'           plus a column called `site_anon` with a masked identifier
#'           that is consistent across all of the tables in `tbls_to_anon`
attach_anon_id <- function(all_sites_tbl,
                           tbls_to_anon){

  # generate map for site anonymization
  distinct_sites <- all_sites_tbl %>%
    distinct(site) %>% collect()
  site_nums <- distinct_sites[sample(1:nrow(distinct_sites)),]%>%
      mutate(sitenum=row_number(),
             site_anon=paste0("site ", sitenum))

  # join all tables to the site map
  tbls_all <- list()
  for(i in 1:nrow(tbls_to_anon)){
    rslt_name <- tbls_to_anon[i,]

    if("site_anon" %in% colnames(results_tbl(rslt_name))){
      anoned_tbl_pre <- results_tbl(rslt_name)%>%
        select(-c(site_anon, sitenum))
    }
    else{
      anoned_tbl_pre <- results_tbl(rslt_name)
    }
    anoned_tbl <- anoned_tbl_pre %>%
      left_join(site_nums,
                by='site',
                copy=TRUE)%>%
      mutate(site_anon=coalesce(site_anon,site)) # bring in "all" or "total" rows

    tbls_all[[paste0(rslt_name)]] <- anoned_tbl
  }
  return(tbls_all)
}

#' Function to pull all table names with 'site' column for only the post-processed tables
#' @param db a database connection
#' @param schema_name a schema name containing the results tables to search
#'
#' @return data.frame with a column `table`
#'             containing all table names with 'site' column
#'

pull_site_tables <- function(db=config('db_src'),
                                 schema_name=config('results_schema')) {
  # pull all results table names from schema
  tbl_names <-
    db %>%
    DBI::dbListObjects(DBI::Id(schema= schema_name)) %>%
    dplyr::pull(table) %>%
    purrr::map(~slot(.x, 'name')) %>%
    dplyr::bind_rows()%>%
    filter(! str_detect(table,'redcap'))%>%
    filter(str_detect(table, 'pp'))

  tbl_names_short <-
    tbl_names %>%
    mutate_all(~gsub(paste0(config('results_name_tag')),'',.))

  # find all table names with site column
  rslt<-matrix()
  for(i in 1:nrow(tbl_names_short)){
    rslt_name <- tbl_names_short[i,]%>%
      select(table)%>%
      pull()

    if(any(colnames(results_tbl(rslt_name)) == 'site')) {
      rslt[i]<-rslt_name
    }
  }
  return(data.frame(rslt)%>%filter(!is.na(rslt))%>%rename(table=rslt))
}

#' output a list of tables to the database
#'    collecting the tables due to error from trying to write a computed table
#'
#' @param output_list list of tables to output
#' @param append logical to determine if you want to append if the table exists
#'
#' @return tables output to the database; if
#' table already exists, it will be appended
#'

output_list_to_db_collect <- function(output_list,
                              append=TRUE) {


  if(append) {

    for(i in 1:length(output_list)) {

      output_tbl_append(data=output_list[[i]]%>%collect_new(),
                        name=names(output_list[i]))

    }

  } else {

    for(i in 1:length(output_list)) {

      output_tbl(data=output_list[[i]]%>%collect_new(),
                 name=names(output_list[i]))

    }

  }

}
