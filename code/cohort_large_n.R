

#' Add summary columns for a large N of sites
#'
#' @param dq_output the output table from processing to which the columns
#'                  should be added
#' @param num_col the numeric column to be used to compute the statistics
#' @param grp_vars grouping variables for the computation
#' @param check_string string indicating the check type
#' @param time a logical indicating whether the check is time dependent
#' @param shape 'long' or 'wide' indicating whether summary statistics should be separate columns (wide) or assigned to separate rows, with the name of the summary statistic in the site column (long)
#'
#' @return a dataframe with summary statistics for the numeric column
#'         provided. contains all columns from `dq_output` plus:
#'
#'         if time = `TRUE`, the mean and median are returned.
#'
#'         if time = `FALSE`, the max, min, q1, q3, mean, and median are returned
#'
#'         These values are treated as "sites" to assist with Shiny implementation
#'
summarize_large_n <- function(dq_output,
                              num_col,
                              grp_vars,
                              check_string,
                              time = FALSE,
                              shape){

  if(!time){

    # summary values already computed for anomaly detection thresholds
    if(check_string=='ecp'){dq_output<-dq_output%>%select(-c(mean_val, median_val,
                                                             max_val, min_val))}
    if(check_string == 'vc' || check_string == 'vs'){
      denoms <- dq_output %>% distinct(check_type, table_application,
                                       total_denom_ct) %>%
        group_by(table_application, check_type) %>%
        summarise(allsite_denom = sum(total_denom_ct))

      newprops <- dq_output %>%
        distinct(table_application, measurement_column,
                 vocabulary_id, total_denom_ct, tot_ct,
                 check_type) %>%
        group_by(check_type, table_application, measurement_column, vocabulary_id) %>%
        summarise(allsite_ct = sum(tot_ct)) %>%
        left_join(denoms) %>%
        mutate(allsite_prop = round(allsite_ct / allsite_denom, 3)) %>%
        pivot_longer(cols = allsite_prop,
                     names_to = 'site',
                     values_to = 'tot_prop') %>% select(-c(allsite_ct, allsite_denom)) %>%
        collect()

      final_dat <- dq_output %>%
        collect() %>%
        bind_rows(newprops)


    }else{
      summ_dat <- dq_output %>%
        group_by(!!!syms(grp_vars)) %>%
        summarise(max_val = max(!!sym(num_col)),
                  min_val = min(!!sym(num_col)),
                  q1 = quantile(!!sym(num_col), 0.25),
                  q3 = quantile(!!sym(num_col), 0.75),
                  mean_val = mean(!!sym(num_col)),
                  median_val = median(!!sym(num_col)))%>%collect()
      if(shape=='long'){
        summ_dat<-summ_dat%>%
          pivot_longer(cols = c(max_val, min_val, q1, q3, mean_val, median_val),
                     names_to = 'site',
                     values_to = num_col)

        final_dat <- dq_output %>%
          collect() %>%
          bind_rows(summ_dat)
      }else if(shape=='wide'){
        final_dat <- dq_output %>%
          collect() %>%
          left_join(summ_dat)
      }
    }

  }else{

    summ_dat <- dq_output %>%
      group_by(!!!syms(grp_vars)) %>%
      summarise(mean_val = mean(!!sym(num_col)),
                median_val = median(!!sym(num_col))) %>%
      pivot_longer(cols = c(mean_val, median_val),
                   names_to = 'site',
                   values_to = num_col) %>% collect()

    final_dat <- dq_output %>%
      collect() %>%
      bind_rows(summ_dat)
  }

  return(final_dat)

}
