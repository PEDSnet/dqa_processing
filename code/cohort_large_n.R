

#' Add summary columns for a large N of sites
#'
#' @param dq_output the output table from processing to which the columns 
#'                  should be added
#' @param num_col the numeric column to be used to compute the statistics
#' @param grp_vars grouping variables for the computation
#' @param time a logical indicating whether the check is time dependent
#'
#' @return a dataframe with summary statistics for the numeric column
#'         provided. contains all columns from `dq_output` plus:
#'         
#'         if time = `TRUE`, the mean and median are returned.
#'         
#'         if time = `FALSE`, the max, min, q1, q3, mean, and median are returned 
#' 
summarize_large_n <- function(dq_output,
                              num_col,
                              grp_vars,
                              time = FALSE){
  
  if(!time){
    
    summ_dat <- dq_output %>%
      group_by(!!!syms(grp_vars)) %>%
      summarise(max_val = max(!!sym(num_col)),
                min_val = min(!!sym(num_col)),
                q1 = quantile(!!sym(num_col), 0.25),
                q3 = quantile(!!sym(num_col), 0.75),
                mean_val = mean(!!sym(num_col)),
                median_val = median(!!sym(num_col)))
    
    final_dat <- dq_output %>%
      left_join(summ_dat)
    
  }else{
    
    summ_dat <- dq_output %>%
      group_by(!!!syms(grp_vars)) %>%
      summarise(mean_val = mean(!!sym(num_col)),
                median_val = median(!!sym(num_col)))
    
    final_dat <- dq_output %>%
      left_join(summ_dat)
  }
  
  
}