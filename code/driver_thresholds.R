# Anomaly detection for thresholds ----
# BMC ----
message('BMC anomaly thresholds')
bmc_thresh<-results_tbl('bmc_anom_pp')%>%distinct(check_type, check_name, site,
                                                  lower_tail, upper_tail)%>%
  collect()%>%
  pivot_longer(cols=c(lower_tail, upper_tail),
               names_to="threshold_operator",
               values_to="threshold",
               values_drop_na=TRUE)%>%
  mutate(threshold_operator=case_when(threshold_operator=='lower_tail'~'lt',
                                      threshold_operator=='upper_tail'~'gt'),
         check_name_app=paste0(check_name, '_rows'),
         application='rows')%>%
  filter(threshold_operator=='lt')

# ECP ----
message("ECP anomaly thresholds")
ecp_thresh<-results_tbl('ecp_anom_pp')%>%distinct(check_type, check_name, site,
                                                  lower_tail, upper_tail)%>%
  pivot_longer(cols=c(lower_tail, upper_tail),
               names_to="threshold_operator",
               values_to="threshold",
               values_drop_na=TRUE)%>%
  mutate(threshold_operator=case_when(threshold_operator=='lower_tail'~'lt',
                                      threshold_operator=='upper_tail'~'gt'),
         check_name_app=paste0(check_name, '_person'),
         application='person')%>%
  filter(threshold_operator=='lt')%>%
  collect()
# DP ----
message("Date plausibility thresholds")
dp_thresh<-results_tbl('dp_anom_pp')%>%distinct(check_type, check_name, site,
                                                lower_tail, upper_tail, check_name_app)%>%
  pivot_longer(cols=c(lower_tail, upper_tail),
               names_to="threshold_operator",
               values_to="threshold",
               values_drop_na=TRUE)%>%
  mutate(threshold_operator=case_when(threshold_operator=='lower_tail'~'lt',
                                      threshold_operator=='upper_tail'~'gt'),
         application='rows')%>%
  filter(threshold_operator=='gt')%>%
  collect()


# default thresholds -----
thresholds_standard<-format_default_thresholds(std_thresholds=read_codeset('threshold_limits', 'cccdc'),
                                               anom_thresholds=bind_rows(bmc_thresh,ecp_thresh)%>%
                                                 bind_rows(dp_thresh))

message('Finding previous thresholds')
# Find n-1 thresholds for those that should be re-set or stop flagging
redcap_prev <- get_argos_default()$qual_tbl(name='dqa_issues_redcap',
                                            schema_tag='dqa_rox',
                                            db=config('db_src_prev'))%>%
  mutate(threshold_operator=case_when(threshop=='greater than'~'gt',
                                      threshop=='less than'~'lt'),
         rc_finalflag=case_when(finalflag=='Continue to flag'~1L,
                                finalflag=='Stop flagging'~2L,
                                finalflag=='Continue flagging with new threshold'~3L,
                                finalflag=='Other'~4L))%>%
  # bring in ndq issues that should stop being flagged or assigned new thresholds
  filter(rc_finalflag%in%c(2L,3L)|
  # and ones from prior ssdqa to keep flagging
           (rc_finalflag==1L&app=='ssdqa'))%>%
  collect()

thresholds_history<-get_argos_default()$qual_tbl(name='thresholds_history',
                                                 schema_tag='dqa_rox',
                                                 db=config('db_src_prev'))%>%collect()


thresholds_this_version<-determine_thresholds(default_thresholds=thresholds_standard,
                                              newset_thresholds=redcap_prev,
                                              history_thresholds=thresholds_history)
# replace uc_procsall-scid with the same as the site's threshold for uc_procsall
thresholds_procsall<-thresholds_this_version%>%filter(check_name_app=='uc_procsall_rows')%>%
  select(site, threshold)%>%
  rename(t_new=threshold)
thresholds_this_version<-thresholds_this_version%>%
  left_join(thresholds_procsall, by = 'site')%>%
  mutate(threshold=case_when(check_name_app=='uc_procsall-scid_rows'~t_new,
                             TRUE~threshold))%>%
  select(-t_new)
message('Creating table to track threshold versions')
thresholds_history_new <- bind_rows(thresholds_this_version,
                                    thresholds_history)
output_tbl(thresholds_history_new,
           name='thresholds_history')

message('Create threshold table')
thresholds_applied <- apply_thresholds(check_app_tbl=read_codeset('check_apps', col_types = 'cccccc'),
                                       threshold_tbl = thresholds_this_version)


output_list_to_db(thresholds_applied,
                  append=FALSE)

threshold_violations <- reduce(.x=thresholds_applied,
                               .f=dplyr::bind_rows)%>%
  filter(violation&
          (is.na(rc_finalflag)|rc_finalflag!=2))

output_tbl(threshold_violations,
            name='threshold_tbl_violations')
