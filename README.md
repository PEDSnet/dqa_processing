# PEDSnet Data Quality Processing

## Purpose

The code in this repository transforms the data from [dqa_library](https://github.com/PEDSnet/dqa_library) into a format more easily read by [dqa_shiny](https://github.com/PEDSnet/dqa_shiny) to reduce computation required to display visualizations on the PEDSnet Data Quality Dashboard and to generate threshold-based performance measures to feed back to individual institutions.

## How to Use

### System Requirements

The code was developed on R version 4.2.0 (2022-04-22). To execute the R code in this repository, users will need to install the packages named in the top lines of [driver.R](code/driver.R). 

### Data Requirements

The data is expected to be in the format of the output from the [dqa_library](https://github.com/PEDSnet/dqa_library) step of the PEDSnet DQA process.

### Execution Process

1) Set up configurations for execution of PEDSnet standardized R framework code in [run.R](site/run.R) and [site_info.R](site/site_info.R), including setting up srcr .json config file to successfully establish connection to the database containing the DQA results.
2) Edit run.R:
    - `config('results_schema', 'dqa_rox')`: change 'dqa_rox' to the name of the schema containing the DQA output 
    - `config('new_site_pp',FALSE)`: if running against all sites, set to `FALSE`. If there is already output for some sites and you want to add in another site, set to `TRUE`
    - `config('results_schema_other', NA)`: if running against all sites, set to `NA`. If there is already output for some sites and you want to add in another site, set to the name of the results schema containing the output of dqa_library for the other site
    - `config('results_name_tag', NA)`: if the suffix on the tables output from dqa_library is anything other than the default, change to the suffix on the tables. By default, set to `NA` to expect no suffix on the table names
    - `config('current_version', 'vxx')`: change 'vxx' to the name of the current version of the data. Should match the name assigned in the DQA library step in the column database_version in the output
    - `config('previous_version', 'vyy')`: change 'vyy' to the name of the previous version of the data. Should match the name assigned in the DQA library step in the column database_version in the output
3) Edit site_info.R:
    - `config('db_src')` should set up the connection to the database containing the schema with the data output from dqa_library where you will also output the results from processing. 
    - You will also need to establish a connection with the database containing output from `dqa_redcap`, specifically the table `dqa_issues_redcap`. `config('db_src_prev')` will do this for you, but you need to make sure the connection information is either in a file named `config_dqa_prev.json` or, if it is contained in a file with another name, the environmental variable `PEDSNET_DB_SRC_CONFIG_BASE_PREV` is set to the name of the file. This can be done either in the console or in your .Rprofile. It is assumed that the results from the previous DQ run are in a schema named `dqa_rox`. If this is not the case, this needs to be edited in the call to generate redcap_prev in [driver.R](code/driver.R)
4) Either set `config('execution_mode', '')` to `production` in [run.R](site/run.R) or set to `development` and highlight the contents within .run{} in [driver.R](code/driver.R)

### Code Description

All of the processing steps execute through the [driver.R](code/driver.R) file. This code:

  1. Establishes thresholds to apply to the DQ output based on standard PEDSnet thresholds or a site-specific threshold that has been established, if one exists.
  2. Generates and tracks a history of threshold values.
  3. Generates at least one table for each table output from [dqa_library](https://github.com/PEDSnet/dqa_library), containing a version of the data with post-processing steps applied, and outputs each table with the suffix `pp`.
      - The tables with a `pp` suffix are the ones accessed by the dashboard code
  4. Generates a version of each of the `pp` tables in a format in which the thresholds should be applied and outputs each table with the prefix `thr`.
  5. Applies the thresholds established in step 1 to the tables generated in step 4 and outputs violations that were not previously indicated as issues to stop raising. An indicator for whether to continue or stop raising a consistent issue across cycles is pulled from the REDCap review form.
  6. Generates an anonymous site identifier for each site name in the `pp` tables and creates a column in each of the `pp` tables with the anonymous identifier.

