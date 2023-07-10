#' Create a new step log for results and progress info
#'
#' This function sets up the data structure used to track request progress in
#' the "step log".  As each step of request execution is completed, you should
#' add an event to the step log using [append_sum()] that summarizes the work
#' done.  The accumulated information can be used to describe the process of
#' data manipulation as well as the resource requirements for execution.
#'
#' The step log is a table of events saved during execution to summarize the
#' process. The content includes at least three columns for each event: `site`,
#' which is the name of the current site from `site_info.R`, `stamp`, which
#' contains the ISO-8601 current time, and `used`, which contains a summary of
#' CPU utilization to this point.
#'
#' Additional for a particular request is flexible, and is set by the key-value
#' pairs passed to init_sum().  This typically includes a descriptive name for
#' each step, and may include counts or other aggregate data resulting from that
#' step.  For cohort creation, these values might be used to create an attrition
#' table, while for modeling, these might include key parameter values from
#' successive rounds of fitting.  There is no constraint on the data recorded,
#' but values for the same names must be recorded at each step.
#'
#' @param ... Name-value pairs to be added to the step log
#' @param set_default A Boolean value which determines whether the
#'   result is stored as the default event accumulator.
#'
#' @return The step log, contaning the initial record
#' @seealso [append_sum()], [output_sum()]
#' @md
init_sum <- function(..., set_default = TRUE) {
  x <- tibble(!!! dots_list(...),
              site = config('site'),
              stamp = Sys.time(),
              used = paste(proc.time(), collapse = ';'))
  if (set_default) config('_step_log', x)
  x
}

#' Add rows to a step log
#'
#' This function adds rows to an existing step log, filling in values
#' for the three extra columns created by [init_sum()] in addition to the data
#' passed in the current call.
#'
#' @param ... Additional name-value pairs to add.  These must match the names
#'   used to set up the step log in the call to [init_sum()].
#' @param step_log The step log in which to accumulate event information.
#'   If it is NA (the default), the default event accumulator is
#'   augmented.  Note that if you wish to override the default, you
#'   must pass this parameter by name.
#'
#' @return The step log with the new record added.
#' @seealso [init_sum()], [output_sum()]
#' @md
append_sum <- function(..., step_log = NA) {
  events <- if (is.na(step_log)) config('_step_log') else step_log
  x <- dplyr::union(tibble(!!! dots_list(...),
                           site = config('site'),
                           stamp = Sys.time(),
                           used = paste(proc.time(), collapse = ';')),
                    events)
  if (is.na(step_log)) config('_step_log', x)
  x
}

#' Output step log
#'
#' This function writes out the summary data accumulated by
#' [init_sum()] and [append_sum()].
#'
#' @param step_log The tbl in which event information was
#'   accumulated.  Defaults to the default event accumulator.
#' @param name The name to give the output file or table.
#' @param ... Additional arguments passed to [output_tbl()].
#'
#' @return The return value of [output_tbl()].
#' @seealso [init_sum()], [append_sum()]
#' @md
output_sum <- function(step_log = config('_step_log'),
                       name = paste0(config('site'), '_steps'),
                       ...) {
  output_tbl(step_log, name = name, ...)
}

#' Count unique elements in a resultset.
#'
#' Given a column (e.g. a foreign key), returns as an integer the
#' number of unique elements in that column. This can be used as one
#' of the outputs for [append_sum()] calls in the driver file.
#'
#' @param rset The [dplyr::tbl()] describing the resultset
#' @param id_col The name of the column in which to count elements.
#'
#' @return The number of distinct values
#' @md
distinct_ct <- function(rs, id_col = 'person_id') {
  # A little clunky, but n_distinct() doesn't support quoting of
  # identifiers to the DBMS
  rs %>% select(all_of(id_col)) %>% distinct() %>%
    summarize(dist_ct = n()) %>% pull(dist_ct)
}

#' Create a sequence with which to replace IDs
#'
#' This funcion creates an integer sequence matching distinct values
#' in a column of the input tbl. The resulting crosswalk can be used
#' to replace IDs in datasets when unique identifiers that are not
#' stable across datasets are needed.
#'
#' @param input A tbl containing the column of interest
#' @param id_col The name of the column containing IDs to be mapped.
#' @param rep_col The name of the column to contain the replacement
#'   integers.
#'
#' @return A two-column tbl containing unique values from id_col and
#'   replacement values in rep_col
#' @md
gen_xwalk <- function(input, id_col, rep_col = 'seq_id') {
  idq <- quo_name(id_col)
  req <- quo_name(rep_col)

  input %>% select(!!idq) %>% distinct() %>%
    arrange_at(vars(!!idq)) %>% # Oracle insists on an ordering
    mutate( !!req := as.integer(row_number()))
}

#' Add or replace one ID column with another
#'
#' This function takes a crosswalk between the values in a specific
#' column of a tbl and a new value, and either adds a column with the
#' new values or replaces the old values with the new.  It is intended
#' for use replacing an ID that should be redacted out of the data
#' with a lower-risk ID.
#'
#' @param data The tbl containing the data
#' @param id_col The name of the column containing the ID to join.
#'   Defaults to the name of data suffixed by `_id`
#' @param xwalk The tbl containing crosswalk between id_col and new
#'   values.  Defaults to using a sequence of integers.
#' @param replace  If TRUE, replace the contents of id_col with the
#'   first non-id_col column of xwalk.  If FALSE, leave the original
#'   id_col in place.
#'
#' @return The tbl with augmented or replaced IDs
#' @md
new_id <- function(data,
                   id_col = paste0(deparse(substitute(data)), '_id'),
                   xwalk = select_at(data, vars(id_col)) %>%
                     mutate(seq_id = row_number()),
                   replace = FALSE) {

  xform <- data %>% left_join(xwalk, by = id_col)
  if (replace) {
    newcol <- grep(id_col, tbl_vars(xwalk),
                   invert = TRUE, value = TRUE, fixed = TRUE)[1]
    xform <- xform %>% select(-one_of(id_col)) %>%
      rename_at(vars(newcol), function (x) { id_col })
  }
  xform
}

#' Convert dates to ages
#'
#' For a simple tbl resembling the PEDSnet (or PCORnet, since both
#' use a `_date` suffix to denote dates) data model, create a new tbl in which
#' dates are replaced with ages in days.  Times (columns with names ending in
#' `_time` or `_datetime`) are removed.
#'
#' @param cohort The tbl to scrub
#' @param person_tbl The tbl containing person_id and birth_datetime
#'   to use for age calculations.
#'
#' @return The scrubbed tbl
#' @md
dates_to_ages <- function(cohort,
                          person_tbl = cdm_tbl('person')) {
  cohort_vars <- tbl_vars(cohort)

  if (! any(grepl('_date', cohort_vars))) return(cohort)

  if (any(cohort_vars == 'birth_date')) {
    cohort <- cohort %>% mutate(birth_dt = birth_date)
  }
  else {
    if (! any(cohort_vars == 'birth_datetime')) {
      cohort <- inner_join(cohort,
                           select(person_tbl, person_id, birth_datetime),
                           by = 'person_id')
      }
    cohort <- mutate(cohort,
                     birth_dt = sql('cast("birth_datetime" as date)'))
  }

  cohort <- cohort %>%
    mutate_at(vars(ends_with('_date')), list(age = ~(. - birth_dt))) %>%
    rename_at(vars(ends_with('_date_age')), list(~sub('_date', '', .))) %>%
    select_at(vars(-ends_with('_date'))) %>%
    select_at(vars(-ends_with('_datetime'))) %>%
    select_at(vars(-ends_with('_time'))) %>%
    select(-birth_dt)


  # Fix mutate_at()'s "helpful" removal of source column name when there is
  # only one column changed
  dates <- grep('_date', cohort_vars, value = TRUE)
  if (length(dates) == 1) {
    agenew <- sub('_date', '_age',dates[1])
    cohort <- cohort %>% rename(!! agenew := age)
  }
  cohort
}


#' Replace person IDs and convert dates to ages
#'
#' For a simple table structure resembling the PEDSnet CDM, create
#' variants with nonce person IDs (via [new_id()]) and dates converted to ages
#' in days (via [dates_to_ages()]).
#'
#' @param cohort The tbl to scrub
#' @param person_xwalk The tbl containing person_id mappings for
#'   [new_id()].  If `NA` (the default), will create a nonce crosswalk
#'   based on the `person_id`s in `cohort`.  The crosswalk is not
#'   returned, so you should do this only if you don't ever need to
#'   walk the nonce IDs back to the originals.
#' @param person_tbl The tbl containing person_id and birth_datetime
#'   to use for age calculations.
#'
#' @return The scrubbed tbl
#' @md
scrub_person_info <- function(cohort, person_xwalk = NA,
                              person_tbl = cdm_tbl('person')) {
  if (any(is.na(person_xwalk))) {
    person_xwalk <- cohort %>% distinct(person_id) %>%
      gen_xwalk('person_id')
  }

  cohort %>%
    dates_to_ages(person_tbl) %>%
    new_id(id_col = 'person_id', xwalk = person_xwalk, replace = TRUE)
}


#' Add a site column to a tbl
#'
#' For multisite databases, a join is done to a linking table to add a
#' `site` column to the input.  For single-site databases, the value
#' of `config('site')` is used as the content for the newly-added
#' `site` column.
#'
#' @param f_tbl the tbl that needs to be joined to obtain site
#'   information.  Must contain id_col.
#' @param site_tbl the tbl that contains the id_col and `site`
#'   information.
#' @param id_col The name of the column on which to join.
#'
#' @return The contents of `f_tbl` with a field added
#'   for site.
#' @md
add_site <- function(f_tbl,
                     site_tbl = cdm_tbl('person'),
                     id_col = 'person_id') {
  if (any(tbl_vars(f_tbl) == 'site')) return(f_tbl)

  idq <- enquo(id_col)

  if (any(tbl_vars(site_tbl) == 'site')) {
    f_tbl <- f_tbl %>%
      left_join(select(site_tbl, !!idq, site), by = id_col,
                copy = !same_src(site_tbl, f_tbl))
  }
  else {
    val <- config('site')
    if ( is.null(val) ) val <- 'unknown'
    f_tbl <- f_tbl %>% mutate('site' = val)
  }
  f_tbl
}

#' Find facts occurring within a specified time before an index date
#'
#' This function filters a tbl for events starting within a certain
#' number of days prior to an index date, supplied in a second tbl.
#'
#' @param data The data table.
#' @param index_tbl A tbl containing (at least) `person_id` and an
#'   index date column against which data will be filtered.
#' @param event_date_col The name of the column containing the event
#'   date of interest.  Defaults to the name of the data tbl, less any
#'   suffix `_occurrence`, with `_start_date` added.
#' @param index_date_col The name of the column in index_tbl
#'   containing the index date.  Defaults to `cohort_start_date` for
#'   compatibility with Atlas output.
#' @param link_col The name of the column on which to join data and index_tbl.
#' @param lookback The lookback interval in days. Defaults to 365.
#'
#' @return A tbl containing filtered rows of interest (i.e. that match
#'   on `person_id` and fall within the lookback interval)
#' @md
lookback_facts <- function(data,
                           index_tbl,
                           event_date_col = NA,
                           index_date_col = 'cohort_start_date',
                           link_col = 'person_id',
                           lookback = 365L) {

  idq <- enquo(index_date_col)

  if (is.na(event_date_col)) {
    dname <- NA
    if (any(class(data) == 'tbl_dbi')) {
      dname  <- dbplyr:::tbl_desc(data)
      dname <- unlist(regmatches(dname,
                                 regexec('(\\w+)"?>', dname, perl = TRUE)))[2]
    }
    if (is.na(dname)) dname <- deparse(substitute(data))
    event_date_col <-
      paste0(sub('_occurrence$', '', dname, ignore.case = TRUE),
             '_start_date')
  }
  data %>%
    inner_join(select(index_tbl, person_id, !!idq),
               by = link_col,
               copy = ! same_src(data, index_tbl)) %>%
    filter(between(dbplyr::ident(index_date_col) -
                     dbplyr::ident(event_date_col),
                   0L, lookback)) %>%
    select(-!!idq)
}
