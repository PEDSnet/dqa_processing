# Add a schema, if defined, and do table name substitution based on config
.qual_name <- function(name, schema_tag, db = config('db_src')) {
  if (inherits(name, c('ident_q', 'dbplyr_schema'))) return(name)
  name_map <- config('table_names')
  name <- base::ifelse(hasName(name_map, name), name_map[[name]], name)
  if (! is.na(schema_tag)) {
    if (config_exists(schema_tag)) schema_tag <- config(schema_tag)
    if (! is.na(schema_tag)) {
      if (packageVersion('dbplyr') < '2.0.0') {
        name <- DBI::dbQuoteIdentifier(dbi_con(db), name)
      }
      name <- dbplyr::in_schema(schema_tag,name)
    }
  }
  name
}

.qual_tbl <-
  function(name, schema_tag, db = config('db_src'))
    tbl(db, .qual_name(name, schema_tag, db))

#' Get DBI-compatible connection handle across dplyr generations
#'
#' Given a database connection the might have been created by [DBI::dbConnect()]
#'   or my one of the `src_foo()` functions in versions of dbplyr, return a
#'   DBI-compatible connection.
#'
#' Note that this function is essentially identical to [dbplyr::remote_con()]
#'   for dbplyr connections.
#'
#' @param db A DBI or dbplyr connection.
#'
#' @return A DBI-compatible connection, or NULL if `db` is not a connection.
#' @md
dbi_con <- function(db) {
  # R introspection is very limited, so best approach is to keep trying.
  if (! any(is.object(db))) return(NULL)
  rslt <- NULL
  # DBI Connection
  if (! is.null(tryCatch( DBI::dbDataType(db, 1L),
                          error = function (e) NULL))) return(db)
  # Modern dbplyr
  if (any(class(db) == 'tbl_sql') &&
      ! is.null(tryCatch( { rslt <- remote_con(db) },
                          error = function (e) NULL))) return(rslt)
  # Older dbplyr query - breaks encapsulation
  if (exists('src', where = db) &&
      ! is.null(tryCatch( { rslt <- db$src$con },
                          error = function (e) NULL))) return(rslt)
  # Older dbplyr connection - breaks encapsulation
  if (exists('con', where = db) &&
      ! is.null(tryCatch( { rslt <- db$con },
                          error = function (e) NULL))) return(rslt)
  rslt
}

#' Connect to an existing CDM data table
#'
#' @param name The name of the table
#' @param db The database connection; you will rarely need to specify this.
#'
#' @return A [dplyr::tbl()]] pointing to the table
#' @md
cdm_tbl <-
  function(name, db = config('db_src'))
    .qual_tbl(name, 'cdm_schema', db)

#' Connect to an existing CDM vocabulary table
#'
#' @param name The name of the table
#' @param db The database connection; you will rarely need to specify this.
#'
#' @return A [dplyr::tbl()]] pointing to the table
#' @md
vocabulary_tbl <-
  function(name, db = config('db_src'))
    .qual_tbl(name, 'vocabulary_schema', db)


# Add a request-specific tag to the name, if so configured
.add_name_tag <- function(name, tag, db = config('db_src')) {
  if ( ! is.na(tag)) {
    add_tag <- function(x) {
      no_tag <- ! grepl(paste0(tag,'$'), x)
      x[no_tag] <- paste0(x[no_tag], tag)
      x
    }
    if (inherits(name, c('ident_q', 'dbplyr_schema'))) {
      # Best guess - works but relies on stringification of dbplyr::in_schema()
      if (packageVersion('dbplyr') < '2.0.0') {
        name <- gsub('["`\']', '', dbplyr::as.sql(name))
      }
      else {
        name <- gsub('["`\']', '', dbplyr::as.sql(name, dbi_con(db)))
      }
      parts <- regmatches(name, regexec('^"*(.+?)"*\\."*(.+?)"*$', name))
      name <- dbplyr::in_schema(parts[[1]][2], add_tag(parts[[1]][3]))
    }
    else {
      name <- add_tag(name)
    }
  }
  name
}

#' Connect to a result table
#'
#' This function sets up the connection to a database table that was presumably
#' created by during execution of this or a prior data request, and is found
#' in the schema designated for results, whether the table itself is intended to
#' be permanent or just an intermediate for this run.
#'
#' @param name The name of the table
#' @param db The database connection; you will rarely need to specify this.
#' @param results_tag The request tag to add to the table name (see [intermed_name()]).
#' @param local_tag The local tag to add to the table name (see [intermed_name()]).
#'
#' @return A [dplyr::tbl()]] pointing to the table
#' @seealso [intermed_name()], for more information on how the table
#'   specification is determined.
#' @md
results_tbl <- function(name, db = config('db_src'),
                        results_tag =  TRUE, local_tag = FALSE) {
  .qual_tbl(intermed_name(name, temporary = FALSE,
                          results_tag = results_tag,
                          local_tag = local_tag),
            'results_schema', db)
}

results_tbl_other <- function(name, db = config('db_src_prev'),
                              results_tag =  FALSE, local_tag = TRUE) {
  .qual_tbl(intermed_name(name, temporary = FALSE,
                          results_tag = results_tag,
                          schema = 'results_schema_other',
                          local_tag = local_tag),
            'results_schema_other', db)
}

#' Construct a (possibly schema-qualified) intermediate result table name
#'
#' If the name passed in is already the result of a call to
#' [dbplyr::in_schema()], then no further modification is made.  Otherwise,
#' schema qualification is done based on the value of `temporary`, and name
#' modification based on the values of `local` and `no_tag`.
#'
#' By default, a request-specific string called the results tag, whose value is
#' set in configuration, is added to the table name.  This is done to prevent
#' tables from this request colliding with those from other requests, if you use
#' a common scnema across data requests.  If you know this is not appropriate
#' (for example, you're passing the name of a table generated by a previous
#' request, and including it's tag in `name`), then you can disable the
#' addition on a one-time basis by setting `results_tag` to FALSE..
#'
#' You also have the option of designating a table name as `local` or not; for
#' the sense of this designation, see [output_tbl()].  A local designation
#' results in the string `_loc` (or whatever else is configured as the local
#' name tag) being added to the table name.
#'
#' This function can be used to get the complete specification for an existing
#' table whose name is known, or called without a `name` value to generate a new
#' table name that is unlikely to collide with an existing table.
#'
#' @param name The name of the table.
#' @param temporary Logical indicating whether the table is temporary.  For some
#'   database systems, this means it must not be schema-qualified.
#' @param results_tag The request tag to add to the table name.  If NA or a
#'   logically false value, no tag is added.  If a character string, that is
#'   used as the tag.  Otherwise, the `config('results_name_tag')` string is
#'   used.
#' @param local_tag The local tag to add to the table name.   If NA or a
#'   logically false value, no tag is added.  If a character string, that is
#'   used as the tag.  Otherwise, the `config('local_name_tag')` string is
#'   used.
#' @param schema A label for the name of the schema to be included in the
#'   result, if `temporary` is false.  If a config element of the same name
#'   exists, its value is used; otherwise, the `schema` parameter is used as
#'   the schema name itself.
#' @param db A database connection, used in more recent versions of dbplyr to
#'   insure quoting of table names matches what the database expects
#'
#' @return The resultant name
#' @md
intermed_name <- function(name = paste0(sample(letters, 12, replace = TRUE),
                                        collapse = ""),
                          temporary = ! config('retain_intermediates'),
                          results_tag =  TRUE,
                          local_tag = NA,
                          schema = 'results_schema',
                          db = config('db_src')) {
  if (! is.na(local_tag) && (is.character(local_tag) || local_tag)) {
    if (! is.character(local_tag)) local_tag <- config('local_name_tag')
    name <- .add_name_tag(name, tag = local_tag, db = db)
  }
  if (! is.na(results_tag) && (is.character(results_tag) || results_tag)) {
    if (! is.character(results_tag)) results_tag <- config('results_name_tag')
    name <- .add_name_tag(name, tag = results_tag, db = db)
  }
  .qual_name(name = name,
             schema_tag = base::ifelse(temporary, NA, schema),
             db = db)
}

#' Construct a fully qualified results table name
#'
#' This convenience function behaves similarly to [intermed_name()], except that
#' the table name must always be provided, and the result is always treated as a
#' non-temporary table.
#'
#' @param name The name of the table.
#' @param results_tag The request tag to add to the table name.  If NA or a
#'   logically false value, no tag is added.  If a character string, that is
#'   used as the tag.  Otherwise, the `config('results_name_tag')` string is
#'   used.
#' @param local_tag The local tag to add to the table name.   If NA or a
#'   logically false value, no tag is added.  If a character string, that is
#'   used as the tag.  Otherwise, the `config('local_name_tag')` string is
#'   used.
#' @param db A database connection, used in more recent versions of dbplyr to
#'   insure quoting of table names matches what the database expects
#'
#' @return The resultant name
#' @md
results_name <- function(name, results_tag =  TRUE, local_tag = NA,
                         db = config('db_src')) {
  intermed_name(name = name, temporary = FALSE, results_tag = results_tag,
                local_tag = local_tag, db = db)
}

# Try to figure out schema/table expressions in various forms
.parse_tblspec <- function(spec) {
  if (inherits(spec, 'Id')) {
    elts <- spec
  }
  else if (inherits(spec, c('dbplyr_schema'))) {
    elts <- unlist(spec)
  }
  else if (inherits(spec, c('ident_q'))) {
    elts <- format(spec)
  }
  else if ((grepl('.', spec, fixed = TRUE) && ! grepl('["`]', spec))) {
    # If we have an unquoted string with a '.', assume schema.table
    elts <- unlist(strsplit(as.character(spec), '.', fixed = TRUE))
  }
  else if (grepl('["`].+["`]\\.["`]', spec)) {
    # And one simple-minded try at a quoted identifier string
    elts <-
      gsub('["`]', '',
           unlist(strsplit(as.character(spec), '["`].["`]')))
  }
  else {
    elts <- spec
  }
  names(elts) <- rev(c('table', 'schema', 'catalog')[1:length(elts)])
  elts
  # nocov end
}

#' Drop a table in a database, schema-aware
#'
#' This function has the same purpose as [DBI::dbRemoveTable()], but many
#'  implementations are unable to accommodate a table name created by
#'  [dbplyr::in_schema()].
#'
#' Note that `db` is the first parameter, even though the default is almost
#' always what you wnat, in order to maintain a calling sequence consistent with
#' what you'd expect from the DBI specification.  So you'll probably want to
#' call this function as `db_remove_table( name =` _tabname_ `)` most of the time.
#'
#' @param db A [dplyr::src()] or DBI connection
#' @param name The name of the table, or an object created by
#'   [dbplyr::in_schema()], or a DBI-style path vector
#' @param tenporary Whether `table` is a temporary table
#' @param fail_if_missing If TRUE, raise an error if `table` does not exist
#'
#' @return TRUE, invisibly, if no error is encountered; an exception is raised
#'   otherwise
#' @md
db_remove_table <- function(db = config('db_src'), name,
                            temporary = FALSE, fail_if_missing = FALSE) {
  con <- dbi_con(db)
  elts <- .parse_tblspec(name)
  
  sql <- paste0('drop table ',
                base::ifelse(fail_if_missing, '', 'if exists '))
  if (any(grepl('ora', class(con), ignore.case = TRUE))) {
    if (! db_exists_table(con, name)) return(TRUE)
    if (length(elts) > 1) {
      elts <- rev(elts)
      DBI::dbRemoveTable(con, elts[1], schema = elts[2])
    } else {
      DBI::dbRemoveTable(con, elts[1])
    }
  }
  else if (!temporary &&
           any(class(con) %in% c('PostgreSQLConnection', 'PqConnection')) &&
           length(elts) > 1) {
    name <- DBI::SQL(paste0(DBI::dbQuoteIdentifier(con, elts[1]),
                            '.',
                            DBI::dbQuoteIdentifier(con, elts[2])))
    DBI::dbExecute(con, paste0(sql, name))
  }
  else {
    name <- paste0(vapply(elts, function (x) DBI::dbQuoteIdentifier(con, x),
                          FUN.VALUE = character(1)), collapse = '.')
    DBI::dbExecute(con, paste0(sql, name))
  }
  invisible(TRUE)
}


#' Test whether a table exists in a database, schema-aware
#'
#' This function has the same purpose as [DBI::dbExistsTable()] or
#' [dbplyr::db_has_table()], but neither of those are able to test a
#' table name created by [dbplyr::in_schema()].
#'
#' Note that `db` is the first parameter, even though the default is almost
#' always what you wnat, in order to maintain a calling sequence consistent with
#' what you'd expect from the DBI specification.  So you'll probably want to
#' call this function as `db_exists_table( name =` _tabname_ `)` most of the time.
#'
#' @param db A [dplyr::src()] or DBI connection
#' @param name The name of the table, or an object created by
#'   [dbplyr::in_schema()], or a DBI-style path vector
#'
#' @return TRUE if the table exists, and FALSE otherwise.
#' @md
db_exists_table <- function(db = config('db_src'), name) {
  con <- dbi_con(db)
  elts <- .parse_tblspec(name)
  
  if (any(grepl('ora', class(con), ignore.case = TRUE)) &&
      length(elts) > 1) {
    elts <- rev(elts)
    return(DBI::dbExistsTable(con, elts[1], schema = elts[2]))
  }
  else if (any(class(con) == 'PostgreSQLConnection') &&
           length(elts) == 1) {
    res <-
      DBI::dbGetQuery(con,
                      paste("select tablename from pg_tables where ",
                            "schemaname !='information_schema' and schemaname !='pg_catalog' ",
                            "and schemaname in (select schemas[nr] from ",
                            "(select *, generate_subscripts(schemas,1) as nr ",
                            "from (select current_schemas(true) as schemas) a ",
                            ") b where schemas[nr] <> 'pg_catalog') and tablename=",
                            DBI::dbQuoteString(con, elts[1]), sep = ""))
    return(as.logical(dim(res)[1]))
  }
  else if (length(elts) > 1) {
    return(DBI::dbExistsTable(con, DBI::Id(elts)))
  }
  else {
    return(DBI::dbExistsTable(con, elts))
  }
}


#' Like compute(), but insure the target doesn't exist and allow for trace output
#'
#' This function creates a database table containing the results of a dplyr
#' expression, whether or not a table by that name already exists.  If tracing
#' of database activity is turned on, it generates logging as well.
#'
#' One of the limitations of [dplyr::compute()] as applied to database tables is
#' that it does not support the `overwrite` parameter, so calls are not
#' idempotent: if the target table already exists, an error occurs.  This
#' function simply drops the target table if it exists.  Note that there is no
#' `overwrite` vs `append` option here; the target is always created anew.
#'
#' While the underlying mechanism is the same, [compute_new()] is generally
#' intended for creation of intermediate tables during computation, and
#' [output_tbl()] for generation of final output.  The latter is more aware of
#' different options for saving results, and will direct data appropriately.
#'
#' Similarly, whether a table is temporary or permanent is meant to be
#' controlled by the user's configuration settings.  It is possible to override
#' this by using the `temporary` parameter, but this should be done only in
#' cases where there is a pressing reason to insist on particular behavior
#' (e.g. computation of an intermediate linking table that is prohibitively
#' large).
#'
#' @param tblx The [dplyr::tbl()] expression to compute
#' @param name The name to give the table
#' @param temporary A Boolean indicator of whether the table should be
#'   temporary.
#' @param ... Other arguments passed to [dplyr::compute()]
#'
#' @return A [dplyr::tbl()] resulting from [dplyr::compute()].
#' @seealso [output_tbl()] for permanent output, [collect_new()] and
#'   [copy_to_new()] for database interactions with similar tracing
#' @md
compute_new <- function(tblx,
                        name = paste0(sample(letters, 12, replace = TRUE),
                                      collapse = ""),
                        temporary = ! config('retain_intermediates'),
                        ...) {
  if (!inherits(name, c('ident_q', 'dbplyr_schema')) && length(name) == 1) {
    name <- gsub('\\s+','_', name, perl = TRUE)
    name <- intermed_name(name, temporary)
  }
  con <- dbi_con(tblx)
  if (db_exists_table(con, name)) db_remove_table(con, name)
  if (config('db_trace')) {
    show_query(tblx)
    if (config('can_explain')) explain(tblx)
    message(' -> ',
            base::ifelse(packageVersion('dbplyr') < '2.0.0',
                         dbplyr::as.sql(name),
                         dbplyr::as.sql(name, con)))
    start <- Sys.time()
    message(start)
  }
  rslt <- dplyr::compute(tblx, name = name, temporary = temporary, ...)
  if (config('db_trace')) {
    end  <- Sys.time()
    message(end, ' ==> ', format(end - start))
  }
  rslt
}


#' Like collect(), but allow for trace output
#'
#' This function is a simple wrapper for [dplyr::collect()], but allows for
#' logging when tracing of database activity is turned on.
#'
#' @param tblx The [dplyr::tbl()] expression to collect
#' @param ... Other arguments passed to [dplyr::collect()]
#'
#' @return A tbl resulting from [dplyr::collect()].
#' @seealso [compute_new()] and [copy_to_new()] for database interactions
#'   with similar tracing
#' @md
collect_new <- function(tblx, ...) {
  if (config('db_trace')) {
    if (inherits(tblx, 'tbl_sql')) {
      show_query(tblx)
      if (config('can_explain')) explain(tblx)
    }
    message(' -> collect')
    start <- Sys.time()
    message(start)
  }
  rslt <- collect(tblx)
  if (config('db_trace')) {
    end  <- Sys.time()
    message(end, ' ==> ', format(end - start))
  }
  rslt
}

#' Like copy_to(), but allow for trace output
#'
#' This function is a simple wrapper around copy_to(), that provides
#' some trace output, allowing that there is less query construction
#' that occurs than in functions starting work in the database.  It
#' also works around a bug in dplyr that renders `overwrite`
#' non-functional in some circumstances.
#'
#' @param dest A remote data source.
#' @param df The data frame/tbl to be copied to the database
#' @param name The name to give the resulting table.  Defaults to the
#'   name of the data frame as qualified by intermed_name().
#' @param overwrite Whether to remove an existing table of the same
#'   name.  Defaults to TRUE, which is different from the dplyr
#'   function of the same name.
#' @param temporary Whether the created table should be temporary.
#'   Defaults to the opposite of `config('retain_intermediates')`.
#' @param ... Other arguments passed to dplyr::copy_to()
#'
#' @return A tbl pointing to the remote table
#' @seealso [compute_new()] and [collect_new()] for database interactions
#'   with similar tracing
#' @md
copy_to_new <- function(dest = config('db_src'), df,
                        name = deparse(substitute(df)),
                        overwrite = TRUE,
                        temporary = ! config('retain_intermediates'),
                        ...) {
  name <- intermed_name(name, temporary = temporary)
  if (config('db_trace')) {
    message(' -> copy_to')
    start <- Sys.time()
    message(start)
    message('Data: ', deparse(substitute(df)))
    message('Table name: ',
            base::ifelse(packageVersion('dbplyr') < '2.0.0',
                         dbplyr::as.sql(name),
                         dbplyr::as.sql(name, dbi_con(dest))),
            ' (temp: ', temporary, ')')
    message('Data elements: ', paste(tbl_vars(df), collapse = ','))
    message('Rows: ', NROW(df))
  }
  if (overwrite &&
      db_exists_table(dest, name)) {
    db_remove_table(dest, name)
  }
  rslt <- dplyr::copy_to(dest = dest, df = df, name = name,
                         overwrite = overwrite, temporary = temporary,
                         ...)
  if (config('db_trace')) {
    end  <- Sys.time()
    message(end, ' ==> ', format(end - start))
  }
  rslt
}


# Write data to a CSV file in a the appropriate results dir
.output_csv <- function(data, name = NA, local = FALSE) {
  if (is.na(name)) name <- quo_name(enquo(data))
  dirs <- config('subdirs')
  collect_new(data) %>%
    write_csv(file.path(config('base_dir'),
                        base::ifelse(local, dirs$local_dir, dirs$result_dir),
                        paste0(name, '.csv')), na = '')
}

#' Output contents of a tbl
#'
#' Write the contents of a tbl to either a CSV file in the appropriate
#' results directory, or to the database, for future reference.
#'
#' This function is the switch point for output from a data request that is
#' meant to be permanent, such as final results or a cohort list that will be
#' reused in future data requests. It classifies output in one of two ways,
#' depending on the value of `local`.  If true, the output is intended to be
#' reviewed or retained by whoever executed the request, but not transmitted
#' back to the requestor.  If false, the output is intended for the requestor.
#' How this translates into practice depends on the destination.  If the output
#' is directed to a file, then the directory is chosen based on this flag, but
#' the file's name is constant.  If the output is directed to a database table,
#' then the local flag has the effect described in [intermed_name()].
#'
#' The decision about where to direct the output should generally be made based
#' on the execution mode of the request.  During development, the database is
#' the preferred target, since it is convenient for further analysis and
#' debugging.  When the request is distributed to a remote site for execution,
#' however, the workflow is typically to return the results to the requestor, so
#' they are typically directed to a file.  For local production runs, results
#' are saved in both locations: the databasw for collaboration and archiving
#' (possibly after merging with results from remote sites), and local files for
#' inspection and report writing.  As with other decisions about output, you
#' should generally not override these configuration-determined results unless
#' there is a pressing reason (e.g. you need to save output to a database table
#' because you know it will be used in a future request).
#'
#' @param data The tbl to write.
#' @param name The name (only) of the output  Defaults to the name of `data`.
#' @param local A value indicating whether the data should be written to a
#'   local-only location, which is not typically returned with query results.
#' @param file An indicator of whether the data should be written out as a CSV
#'   file.  If it is FALSE, no file will be written. If it is TRUE, a CSV file
#'   will be written named _name_, with a `.csv` suffix appended.
#' @param db A database connection, to which to write the results table.  If it
#'   is FALSE or NA, no database table is written.  If it is TRUE, then
#'   config('db_src') is used.
#' @param results_tag A value indicating whether to add a request tag to the
#'   output name (see [intermed_name()]) iff the results are written to the
#'   database.
#' @param ... Additional arguments passed to the database table creation
#'   function.
#'
#' @return The result of the operation, typically a copy of data,
#'   possibly collect()ed.
#' @seealso [compute_new()] and [copy_to_new()] for creating
#'   intermediate tables for internal use during request execution
#' @md
output_tbl <- function(data, name = NA, local = FALSE,
                       file = base::ifelse(config('results_target') == 'file',
                                           TRUE, FALSE),
                       db = if (! file) config('results_target') else NA,
                       results_tag = TRUE, ...) {
  if (is.na(name)) name <- quo_name(enquo(data))
  
  if (file) {
    rslt <- .output_csv(data, name, local)
  }
  
  # Conditional logic is a little convoluted here to allow for legacy behavior
  # of allowing Boolean value for db
  if ( is.object(db) || (!is.na(db) && db)) {
    if (is.logical(db)) db <- config('db_src')
    rname <- intermed_name(name, temporary = FALSE, db = db,
                           results_tag = results_tag, local_tag = local)
    if (any(class(data)  == 'tbl_sql') &&
        identical(dbi_con(data), dbi_con(db))) {
      rslt <- compute_new(data, rname, temporary = FALSE, ...)
    }
    else {
      rslt <- copy_to_new(db, collect_new(data), rname,
                          temporary = FALSE, overwrite = TRUE, ...)
    }
  }
  
  invisible(rslt)
}
