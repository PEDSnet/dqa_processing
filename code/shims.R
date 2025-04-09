#!/usr/bin/env Rscript

# Hacks necessary for database support with current libraries.
# No user-serviceable parts inside.

# OK, Oracle temporary tables aren't temporary; their contents are.
# So we need to clean them out when we're done.
.env_cleanup <- function(db = config('db_src')) {

  if ( ! is.na(Sys.getenv('PEDSNET_SKIP_REQUEST_EXECUTION', unset = NA)) &&
       ! is.null(config('execution_mode')) &&
       config('execution_mode') == 'distribution' ) {
    sink()
    sink(type = 'message')
  }

  if (any(class(db) == 'OraConnection')) .ora_env_cleanup(db)
  else if (any(class(db) == 'PqConnection')) .pq_env_cleanup(db)
  else if (any(class(db) == 'PostgreSQLConnection')) .pgsql_env_cleanup(db)

  invisible(TRUE)
}


.env_setup <- function(db = config('db_src')) {

  if ( is.na(Sys.getenv('PEDSNET_SKIP_REQUEST_EXECUTION', unset = NA)) &&
       ! is.null(config('execution_mode')) &&
       config('execution_mode') == 'distribution' ) {
    logf <- file(file.path(base_dir,
                           config('subdirs')$result_dir,
                           paste0(config('qry_site'), '.log')), open = 'wt')
    sink(logf, type = 'output')
    sink(logf, type = 'message')
  }

  config('can_explain',
         !is.na(tryCatch(db_explain(config('db_src'), 'select 1 = 1'),
                         error = function(e) NA)))

  if (packageVersion('dbplyr') < '1.3.1') {
    assignInNamespace('arrange.tbl_lazy',
                      function(.data, ..., .by_group = FALSE) {
                        dots <- quos(...)
                        dots <-
                          dbplyr:::partial_eval(dots,
                                                vars = dbplyr:::op_vars(.data))
                        names(dots) <- NULL

                        dbplyr:::add_op_single(
                          "arrange",
                          .data,
                          dots = dots,
                          args = list(.by_group = .by_group)
                        )
                      }, ns = 'dbplyr')
  } else if (packageVersion('dbplyr')[1, 1:2] == '2.2') {
    # Fix bug in 2.2.x that calls unname() indisciminately and wipes out schema
    assignInNamespace('compute.tbl_sql',
                        function(x, name = dbplyr:::unique_table_name(),
                                 temporary = TRUE, unique_indexes = list(),
                                 indexes = list(), analyze = TRUE, ...,
                                 cte = FALSE) {
                          if (!any(class(name) %in%
                                   c('dbplyr_schema', 'dbplyr_catalog')))
                              name <- unname(name)
                          vars <- op_vars(x)
                          assertthat::assert_that(all(unlist(indexes) %in% vars))
                          assertthat::assert_that(all(unlist(unique_indexes) %in% vars))

                          x_aliased <- select(x, !!! syms(vars)) # avoids problems with SQLite quoting (#1754)
                          sql <- db_sql_render(x$src$con, x_aliased$lazy_query, cte = cte)

                          name <- db_compute(x$src$con, name, sql,
                                             temporary = temporary,
                                             unique_indexes = unique_indexes,
                                             indexes = indexes,
                                             analyze = analyze, ...)

                          dbplyr:::tbl_src_dbi(x$src,
                                               as.sql(name, con = x$src$con),
                                               colnames(x)) %>%
                            group_by(!!!syms(op_grps(x))) %>%
                            window_order(!!!op_sort(x))
                        }, ns = 'dbplyr')
  }

  if (any(class(db) == 'OraConnection')) .ora_env_setup(db)
  else if (any(class(db) == 'PqConnection')) .pq_env_setup(db)
  else if (any(class(db) == 'PostgreSQLConnection')) .pgsql_env_setup(db)

  invisible(TRUE)
}

.pq_env_setup <- function(db = config('db_src')) {
  if (packageVersion('dbplyr') < '2.0.0') {
    assignInNamespace('db_has_table.DBIConnection',
                      function(con, table, ...) db_exists_table(con, table),
                      ns = 'dbplyr')
  }
}
.pq_env_cleanup <- function(db = config('db_src')) invisible(TRUE)

.pgsql_env_setup <- function(db = config('db_src')) {
  if (packageVersion('dbplyr') < '2.0.0') {
    assignInNamespace('db_has_table.PostgreSQLConnection',
                      function(con, table, ...) db_exists_table(con, table),
                      ns = 'dbplyr')
  }
}
.pgsql_env_cleanup <- function(db = config('db_src')) invisible(TRUE)

.ora_env_setup <- function(db = config('db_src')) {

  # List of Oracle "temp" tables that need to be dropped
  config('ora_drop_me', character(0))

  DBI::dbExecute(db, "alter session set nls_date_format = 'YYYY-MM-DD'")
  DBI::dbExecute(db, "alter session set nls_timestamp_tz_format = 'YYYY-MM-DD HH24:MI:SS TZHTZM'")

  if (packageVersion('dbplyr') < '2.0.0') {
    assignInNamespace('db_save_query.DBIConnection',
                      function (con, sql, name, temporary = TRUE, ...) {
                        if (temporary) {
                          if (any(class(con) == 'OraConnection')) {
                            temptbl <- dbplyr::sql(' ')
                            config('ora_drop_me',
                                   append(config('ora_drop_me'), name))
                          }
                          else {
                            temptbl <- dbplyr::sql('TEMPORARY ')
                          }
                        }
                        tt_sql <- dbplyr::build_sql("CREATE ",
                                                    if (temporary) temptbl,
                                                    'TABLE ',
                                                    dbplyr::as.sql(name, con = con),
                                                    ' AS ', sql,
                                                    con = con)
                        DBI::dbExecute(con, tt_sql)
                        name
                      }, ns = 'dbplyr')

    assignInNamespace('db_create_index.DBIConnection',
                      function(con, tabname, columns, name = NULL,
                               unique = FALSE, ...) {
                        assertthat::assert_that(is_string(tabname), is.character(columns))

                        name <- name %||%
                          gsub('[^A-Za-z0-9_]+', '',
                               paste0(c(tabname, columns), collapse = "_"))
                        now <- as.integer(Sys.time())
                        if (any(class(con) == 'OraConnection') &
                              nchar(name) > 30) {
                          # Generate a likely unique, if not very readable, name
                          name <-
                            substr(sprintf('ix%d%d_%s%s',
                                           Sys.getpid(), now,
                                           paste0(substr(columns,1,1), collapse = ''),
                                           config('results_name_tag')),
                                   1, 30)
                        }
                        fields <- dbplyr::escape(ident(columns), parens = TRUE, con = con)
                        sql <- dbplyr::build_sql(
                          "CREATE ", if (unique) sql("UNIQUE "), "INDEX ",
                          dbplyr::as.sql(name),
                          " ON ", dbplyr::as.sql(tabname), " ", fields,
                          con = con)

                        rslt <- dbExecute(con, sql)
                        if (any(class(con) == 'OraConnection') &&
                              as.integer(Sys.time()) == now) Sys.sleep(1)
                        rslt
                      }, ns = 'dbplyr')
  } else {
    assignInNamespace('sql_query_save.DBIConnection',
                      function (con, sql, name, temporary = TRUE, ...) {
                        if (temporary) {
                          if (any(class(con) == 'OraConnection')) {
                            temptbl <- dbplyr::sql(' ')
                            config('ora_drop_me',
                                   append(config('ora_drop_me'), name))
                          }
                          else {
                            temptbl <- dbplyr::sql('TEMPORARY ')
                          }
                        }
                        build_sql("CREATE ", if (temporary)
                          temptbl, "TABLE \n", as.sql(name, con), " AS ",
                          sql, con = con)
                      }, ns = 'dbplyr')

    assignInNamespace('sql_table_index.DBIConnection',
                      function (con, table, columns, name = NULL,
                                unique = FALSE, ...)
                      {
                        assertthat::assert_that(is_string(table) | dbplyr:::is.schema(table), is.character(columns))
                        name <- name %||%
                          gsub('[^A-Za-z0-9_]+', '',
                               paste0(c(unclass(table), columns), collapse =
                                                                    "_"))
                        if (any(class(con) == 'OraConnection') &
                              nchar(name) > 30) {
                          # Generate a likely unique, if not very readable, name
                          name <- substr(sprintf('ix%d%d_%s%s',
                                                 Sys.getpid(), as.integer(Sys.time()),
                                           paste0(substr(columns,1,1), collapse = ''),
                                           config('results_name_tag')),
                                   1, 30)
                        }
                        fields <- dbplyr::escape(ident(columns), parens = TRUE, con = con)
                        dbplyr::build_sql("CREATE ", if (unique)
                          dbplyr::sql("UNIQUE "), "INDEX ", dbplyr::as.sql(name, con = con), " ON ",
                          dbplyr::as.sql(table, con = con), " ", fields, con = con)
                      }, ns = 'dbplyr')
  }

  assignInNamespace('db_write_table.DBIConnection',
                    function(con, tabname, types, values, temporary = TRUE,
                             overwrite = FALSE, ...) {
                      if (any(class(con) == 'OraConnection')) {
                        if (temporary) config('ora_drop_me',
                                              append(config('ora_drop_me'), tabname))
                        schema <- NULL
                        if (inherits(tabname, c('ident_q', 'dbplyr_schema')) &
                              any(grepl('.', tabname, fixed = TRUE))) {
                          parts <- gsub('"', '', dbplyr::as.sql(tabname), fixed = TRUE)
                          parts <- regmatches(parts,
                                              regexec('(.+)\\.(.+)', parts,
                                                      perl = TRUE))[[1]]
                          tabname <- parts[3]
                          schema <- parts[2]
                        }
                        DBI::dbWriteTable(con, name = tabname, schema = schema,
                                          value = values, field.types = types,
                                          temporary = temporary,
                                          overwrite = FALSE, row.names = FALSE, ...)
                      }
                      else
                        DBI::dbWriteTable(con,
                                          name = dbplyr::dbi_quote(dbplyr::as.sql(tabname), con),
                                          value = values, field.types = types,
                                          temporary = temporary,
                                          overwrite = FALSE, row.names = FALSE, ...)
                      tabname
                    }, ns = 'dbplyr')

  if (packageVersion('dbplyr') < '2.0.0') {
    assignInNamespace('in_schema',
                      function(schema, table) {
                        ei <- function(x)
                          if (inherits(x, c('ident_q', 'SQL')))
                            x
                        else
                          dbplyr::escape(dbplyr::ident(x),
                                         con = config('db_src'))
                        dbplyr::ident_q(paste0(ei(schema), '.', ei(table)))
                      }, ns = 'dbplyr')
  } else {
    assignInNamespace('with_transaction',
                      function(con, in_transaction, code) { code },
                      ns = 'dbplyr')
  }

  invisible(TRUE)
}
.ora_env_cleanup <- function(db = config('db_src')) {
  dm <- config('ora_drop_me')
  if (length(dm) == 0) return(invisible(TRUE))

  vapply(unique(dm),
         function (t)
           tryCatch(db_remove_table(db, t, temporary = TRUE),
                    error = function(e) { message(t, ' - ', e); 0L }),
         FUN.VALUE = 0L)

}
