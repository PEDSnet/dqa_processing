#!/usr/bin/env Rscript

#' Read a codeset from a CSV file
#'
#' This function uses the codeset name to build a path to a CSV file
#' in the data request package containing the codeset data.
#'
#' The default expects the file to be in the spec_dir directory, to have a
#' header, and to contain four columns:
#'
#'  * concept_id - integer
#'  * concept_name - string
#'  * concepd_code - string
#'  * vocabulary_id - string
#'
#' @param name The name of the codeset.  Typically just the file name
#'   without `.csv` suffix, but if for some reason you've expanded the
#'   directory tree under `specs`, you may prefix it with
#'   subdirectories.
#' @param col_types A column specification compatible with
#'   [readr::read_csv()].
#' @param full_path A Boolean value indicating whether `name` represents a full
#'   path or a relative file name that should be expanded.
#'
#' @return A tbl contanining the codeset
#' @md
read_codeset <- function(name,
                         col_types = 'iccc',
                         full_path = FALSE) {
  path <-
    if_else(full_path, name,
            file.path(config('base_dir'), config('subdirs')$spec_dir,
                      paste0(name, '.csv')))
    read_csv(path, col_names = TRUE, col_types = col_types)
}

# Invalidate cache when run starts
config_rm('_codesets')

#' Create a db table with a codeset as per the query
#'
#' Reads the named codeset from a CSV file, and creates a like-named
#' intermediate table in the database with the contents.
#'
#' You will typically want to construct an index on the principal code column,
#' and if the codeset is very large, columns you will use to subset it
#' during use.
#'
#' Once a codeset is loaded, it is cached by name, and future calls to
#' load_codeset() with the same name will return the cached table.
#'
#' @inheritParams read_codeset
#' @param table_name An optional name for the table, if you want it to
#'   differ from `name`.
#' @param indexes A list of columns on which indexes should be created.
#' @param db A connection to the database into which to load the codeset.
#'
#' @return A tbl pointing to the table in the database
#' @md
load_codeset <- function(name,
                         col_types = 'iccc',
                         table_name = name,
                         indexes = list('concept_id'),
                         full_path = FALSE,
                         db = config('db_src')) {

  if (config('cache_enabled')) {
    if (is.null(config('_codesets'))) config('_codesets', list())
    cache <- config('_codesets')
    if (! is.null(cache[[name]])) return(cache[[name]])
  }
  codes <-
    copy_to_new(db,
                read_codeset(name, col_types),
                name = table_name,
                overwrite = TRUE,
                indexes = indexes)

  if (config('cache_enabled')) {
    cache[[name]] <- codes
    config('_codesets', cache)
  }

  codes
}


#' Find descendants of a codeset's elements
#'
#' Given a codeset in the database with (presumably standard) codes listed in
#' `concept_id`, create a new codeset with descendants.  By default, the new
#' codeset takes its name from the original, with `_exp` added.
#'
#' Note that this function is intended primarily for expanding a codeset
#' containing high-level terms during query execution.  For a similar method
#' better suited to construction of codesets to be saved for later use, see
#' [expand_codeset()] in `locode/build_concepts.R`.
#'
#' @param codeset A tbl containing the starting codeset
#' @param table_name The name to give the expanded codeset, if you're not happy
#'   with the default.
#'
#' @return A tbl containing the expanded codeset, with the standard 4 columns
#'   (see above), indexed on `concept_id`.
#' @md
get_descendants <- function(codeset, table_name = NA) {
  if (is.na(table_name)) {
    table_name <- dbplyr:::tbl_desc(codeset)
    table_name <- unlist(regmatches(table_name,
                                    regexec('(\\w+)>', table_name,
                                            perl = TRUE)))[2]
    table_name <- paste0(table_name, '_exp')
  }

 if (is.null(config('_codesets'))) config('_codesets', list())
  cache <- config('_codesets')
  if (! is.null(cache[[table_name]])) return(cache[[table_name]])

  codes <- vocabulary_tbl('concept_ancestor') %>%
    inner_join(codeset, by = c('ancestor_concept_id' = 'concept_id')) %>%
    select(descendant_concept_id) %>%
    inner_join(vocabulary_tbl('concept'),
               by = c('descendant_concept_id' = 'concept_id')) %>%
    select(concept_id = descendant_concept_id, concept_name, concept_code,
           vocabulary_id) %>%
    distinct() %>%
    compute_new(indexes = list('concept_id'), name = table_name)
  cache[[table_name]] <- codes
  config('_codesets', cache)
  codes
}
