#!/usr/bin/env Rscript

require(readr)
require(stringr)
require(dplyr)

#' Set up environment
#' @md
#'
#' Sources the files needed to execute vocabulary queries
#'
#' @return TRUE
setup <- function() {
  source('code/config.R')
  source('site/site_info.R')
  source('code/util_tbl.R')
}

#' Find OHDSI concept data for concept codes
#' @md
#'
#' Given a set of concept codes and potential vocabularies of origin,
#' find the concept table entries for the codes.
#'
#' @param codes A tbl with the codes in the `code` column, and a free
#'   text description of each code in the `description` column
#' @param vocabularies A character vector of potential vocabularies of
#'   origin
#'
#' @return A tbl in the vocabulary database with the resulting concept
#'   information, including `concept_id`, `concept_code`, `concept_name`,
#'   and `vocabulary_id`.  The original `description` is included as well.
find_concepts_from_codes <- function(codes, vocabularies) {
  vocabulary_tbl('concept') %>%
    filter(is.na(invalid_reason),
           vocabulary_id %in% vocabularies) %>%
    inner_join(codes, by = c('concept_code' = 'code'), copy = TRUE) %>%
    select(concept_code, description, concept_id, concept_name, vocabulary_id)
}

#' Find OHDSI concept data for concept IDs
#' @md
#'
#' Given a set of concept IDs, find the concept table entries for the codes.
#'
#' @param codes A tbl with the concept IDs in the `concept_id` column
#' @param valid_only A Boolean value indicating whether the returned data should contain
#'   only currently valid concepts.  Defaults to FALSE.
#'
#' @return A tbl in the vocabulary database with the resulting concept
#'   information, including `concept_id`, `concept_code`, `concept_name`,
#'   and `vocabulary_id`.  The original `description` is included as well.
find_concepts_from_cids <- function(cids, valid_only = FALSE) {
  conc <- vocabulary_tbl('concept')
  if (valid_only) conc <- filter(concept, is.na(invalid_reason))
  conc %>% inner_join(cids, by = 'concept_id', copy = TRUE) %>%
    select(concept_id, concept_name, concept_code, vocabulary_id) %>%
    mutate(description = concept_name)
}


#' Find standard concept_ids
#' @md
#'
#' Given a set of concept_ids, find the corresponding standard
#'   concepts
#'
#' @param concepts A tbl in the form produced by [find_concepts()]
#'
#' @return A tbl with the standard concepts.  For each, the standard
#'   concept characteristics are in `std_concept_id`, `concept_code`,
#'   `concept_name`, and `vocabulary_id`, while the source concept
#'   characteristics are in `source_concept_id` and
#'   `source_vocabulary_id`, and the characteristics of the originally
#'   supplied code are in `orig_code` and `orig_description`.
standardize_concepts <- function(concepts) {
  orig_tags <- concepts %>%
    select(concept_id, orig_code = concept_code,
           source_vocabulary_id = vocabulary_id,
           orig_description = description)
  distinct(concepts, concept_id) %>%
    inner_join(vocabulary_tbl('concept_relationship'),
               by = c('concept_id' = 'concept_id_1'), copy = TRUE) %>%
    filter(relationship_id == 'Maps to', is.na(invalid_reason)) %>%
    select(source_concept_id = concept_id, concept_id_2) %>%
    distinct() %>%
    inner_join(vocabulary_tbl('concept'),
               by = c('concept_id_2' = 'concept_id')) %>%
    select(source_concept_id, std_concept_id = concept_id_2,
           std_concept_name = concept_name,
           std_concept_code = concept_code,
           std_vocabulary_id = vocabulary_id) %>%
    distinct() %>%
    inner_join(orig_tags, by = c('source_concept_id' = 'concept_id'))
}


#' Find descendants of standard concepts
#' @md
#'
#' @param std_concepts A table contaning standard concepts, in the
#'   form returned by [standardize_concepts()].
#'
#' @return A tbl in the vocabulary database, with the descendant data
#'   in `std_concept_id`, `std_concept_name`, `std_concept_code`, and
#'   `std_vocabulary_id`, and each row annotated with
#'   `ancestor_concept_id` and `ancestor_concept_name`, containing the original
#'   `std_concept_id` and `std_concept_name`, respectively.
expand_codeset <- function(std_concepts) {
  probe <- std_concepts %>% select(ancestor_concept_id = std_concept_id,
                                   ancestor_concept_name = std_concept_name)
  probe %>%
    inner_join(vocabulary_tbl('concept_ancestor'),.,
               by = 'ancestor_concept_id',
               copy = TRUE) %>%
    inner_join(vocabulary_tbl('concept'),
               by = c('descendant_concept_id' = 'concept_id')) %>%
    select(std_concept_id = descendant_concept_id,
           std_concept_name = concept_name,
           std_concept_code = concept_code,
           std_vocabulary_id = vocabulary_id,
           ancestor_concept_id) %>%
    distinct() %>%
    inner_join(probe,
               by = 'ancestor_concept_id',
               copy = TRUE)
}

#' Find and map concepts in OHDSI vocabulary from a tbl of codes
#' @md
#'
#' Given a tbl of codes and a vector of vocabulary IDs, call
#' [find_concepts()] and [standardize_concepts()], returning the
#' results and providing a progress summary.
#'
#' @param codes A tbl with the codes in the `code` column, and a free
#'   text description of each code in the `description` column
#' @param vocabularies A character vector of potential vocabularies of
#'   origin
#'
#' @return A list with two elements named `found` and `standardized`,
#'   containing the results of the respective functions
process_codes <- function(codes, vocabularies) {
  message('Source codes: ', collect(count(distinct(codes)))$n)
  found <- find_concepts(codes, vocabularies)
  message('Source concepts: ', collect(count(distinct(found)))$n)
  mapped <- standardize_concepts(found)
  message('Mapped concepts: ',
          collect(count(distinct(mapped, source_concept_id)))$n)
  message('Standard concepts: ',
          collect(count(distinct(mapped, std_concept_id)))$n)
  list(found = found, standardized = mapped)
}

#' Find and map concepts for contents of a CSV file
#' @md
#'
#' Read a CSV file containing codes, and attempt to locate standard
#' OHDSI concepts corresponding to those codes.  The CSV should have a
#' column named `code` containing the codes and `description`
#' containing the descriptions.
#'
#' If candidate vocabularies are not explicitly provided, a check is
#' made for words in parentheses at the end of each description line,
#' and any such words are used to construct the set of candidate
#' vocabularies.
#'
#' In addition to returning the results, the data returned by
#' [find_concepts()] is written to a CSV named like the input, but
#' with a `_src_concepts` suffix added, and the data returned by
#' [standardize_concepts()] is written to a CSV with a `_mapped`
#' suffix added.
#'
#' The latter CSV file, in particular, is intended as a substrate for
#' curation before expanding to descendant concepts.
#'
#' @param fname The name of the CSV file
#' @param vocabularies A character vector containing the names of
#'   candidate vocabularies.  Note that each code is searched for in
#'   all of the vocabularies listed.  Defaults to NA.
#'
#' @return The list returned by [process_set()].
process_codes_from_file <- function(fname, vocabularies = NA) {
  source_data <- read_csv(fname)

  if (is.na(vocabularies)) {
    vocabularies <- source_data %>%
      mutate(vocab = str_match(description, '\\((\\w+)\\)\\s*$')[,2]) %>%
      distinct(vocab)
    vocabularies <- vocabularies$vocab
    if (any(vocabularies == 'ICD10CM'))
      vocabularies <- c(vocabularies, 'ICD10')
  }

  ohdsi <- process_set(source_data, unique(vocabularies))
  outfbase <- sub('\\.csv$', '', fname)
  write_csv(collect(ohdsi$found), paste0(outfbase, '_src_concepts.csv'))
  write_csv(collect(ohdsi$standardized), paste0(outfbase, '_mapped.csv'))
  invisible(ohdsi)
}

#' Generate an expanded codeset from tbls of standard concepts
#' @md
#'
#' From a list of tbls containing standard concept information, build a merged codeset
#' consisting of the input concepts and their descendants, and write the result to a CSV
#' file in the `specs` directory.
#'
#' The input is typically a set of standard concepts generated by [standardize_concepts()]
#' and then reviewed manually to remove concepts not actually of interest.  This may occur
#' due to semantic mismatch between source and standard terminologies, or more often
#' because a pre-coordinated source concept will map to a standard concept of interest but
#' also a (frequently rather general) second standard concept describing disease context,
#' anatomic location, or a similar modifier.
#'
#' Summary messages are output after construction.
#'
#' @param concepts A tbl containing input concept information, in the format produced by
#'   [standardize_concepts()].
#' @param expand A Boolean value indicating whether descendant codes should be found
#'   via [expand_codeset()] before writing the result.  Defaults to TRUE.
#' @param outfname The path to which to write the output.  Defaults
#'   to `specs/final_codeset.csv`.
#'
#' @return The tbl produced by [expand_codeset()] during final codeset
#'   generation.  Note that only the `std_concept_id` and
#'   `std_concept_name` values from the input are passed to
#'   [expand_codeset()], so only those columns annotate the output.
merge_sets <- function(concepts,
                        expand = TRUE,
                        outfname = file.path('specs', 'final_codeset.csv')) {
  for (cs in concepts) {
    cids <- cs %>%
      distinct(std_concept_id, std_concept_name)
    merged <- if (exists('merged')) dplyr::union(merged, cids) else cids
  }
  if (expand) merged <- expand_codeset(merged)
  nostd <- function(x) sub('^std_','',x)
  out <- merged %>% distinct(std_concept_id, std_concept_name, std_concept_code,
                     std_vocabulary_id) %>%
    rename_at(vars(starts_with('std_')), nostd)

  write_csv(collect(out), outfname)

  if (expand) {
    message('Descendant summary:\n')
    # Have to just print this one, since message() flattens the tbl to
    # its component parts
    merged %>% group_by(ancestor_concept_name) %>%
      summarise(desc_ct = n()) %>% arrange(desc(desc_ct)) %>%
      collect() %>% print()
  }
  message('Final count: ',
          count(out) %>% pull(n))
  invisible(merged)
}


#' Generate an expanded codeset from CSVs of standard concepts
#' @md
#'
#' From a set of CSV files containing standard concept information,
#' build a merged codeset consisting of the input concepts and their
#' descendants, and write the result to a CSV file in the `specs`
#' directory, using [buld_final()].
#'
#' Progress messages are output during construction.
#'
#' @param infnames A character vector of paths to CSV files containing
#'   input concept information, in the format produced by
#'   [standardize_concepts()].
#' @param outfname The path to which to write the output.  Defaults
#' to `specs/final_codeset.csv`.
#'
#' @return The tbl produced by [expand_codeset()] during final codeset
#'   generation.  Note that only the `std_concept_id` and
#'   `std_concept_name` values from the input are passed to
#'   [expand_codeset()], so only those columns annotate the output.
merge_sets_from_files <- function(infnames,
                                  outfname = file.path('specs', 'final_codeset.csv')) {
  for (fn in infnames) {
    cs <- read_csv(fn)
    merged <- if (exists('merged')) dplyr::union(merged, cs) else cs
  }
  build_final(cs, outfname)
}
