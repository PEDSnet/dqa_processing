#!/bin/bash

if [[ ! -d 'locode' ]]
then
    echo "Please execute from top-level directory"
    exit 1
fi
if [[ $(grep '^Provide a summary of the scientific' locode/request_info.Rmd) ]]
then
    echo "Please edit request-specific description locode/request_info.Rmd"
    exit 1
fi
if [[ $(grep 'Title of Project' code/req_info.R) ]]
then
    echo "Please edit request-specific specifications code/req_info.R"
    exit 1
fi


name=`basename $PWD`

Rscript -e 'library(knitr); knit("locode/request_info.Rmd")'
pandoc -s -o README.pdf request_info.md
rm -f request_info.md
zip -r9X "$name".zip README.pdf code local results specs site -x \*/.\* \*/\*~ results/?\* local/?\*
rm -f README.pdf

mkdir "$name"
(cd "$name" && unzip ../"$name".zip)
rm -f "$name".zip
zip -r9X "$name".zip "$name"
rm -rf "$name"
