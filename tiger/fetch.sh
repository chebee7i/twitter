#!/usr/bin/env bash

# https://www.census.gov/geo/maps-data/data/tiger-line.html

# Info on files
curl -O https://www.census.gov/geo/maps-data/data/pdfs/tiger/tgrshp2014/TGRSHP2014_TechDoc.pdf

# SHP files for counties, states, and the 114th congressional districts.
curl -O ftp://ftp2.census.gov/geo/tiger/TIGER2014/COUNTY/tl_2014_us_county.zip
curl -O ftp://ftp2.census.gov/geo/tiger/TIGER2014/COUNTY/tl_2014_us_state.zip
curl -O ftp://ftp2.census.gov/geo/tiger/TIGER2014/COUNTY/tl_2014_us_cd114.zip


