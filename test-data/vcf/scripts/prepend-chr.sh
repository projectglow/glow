#!/bin/sh

awk 'BEGIN {OFS="\t"} {if ($1 !~ /^#/) $1="chr"$1} {print $0}' -
