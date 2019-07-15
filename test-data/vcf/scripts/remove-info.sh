#!/bin/sh

awk 'BEGIN {OFS="\t"} {if ($1 !~ /^#/) $8 = "."} {if ($1 !~ /^##INFO/) print $0}' -
