#!/bin/sh

awk 'BEGIN {OFS="\t"} {if ($1 ~ /^#/) print $0}' -
