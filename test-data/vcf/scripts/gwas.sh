#!/bin/sh

# Simulates a GWAS interface from VCF to space-separated CSV.
# If there is at least one sample and one variant in the VCF:
#   - Print a header
#   - For each variant:
#     - Print the site and a p-value.

awk 'BEGIN {
       FS="\t"
       printedHeaderLine="false"
       nHeaderSamples=0
     }
     {
        if ($1=="#CHROM") {
          nHeaderSamples=NF-9
        }
     }
     {
       if ($1 !~ /^#/ && nHeaderSamples==NF-9 && nHeaderSamples>0) {
         if (printedHeaderLine=="false") {
           print "CHR","POS","pValue"
           printedHeaderLine="true"
         }
         print $1,$2,0.5
       }
     }'
