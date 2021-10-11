#!/usr/bin/env bash
rm -r /opt/liftover
mkdir /opt/liftover
curl https://raw.githubusercontent.com/broadinstitute/gatk/master/scripts/funcotator/data_sources/gnomAD/b37ToHg38.over.chain --output /opt/liftover/b37ToHg38.over.chain
