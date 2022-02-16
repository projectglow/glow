#!/bin/bash

# remove old ganglia rrd files

set -u

NUM_HOURS=$1
NUM_MINUTES=$((60*NUM_HOURS))

RRD_DIR="/var/lib/ganglia/rrds"
# rmove old rrd files
find $RRD_DIR -type f -mmin +$NUM_MINUTES -exec rm -v "{}" \;

# remove empty dir
find $RRD_DIR -depth -empty -type d -exec rmdir -v "{}" \;
echo "Done deleting files in $RRD_DIR that are more than $NUM_HOURS hours old"