#!/bin/bash

# This is a wrapper script for `start_ganglia.py` which marshals user-specified environment
# variables from a file at `/databricks/spark/conf/spark-env.sh`
# Right now two environment variables are supported:
#  a) DATABRICKS_GANGLIA_ENABLED - set to `true` to start ganglia services
#  b) DATABRICKS_GANGLIA_PORT - set to an alternative port to have ganglia send/listen on an
#                               alternative port.
#  c) DATABRICKS_GANGLIA_SNAPSHOT_PERIOD_MINUTES - how often to take an image snapshot of the
#                                                  ganglia UI

source $SPARK_HOME/conf/spark-env.sh

# by default, use port 8649
port=${DATABRICKS_GANGLIA_PORT:-8649}

snapshot_period=${DATABRICKS_GANGLIA_SNAPSHOT_PERIOD_MINUTES:-15}

rrd_cleanup_threshold_hours=${DATABRICKS_GANGLIA_RRD_CLEANUP_THRESHOLD_HOURS:-24}

if [ "${DATABRICKS_GANGLIA_ENABLED:-true}" = true ]; then
  $SPARK_HOME/scripts/ganglia/start_ganglia \
    --driver_port $port \
    --snapshot_period_min $snapshot_period \
    --rrd_cleanup_threshold_hours $rrd_cleanup_threshold_hours \
    "$@" \
    &
else
  echo "Ganglia is not enabled (DATABRICKS_GANGLIA_ENABLED = $DATARICKS_GANGLIA_ENABLED)"
fi
