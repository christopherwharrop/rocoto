#!/bin/sh
set -x
sleep 1
echo top of script
unset LSB_PJL_TASK_GEOMETRY
env
# Get the directory where the WFM is installed
wfmdir=`dirname $0`

# Run the command passed in arguments
exec "$@"
