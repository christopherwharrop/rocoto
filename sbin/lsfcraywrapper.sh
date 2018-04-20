#!/bin/sh

sleep 1

unset LSB_PJL_TASK_GEOMETRY

# Get the directory where the WFM is installed
wfmdir=`dirname $0`

# Run the command passed in arguments
exec "$@"
