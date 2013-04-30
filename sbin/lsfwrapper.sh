#!/bin/sh

# Get the directory where the WFM is installed
wfmdir=`dirname $0`

# Run the command passed in arguments
$*

# Get the status of the command
error=$?

# If we are using POE, call special exit script
if [ "${LSF_PJL_TYPE}" == "poe" ]; then
  export LSB_PJL_TASK_GEOMETRY="{(0)}"
  mpirun.lsf ${wfmdir}/lsfexit.sh ${error}
  exit ${error}
else
  exit ${error}
fi
