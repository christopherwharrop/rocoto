#! /bin/bash --login

if [[ -t 1 ]] ; then
    set -x
fi

set -ue

six_days_ago=$( date +%m%d%y -d "6 days ago" )
tgtfile="$HOME/sacct-cache/sacct.txt"
workdir=$( dirname "$tgtfile" )
[[ -d "$workdir" ]] || mkdir "$workdir" || sleep 3
temp=$( mktemp --tmpdir="$workdir" )

set +ue

(
    set -ue
    sacct -S "$six_days_ago" -L -o "jobid,user%30,jobname%30,partition%20,priority,submit,start,end,ncpus,exitcode,state%12" -P > "$temp" ;
    (( $( wc -l < "$temp" ) > 1 )) && /bin/mv -f "$temp" "$tgtfile"
)

if [[ -e "$temp" ]] ; then
    rm -f "$temp"
fi
