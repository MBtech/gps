#!/bin/bash
source machines.txt

if [ $# -ne 2 ]
then
    echo "You have to give the total number of machines and the prefix of the output files as arguments"
    echo "Example:./copy_output_logs_to_local.sh 8 static-mina-LiveJournal01"
    exit
fi


for i in `seq 1 $1`; do
   ILC_ID=${MACHINES[$i]}
   GPS_MACHINE_ID=$((i - 1))
  if [ $ILC_ID -eq 0 ]; then
    echo "copying local output to logs"
    cp /home/semih/var/tmp/${2}-machine${GPS_MACHINE_ID}-output.txt /home/semih/var/logs/
  else
    echo "copying machine${i}'s output to /home/semih/var/logs/"
    scp ilc${ILC_ID}:/home/semih/var/tmp/${2}-machine${GPS_MACHINE_ID}-output.txt /home/semih/var/logs/
  fi
done
