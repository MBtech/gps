#!/bin/bash

source ../conf/gps-env.sh

if [ $# -lt 1 ]
then
    echo "You have to give the total number machines as an argument"
    echo "Example:./make_gps_log_directories_in_slaves.sh 8"
    exit
fi

USER=`whoami`

awk -v numMachines=$1 -v user=$USER -v sshOptions="$SSH_OPTS" -v gpsLogDir=$GPS_LOG_DIRECTORY '{\
  count++;\
  print  count, $0, numMachines;\
  if (count <= numMachines) {\
    sshString = sprintf("ssh %s %s@%s \"mkdir -p %s\"", sshOptions, user, $0, gpsLogDir);\
    echoString = sprintf("echo \"running command: %s\"", sshString);\
    system(echoString);\
    system(sshString);\
   }\
}' slaves