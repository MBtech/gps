#!/bin/bash

source ../conf/gps-env.sh

if [ $# -lt 1 ]
then
    echo "You have to give the total number machines as an argument"
    exit
fi

USER=`whoami`

GPS_MASTER_SCRIPTS_DIR=`pwd`
echo $GPS_MASTER_SCRIPTS_DIR > .master_scripts_dir_name
GPS_DIR=`sed "s/\/master-scripts//" .master_scripts_dir_name`
rm .master_scripts_dir_name

echo $GPS_DIR
echo $USER

awk -v numMachines=$1 -v user=$USER -v sshOptions="$SSH_OPTS" -v gpsDir=$GPS_DIR '{\
  count++;\
  if (count <= numMachines) {\
    scpString = sprintf("scp %s %s/scripts/start_gps_node.sh %s@%s:%s/scripts", sshOptions, gpsDir, user, $0, gpsDir);\
    echoString = sprintf("echo \"running command: %s\"", scpString);\
    system(echoString);\
    system(scpString);\
   }\
}' slaves
