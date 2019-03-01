#!/bin/bash

source ../conf/gps-env.sh

TAR_NAME=gps-$GPS_VERSION-slave.tar.gz

if [ $# -lt 1 ]
then
    echo "You have to give the total number machines as an argument"
    echo "Example:./copy_and_untar_gps_tar_to_slaves.sh 8"
    exit
fi

USER=`whoami`
GPS_MASTER_SCRIPTS_DIR=`pwd`
echo $GPS_MASTER_SCRIPTS_DIR > .master_scripts_dir_name
GPS_DIR=`sed "s/\/master-scripts//" .master_scripts_dir_name`
rm .master_scripts_dir_name

echo $USER
awk -v numMachines=$1 -v user=$USER -v gpsDir=$GPS_DIR -v sshOptions="$SSH_OPTS" -v tarName=$TAR_NAME '{\
  count++;\
  print  count, $0, numMachines;\
  if (count <= numMachines) {\
    scpString = sprintf("scp %s %s/%s %s@%s:~/", sshOptions, gpsDir, tarName, user, $0);\
    echoString = sprintf("echo \"running command: %s\"", scpString);\
    system(echoString);\
    system(scpString);\
    untarString = sprintf("ssh %s %s@%s \"tar xvzf %s\"", sshOptions, user, $0, tarName);\
    echoString = sprintf("echo \"running command: %s\"", untarString);\
    system(echoString);\
    system(untarString);\
   }\
}' slaves
