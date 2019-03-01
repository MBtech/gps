#!/bin/bash

source ../conf/gps-env.sh

if [ $# -lt 3 ]
then
    echo "You have to give the total number of slaves, name of experiments, and other opts as arguments"
    echo "Example: TODO(semih): complete"
    echo "static-mina-cntryNet-UK, running pr=pagerank, with extra argument -dynamic and output will be suffixed with first-try)"
    exit
fi

GPS_MASTER_SCRIPTS_DIR=`pwd`
echo $GPS_MASTER_SCRIPTS_DIR > .master_scripts_dir_name
GPS_DIR=`sed "s/\/master-scripts//" .master_scripts_dir_name`
rm .master_scripts_dir_name

echo "master GPS_DIR=$GPS_DIR"
MASTER_GPS_ID=-1

echo "starting gps master locally"
cd ../scripts
./start_gps_node.sh $GPS_DIR/scripts $MASTER_GPS_ID ${1} ${2} "${3}"
cd ../master-scripts

awk -v numSlaves=$1 -v gpsDir=$GPS_DIR -v sshOptions="$SSH_OPTS" \
   -v v1="$1" -v v2="$2" -v v3="$3" '{\
  count++;\
  print  count, $0, numSlaves;\
  if (count <= numSlaves) {\
    printf("ssh %s %s \"%s/scripts/start_gps_node.sh %s %s %s \\\"%s\\\"\"", sshOptions, $0, gpsDir, gpsNodeId, v1, v2, v3);\
    sshString = sprintf("ssh %s %s \"%s/scripts/start_gps_node.sh %s/scripts %s %s %s \\\"%s\\\"\"", sshOptions, $0, gpsDir, gpsDir, (count - 1), v1, v2, v3);\
    echoString = sprintf("echo \"running command: %s\"", sshString);\
    system(echoString);\
    system(sshString);\
   }\
}' slaves
