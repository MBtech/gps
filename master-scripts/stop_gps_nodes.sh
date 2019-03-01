#!/bin/bash
source ../conf/gps-env.sh

USER=`whoami`
GPS_MASTER_SCRIPTS_DIR=`pwd`
echo $GPS_MASTER_SCRIPTS_DIR > .master_scripts_dir_name
GPS_DIR=`sed "s/\/master-scripts//" .master_scripts_dir_name`
rm .master_scripts_dir_name

MASTER_GPS_ID=-1

echo "stopping gps master locally"
../scripts/stop_gps_node.sh

awk -v numMachines=$1 -v user=$USER -v gpsDir=$GPS_DIR -v sshOptions="$SSH_OPTS" -v gpsNodeId=$GPS_NODE_ID \
   -v v1="$1" -v v2="$2" -v v3="$3" -v v4="$4" -v v5="$5" '{\
  count++;\
  print  count, $0, numMachines;\
  sshString = sprintf("ssh %s %s@%s \"%s/scripts/stop_gps_node.sh\"", sshOptions, user, $0, gpsDir);\
  echoString = sprintf("echo \"running command: %s\"", sshString);\
  system(echoString);\
  system(sshString);\
}' slaves
