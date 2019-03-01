#!/bin/bash
source machines.txt

PROJECTS_DIR=/home/semih/projects
RELATIVE_PROJECTS_DIR=projects
GPS_DIR=gps
SRC_DIR=src
CONF_DIR=conf

if [ $# -ne 1 ]
then
    echo "You have to give the total number of machines to clean"
    echo "Example:./clean_directories.sh 8 (meaning the first 8 machine instances as specified by the array in the \
          machines.txt file will be cleaned)"
    exit
fi

for i in `seq 1 $1`; do
   ILC_ID=${MACHINES[$i]}
   GPS_WORKER_ID=$((i - 1))
  if [ $ILC_ID -eq 0 ]; then
      echo "cleaning gps/src directories on local machine (ilc0)"
      rm -rf ${PROJECTS_DIR}/${GPS_DIR}/${SRC_DIR}
  else
      echo "cleaning gps/src directories on machine ilc${ILC_ID}"
      ssh ilc${ILC_ID} "rm -rf ${PROJECTS_DIR}/${GPS_DIR}/${SRC_DIR}"
  fi
done
