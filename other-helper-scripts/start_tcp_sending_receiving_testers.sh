#!/bin/bash
PROJECTS_DIR=/home/semih/projects
RELATIVE_PROJECTS_DIR=projects
PREGEL_DIR=pregel
SRC_DIR=src
CONF_DIR=conf
SCRIPTS_DIR=scripts
LIBS_DIR=libs

if [ $# -ne 2 ]
then
    echo "You have to give the total number of machines and attempt number as arguments"
    echo "Example:./start_nodes.sh 8 4 (meaning pregel node 3 is running on ilc5) for the 4th time"
    exit
fi

MACHINES[1]=0
MACHINES[2]=1
MACHINES[3]=4
MACHINES[4]=5
MACHINES[5]=6
MACHINES[6]=7
MACHINES[7]=9
MACHINES[8]=11
MACHINES[9]=13
MACHINES[10]=14
MACHINES[11]=15
MACHINES[12]=16
MACHINES[13]=17
MACHINES[14]=18
MACHINES[15]=19
MACHINES[16]=20
MACHINES[17]=0
MACHINES[18]=1
MACHINES[19]=4
MACHINES[20]=5
MACHINES[21]=6
MACHINES[22]=7
MACHINES[23]=9
MACHINES[24]=11
MACHINES[25]=13
MACHINES[26]=14
MACHINES[27]=15
MACHINES[28]=16
MACHINES[29]=17
MACHINES[30]=18
MACHINES[31]=19
MACHINES[32]=20

echo "cding to ${PROJECTS_DIR}/${PREGEL_DIR}/${SRC_DIR}"
cd ${PROJECTS_DIR}/${PREGEL_DIR}/${SRC_DIR}
echo "syndicating ${PROJECTS_DIR}/${PREGEL_DIR}/${SRC_DIR}"
/root/bin/syndicate

echo "cding to ${PROJECTS_DIR}/${LIBS_DIR}"
cd ${PROJECTS_DIR}/${LIBS_DIR}
echo "syndicating ${PROJECTS_DIR}/${LIBS_DIR}"
/root/bin/syndicate

echo "cding to ${PROJECTS_DIR}/${PREGEL_DIR}/${CONF_DIR}"
cd ${PROJECTS_DIR}/${PREGEL_DIR}/${CONF_DIR}
echo "syndicating ${PROJECTS_DIR}/${PREGEL_DIR}/${CONF_DIR}"
/root/bin/syndicate

echo "cding to ${PROJECTS_DIR}/${PREGEL_DIR}/${SCRIPTS_DIR}"
cd ${PROJECTS_DIR}/${PREGEL_DIR}/${SCRIPTS_DIR}
echo "syndicating ${PROJECTS_DIR}/${PREGEL_DIR}/${SCRIPTS_DIR}"
/root/bin/syndicate

echo "cding back to ${PROJECTS_DIR}/${PREGEL_DIR}"
cd ${PROJECTS_DIR}/${PREGEL_DIR}
for i in `seq 1 $1`; do
   ILC_ID=${MACHINES[$i]}
   PREGEL_NODE_ID=$((i - 1))
  if [ $ILC_ID -eq 0 ]; then
    echo "running pregel node locally"
    ./${SCRIPTS_DIR}/run_tcp_sending_receiving_tester.sh $PREGEL_NODE_ID ${1} ${2}&
  else
     ssh ilc${ILC_ID} "./${RELATIVE_PROJECTS_DIR}/${PREGEL_DIR}/${SCRIPTS_DIR}/run_tcp_sending_receiving_tester.sh $PREGEL_NODE_ID ${1} ${2}"
  fi
done
