#!/bin/bash

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

for i in `seq 1 $1`; do
   ILC_ID=${MACHINES[$i]}
   PREGEL_NODE_ID=$((i - 1))
  if [ $ILC_ID -eq 0 ]; then
    echo "copying local output to logs"
    cp /home/semih/var/tmp/tcp_sending_receiving_tester_mina_64MB_${1}machines_machine${PREGEL_NODE_ID}_run_${2}.txt /home/semih/var/logs/
  else
    echo "copying machine${i}'s output to /home/semih/var/logs/"
    scp ilc${ILC_ID}:/home/semih/var/tmp/tcp_sending_receiving_tester_mina_64MB_${1}machines_machine${PREGEL_NODE_ID}_run_${2}.txt /home/semih/var/logs/
  fi
done