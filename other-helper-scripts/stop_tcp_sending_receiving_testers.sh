PROJECTS_DIR=/home/semih/projects
RELATIVE_PROJECTS_DIR=projects
PREGEL_DIR=pregel
SRC_DIR=src
SCRIPTS_DIR=scripts
echo "cding to ${PROJECTS_DIR}/${PREGEL_DIR}/${SCRIPTS_DIR}"
echo cd ${PROJECTS_DIR}/${PREGEL_DIR}/${SCRIPTS_DIR}
echo "syndicating ${PROJECTS_DIR}/${PREGEL_DIR}/${SCRIPTS_DIR}"
echo /root/bin/syndicate


echo "cding back to ${PROJECTS_DIR}/${PREGEL_DIR}"
cd ${PROJECTS_DIR}/${PREGEL_DIR}

for i in `seq 0 20`; do
  if [ $i -eq 2 -o $i -eq 8 -o $i -eq 10 -o $i -eq 12 ]
  then
    echo "skipping machine ${i}"
  else 
    if [ $i -eq 0 ]
    then
      echo "running stop_tcp_sending_receiving_tester.sh in local machine"
      ./${SCRIPTS_DIR}/stop_tcp_sending_receiving_tester.sh &
    else
      echo "running stop_tcp_sending_receiving_tester.sh in machine ilc${i}"
      ssh ilc${i} "./${RELATIVE_PROJECTS_DIR}/${PREGEL_DIR}/${SCRIPTS_DIR}/stop_tcp_sending_receiving_tester.sh"
    fi
  fi
done