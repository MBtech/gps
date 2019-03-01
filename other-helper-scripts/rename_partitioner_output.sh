#!/bin/bash 
if [ $# -ne 3 ]
then
    echo "You have to give the hadoop input directory the number of partitions and the network name as an argument"
    echo "Example:./rename_partitioner_output.sh 8 /user/semih/hadoop/input/eDonkey-partition_8_machines eDonkey2days2004"
    exit
fi

for i in `seq 1 $1`; do
   PARTITION_ID=$((i - 1)) 
   if [ ${PARTITION_ID} -lt 10 ]; then
       PART_NAME=0000${PARTITION_ID}
   else
       PART_NAME=000${PARTITION_ID}
   fi
   hadoop fs -mv ${2}/part-${PART_NAME} /user/semih/gps/partitions/${3}-partition-${PARTITION_ID}-of-${1}
done

