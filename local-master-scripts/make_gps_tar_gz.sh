#!/bin/bash

source ../conf/gps-env.sh

cd ..
GPS_DIR="`pwd`"

echo "removing gps-$GPS_VERSION-slave.tar.gz"
rm $GPS_DIR/gps-$GPS_VERSION-slave.tar.gz

echo "removing gps-$GPS_VERSION-master.tar.gz"
rm $GPS_DIR/gps-$GPS_VERSION-master.tar.gz

cd $GPS_DIR
echo "making gps-$GPS_VERSION-slave.tar.gz into $GPS_DIR"
tar cvzf gps-$GPS_VERSION-slave.tar.gz --exclude *.svn* gps_node_runner.jar scripts/ conf/
#tar cvzf gps-$GPS_VERSION-slave.tar.gz --exclude *.svn* gps_node_runner.jar scripts/ conf/ libs/

# echo "making gps_$GPS_VERSION-master.tar.gz into $GPS_DIR"
# tar cvzf gps-$GPS_VERSION-master.tar.gz  --exclude *.svn* gps-$GPS_VERSION-slave.tar.gz master-scripts/

echo "done make_gps_tar_gz.sh..."
