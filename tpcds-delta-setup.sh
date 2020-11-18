#!/bin/bash

function usage {
	echo "Usage: tpcds-setup.sh scale_factor [temp_directory]"
	exit 1
}

function runcommand {
	if [ "X$DEBUG_SCRIPT" != "X" ]; then
		$1
	else
		$1 2>/dev/null
	fi
}

if [ ! -f tpcds-gen/target/tpcds-gen-1.0-SNAPSHOT.jar ]; then
	echo "Please build the data generator with ./tpcds-build.sh first"
	exit 1
fi
which hive > /dev/null 2>&1
if [ $? -ne 0 ]; then
	echo "Script must be run where Hive is installed"
	exit 1
fi

# Get the parameters.
SCALE=$1
UPDATES=$2
DIR=$3
if [ "X$DEBUG_SCRIPT" != "X" ]; then
	set -x
fi

# Sanity checking.
if [ X"$SCALE" = "X" ]; then
	usage
fi
if [ X"$DIR" = "X" ]; then
	DIR=/tmp/tpcds-delta-generate
fi
if [ $SCALE -eq 1 ]; then
	echo "Scale factor must be greater than 1"
	exit 1
fi

HIVE="beeline -n hive -u 'jdbc:hive2://localhost:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2?tez.queue.name=default' "

# Do the actual data load.
hdfs dfs -mkdir -p ${DIR}

for ((i=1;i<=$UPDATES;i++)); do
do
    if test "$(jobs | wc -l)" -ge 8; then
      wait -n
    fi
    {
      hdfs dfs -ls ${DIR}/${SCALE}_$i > /dev/null
      if [ $? -ne 0 ]; then
        echo "Generating data at scale factor $SCALE."
        (cd tpcds-gen; hadoop jar target/*.jar -d ${DIR}/${SCALE}_$i/ -s ${SCALE} -u ${UPDATE})
      fi
      hdfs dfs -ls ${DIR}/${SCALE}_$i > /dev/null
      if [ $? -ne 0 ]; then
        echo "Data generation failed, exiting."
        exit 1
      fi
      hadoop fs -chmod -R 777  ${DIR}/${SCALE}_$i

      # Create the text/flat tables as external tables. These will be later be converted to ORCFile.
      echo "Loading text data into external tables."
      runcommand "$HIVE  -i settings/load-flat.sql -f ddl-tpcds/text/deltatables.sql --hivevar DB_DIMS=tpcds_text_${SCALE} -hivevar DB_DELTA=tpcds_delta_text_${SCALE}_$i --hivevar LOCATION=${DIR}/${SCALE}_$i"
    } &
done

wait
echo "TPC-DS text data generation & load complete."


