
#!/bin/bash
#$ -pe mpi-fill 4
#$ -l exclusive=TRUE

HADOOP_HOME=/usr/local/hadoop
. $HADOOP_HOME/conf/hadoop-sge.sh
. /opt/sge/default/common/settings.sh

# This configures the environment and starts the Hadoop Cluster.
hadoop_start


##############################
# Place your hadoop job here #
##############################

# Copy the input file into the HDFS filesystem
#hadoop fs -put file.txt file.txt
HADOOP_MAPPER=$3
HADOOP_REDUCER=$3
# Running the hadoop task(s) here. I am specifying the jar, class, input, and output:
hadoop jar PA1.jar PA1

# Copying the output files from the HDFS filesystem
#hadoop fs -get output hadoop-output.$JOB_ID


# Stops the Hadoop cluster.
hadoop_end


