#!/usr/bin/env bash
# This script reads in a file representing a matrix from the specified path, uses spark to compute
# the SVD (M=U*S*V^{*}) of the matrix, and writes out a file in HDFS for U, and two local files for
# a vector representation of the singular vectors from S and V.
#
# usage: spark-svd.sh inputPath outUPath outSPath outVPath master rank
# Where inputPath and outUPath are paths in hdfs, outSPath and outVPath are local paths, and
# master is a URL for a Spark master.

INPUT_PATH=$1
OUT_U=$2
OUT_S=$3
OUT_V=$4
MASTER=$5
RANK=$6

export SPARK_HOME=$7
export HADOOP_CONF_DIR=/etc/hadoop/conf

$SPARK_HOME/bin/spark-submit --class com.cloudera.ds.svdbench.SparkSVD \
  --conf spark.yarn.jar=hdfs:///user/juliet/bin/spark-1.3.0-bin-hadoop2.4/lib/spark-assembly-1.3.0-hadoop2.4.0.jar \
  --master $MASTER --executor-memory 14g --executor-cores 5 --num-executors 18 \
  --driver-class-path ./target/svd-benchmark-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  ./target/svd-benchmark-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  --inPath $INPUT_PATH --outUPath $OUT_U --outSPath $OUT_S --outVPath $OUT_V --rank $RANK
