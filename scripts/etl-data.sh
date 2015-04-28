#!/usr/bin/env bash
# usage: etl-data projecthome
# where projecthome is the root dir of this project.

ROOT_DIR=$1

FREQ_PATH=${ROOT_DIR}/Word_stats_MACs_0708.csv
TFIDF_PATH=${ROOT_DIR}/Word_stats_MACs_0708_TF-IDF.csv

# ETL Freq data
$SPARK_HOME/bin/spark-submit --class com.cloudera.ds.svdbench.SparkSVD \
  --conf spark.yarn.jar=hdfs:///user/juliet/bin/spark-1.3.0-bin-hadoop2.4/lib/spark-assembly-1.3.0-hadoop2.4.0.jar \
  --master $MASTER --executor-memory 14g --executor-cores 5 --num-executors 18 \
  --driver-class-path ./target/svd-benchmark-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  ./target/svd-benchmark-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  --inPath $INPUT_PATH --outUPath $OUT_U --outSPath $OUT_S --outVPath $OUT_V --rank $RANK

# ETL