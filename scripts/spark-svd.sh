#!/usr/bin/env bash
#

COUNT_PATH=hdfs:///user/juliet/legallsa/count.csv
COUNT_HEADER_PATH=hdfs:///user/juliet/legallsa/count-header
COUNT_U=hdfs:///user/juliet/legallsa/out/count-u.csv
COUNT_S=hdfs:///user/juliet/legallsa/out/count-s.csv
COUNT_V=hdfs:///user/juliet/legallsa/out/count-v.csv
COUNT_CT_PATH=hdfs:///user/juliet/legallsa/out/count-ct.txt

TFIDF_PATH=hdfs:///user/juliet/legallsa/tfidf.csv
TFIDF_HEADER_PATH=hdfs:///user/juliet/legallsa/tfidf-header
TFIDF_U=hdfs:///user/juliet/legallsa/out/tfidf-u.csv
TFIDF_S=hdfs:///user/juliet/legallsa/out/tfidf-s.csv
TFIDF_V=hdfs:///user/juliet/legallsa/out/tfidf-v.csv
TFIDF_CT_PATH=hdfs:///user/juliet/legallsa/out/tfidf-ct.txt
MASTER=$1
RANK=40

export SPARK_HOME=/home/juliet/src/legal-lsa/spark-1.3.0-bin-hadoop2.4
export HADOOP_CONF_DIR=/etc/hadoop/conf

$SPARK_HOME/bin/spark-submit --class com.cloudera.ds.svdbench.SparkSVD \
  --conf spark.yarn.jar=hdfs:///user/juliet/bin/spark-1.3.0-bin-hadoop2.4/lib/spark-assembly-1.3.0-hadoop2.4.0.jar \
  --master $MASTER --executor-memory 14g --executor-cores 5 --num-executors 18 \
  --driver-class-path ./legal-lsa-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  ./legal-lsa-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  --matrixPath $COUNT_PATH --headerPath $COUNT_HEADER_PATH --outUPath $COUNT_U --outSPath $COUNT_S
  --outVPath $COUNT_V --conceptTermPath $COUNT_CT_PATH --rank $RANK

$SPARK_HOME/bin/spark-submit --class com.cloudera.ds.svdbench.SparkSVD \
  --conf spark.yarn.jar=hdfs:///user/juliet/bin/spark-1.3.0-bin-hadoop2.4/lib/spark-assembly-1.3.0-hadoop2.4.0.jar \
  --master $MASTER --executor-memory 14g --executor-cores 5 --num-executors 18 \
  --driver-class-path ./legal-lsa-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  ./legal-lsa-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
  --matrixPath $TFIDF_PATH --headerPath $TFIDF_HEADER_PATH --outUPath $TFIDF_U --outSPath $TFIDF_S
  --outVPath $TFIDF_V --conceptTermPath $TFIDF_CT_PATH --rank $RANK

