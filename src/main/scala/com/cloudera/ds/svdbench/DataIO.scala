package com.cloudera.ds.svdbench

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.hadoop.io.{IntWritable, NullWritable}
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.mahout.math.{VectorWritable, DenseVector => DenseMahoutVector, Vector => MahoutVector}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, Vectors, Vector => SparkVector}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

object DataIO {

  /** Writes a Spark matrix to a UTF-8 encoded csv file. */
  def writeSparkMatrix(path: String, matrix: Matrix) = {
    val colLength = matrix.numRows
    val csvMatrix = matrix.toArray.grouped(colLength).map(column => column.mkString(",")).mkString("\n")
    Files.write(Paths.get(path), csvMatrix.getBytes(StandardCharsets.UTF_8))
  }

  /** Writes a spark vector to a UTF-8 encoded csv file. */
  def writeSparkVector(path: String, vector: SparkVector) = {
    Files.write(Paths.get(path), vector.toArray.mkString(",").getBytes(StandardCharsets.UTF_8))
  }
}
