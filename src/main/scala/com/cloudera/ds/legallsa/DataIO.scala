package com.cloudera.ds.legallsa

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Vectors, Matrix, Vector}
import org.apache.spark.rdd.RDD

object DataIO {
  val raw_count_path = "hdfs:///user/juliet/legallsa/Word_stats_MACs_0708.csv"
  val tfidf_path = "hdfs:///user/juliet/legallsa/Word_stats_MACs_0708_TF-IDF.csv"

  def read_marix_input(sc: SparkContext, path: String): RowMatrix = {
    val lines: RDD[String] = sc.textFile(path)
    val header = lines.first()
    val vectors = lines.map{ line =>
      val arr =line.split(",")
      val word_arr = arr.slice(12, arr.length)

      val nonzeroElems: Seq[(Int, Double)] = word_arr.map(elem => elem.toDouble).zipWithIndex.filter(tuple =>
      tuple._1 != 0.0).map(tuple => (tuple._2, tuple._1)).toSeq
      Vectors.sparse(word_arr.length, nonzeroElems)
    }
    new RowMatrix(vectors)
  }


  /** Writes a Spark matrix to a UTF-8 encoded csv file. */
  def writeSparkMatrix(path: String, matrix: Matrix) = {
    val colLength = matrix.numRows
    val csvMatrix = matrix.toArray.grouped(colLength).map(column => column.mkString(",")).mkString("\n")
    Files.write(Paths.get(path), csvMatrix.getBytes(StandardCharsets.UTF_8))
  }

  /** Writes a spark vector to a UTF-8 encoded csv file. */
  def writeSparkVector(path: String, vector: Vector) = {
    Files.write(Paths.get(path), vector.toArray.mkString(",").getBytes(StandardCharsets.UTF_8))
  }
}
