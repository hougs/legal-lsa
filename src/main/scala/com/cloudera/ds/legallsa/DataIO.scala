package com.cloudera.ds.legallsa

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Vectors, Matrix, Vector}
import org.apache.spark.rdd.RDD

object DataIO {
  val count_path = "hdfs:///user/juliet/legallsa/count.csv"
  val count_header_path = "hdfs:///user/juliet/legallsa/count-header.csv"
  val tfidf_path = "hdfs:///user/juliet/legallsa/tfidf.csv"
  val tfidf_header_path = "hdfs:///user/juliet/legallsa/tfidf-header.csv"

  def read_matrix_input(sc: SparkContext, path: String): RowMatrix = {
    val lines: RDD[String] = sc.textFile(path)
    val header = lines.first()
    val vectors = lines.map{ line =>
      val arr =line.split(",")
      val word_arr = arr.slice(12, arr.length)

      val nonzeroElems: Seq[(Int, Double)] = word_arr.map(elem => elem.toDouble).zipWithIndex.filter(tuple =>
      tuple._1 != 0.0).map(tuple => (tuple._2, tuple._1)).toSeq
      Vectors.sparse(word_arr.length, nonzeroElems)
    }
    vectors.cache()
    new RowMatrix(vectors)
  }


  /** Writes a Spark matrix to a UTF-8 encoded csv file. */
  def writeSparkMatrix(path: String, matrix: Matrix) = {
    val colLength = matrix.numRows
    val csvMatrix = matrix.toArray.grouped(colLength).map(column => column.mkString(",")).mkString("\n")
    Files.write(Paths.get(path), csvMatrix.getBytes(StandardCharsets.UTF_8))
  }

  def writeRowMatrix(path: String, rowMatrix: RowMatrix) = {
    rowMatrix.rows.map{ vector =>
      vector.toArray.map(_.toString).mkString(",")
    }.saveAsTextFile(path)
  }

  /** Writes a spark vector to a UTF-8 encoded csv file. */
  def writeSparkVector(path: String, vector: Vector) = {
    Files.write(Paths.get(path), vector.toArray.mkString(",").getBytes(StandardCharsets.UTF_8))
  }
}
