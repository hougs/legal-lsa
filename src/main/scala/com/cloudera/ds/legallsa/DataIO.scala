package com.cloudera.ds.legallsa

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors}
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

  def readHeader(sc: SparkContext, path: String): Map[Int, String] = {
    val header = sc.textFile(path)
    val terms = header.flatMap(_.split(",")).collect()
    terms.zipWithIndex.map(_.swap).toMap
  }


  /** Writes a Spark matrix to a UTF-8 encoded csv file. */
  def writeSparkMatrix(path: String, matrix: Matrix, sc: SparkContext) = {
    val colLength = matrix.numRows
    val csvMatrix = matrix.toArray.grouped(colLength).map(column => column.mkString(",")).mkString("\n")
    sc.parallelize(csvMatrix).saveAsTextFile(path)
}
  def writeRowMatrix(path: String, rowMatrix: RowMatrix) = {
    rowMatrix.rows.map{ vector =>
      vector.toArray.map(_.toString).mkString(",")
    }.saveAsTextFile(path)
  }

  /** Writes a spark vector to a UTF-8 encoded csv file. */
  def writeSparkVector(path: String, vector: Vector, sc: SparkContext) = {
    sc.parallelize(vector.toArray.mkString(",")).saveAsTextFile(path)
  }
}
