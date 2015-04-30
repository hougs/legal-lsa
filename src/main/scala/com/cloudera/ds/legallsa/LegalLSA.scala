package com.cloudera.ds.legallsa

import com.cloudera.datascience.lsa.RunLSA
import com.quantifind.sumac.{ArgMain, FieldArgs}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition}
import org.apache.spark.{SparkConf, SparkContext}

class LSAArgs extends FieldArgs {
  var matrixPath = ""
  var headerPath = ""
  var conceptTermPath = ""
  var outUPath = ""
  var outSPath = ""
  var outVPath = ""
  var master = "yarn-client"
  var rank = 20

}

object LegalLSA extends ArgMain[LSAArgs] {
  /** Configure our Spark Context. */
  def configure(master: String): SparkConf = {
    val conf = new SparkConf()
    conf.setMaster(master)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.setAppName("Legal SVD")
    conf
  }

  def main(args: LSAArgs): Unit = {
    val sc = new SparkContext(configure(args.master))
    val counts: RowMatrix = DataIO.read_matrix_input(sc, DataIO.count_path)
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = counts.computeSVD(args.rank,
      computeU = true)
    // Write out svd to files.
    val count_u = DataIO.writeRowMatrix(args.outUPath, svd.U)
    val count_s =  DataIO.writeSparkVector(args.outSPath, svd.s)
    val count_v = DataIO.writeSparkMatrix(args.outVPath, svd.V)

    val termIds: Map[Int, String] = DataIO.readHeader(sc, args.headerPath)
    var conceptTerms = new StringBuilder
    conceptTerms ++= "Singular values: " + svd.s
    val topConceptTerms = RunLSA.topTermsInTopConcepts(svd, 10, 10, termIds)
    for (terms <- topConceptTerms) {
      conceptTerms ++= "Concept terms: " + terms.map(_._1).mkString(", ") + "\n"
    }
    sc.parallelize(conceptTerms).saveAsTextFile(args.conceptTermPath)
  }
}
