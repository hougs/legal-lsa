package com.cloudera.ds.svdbench

import com.quantifind.sumac.{ArgMain, FieldArgs}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition}
import org.apache.spark.{SparkConf, SparkContext}

class SVDArgs extends FieldArgs {
  var inPath = ""
  var outUPath = ""
  var outSPath = ""
  var outVPath = ""
  var master = "yarn-client"
  var rank = 20
}

object SparkSVD extends ArgMain[SVDArgs] {
  /** Configure our Spark Context. */
  def configure(master: String): SparkConf = {
    val conf = new SparkConf()
    conf.setMaster(master)
    conf.setAppName("Legal SVD")
    conf
  }

  def main(args: SVDArgs): Unit = {
    val sc = new SparkContext(configure(args.master))
    val matrix: RowMatrix = DataIO.readMahoutMatrix(args.inPath, sc)
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = matrix.computeSVD(args.rank,
      computeU = true)
    // Write out svd to files.
    val u = DataIO.writeSparkMatrix(args.outUPath, svd.U)
    val s =  DataIO.writeSparkVector(args.outSPath, svd.s)
    val v = DataIO.writeSparkMatrix(args.outVPath, svd.V)
  }
}
