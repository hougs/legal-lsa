package com.cloudera.ds.legallsa

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
    val counts: RowMatrix = DataIO.read_matrix_input(sc, DataIO.count_path)
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = counts.computeSVD(args.rank,
      computeU = true)
    // Write out svd to files.
    val count_u = DataIO.writeRowMatrix(args.outUPath, svd.U)
    val count_s =  DataIO.writeSparkVector(args.outSPath, svd.s)
    val count_v = DataIO.writeSparkMatrix(args.outVPath, svd.V)
  }
}
