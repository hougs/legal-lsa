package com.cloudera.ds.svdbench

import org.apache.mahout.math.VectorWritable
import org.scalatest.ShouldMatchers

class TestGenerateMatrix extends SparkTestUtils with ShouldMatchers {
  def countNonZero(vector: VectorWritable) = {
    vector.get().getNumNonZeroElements
  }

  sparkTest("Test Generate Matrix. ") {
    val nRows = 300
    val nCols = 200
    val sparsity = 0.1
    val matrix = GenerateMatrix.generateSparseMatrix(nRows, nCols, sparsity, 10, sc)
    matrix.count() shouldEqual 300L
    val nNonZero = matrix.values.map(vec => vec.get.getNumNonZeroElements).sum()
    println( nNonZero/(nRows * nCols))
    math.abs(nNonZero/(nRows * nCols) - sparsity)  should be < 0.05
  }
}
