#!/usr/bin/env bash
#
# This script reads in a file representing a matrix from the specified path, uses Mahout's
# Stochastic SVD implementation to compute the SVD (M=U*S*V^{*}) of the matrix, and writes out a file in HDFS
# representing the cleaned eigenvectors. We are doing a ssvd factorization followed by a poer iteration to
# improve accuracy.
#
#
# usage: stochastic-svd.sh inputPath outPath rank
# Where inputPath and outPath are paths in hdfs and rank specifies the number of signular vectors to compute.

INPUT=$1
OUTPUT=$2
RANK=$3

mahout ssvd --input $INPUT --output $OUTPUT --rank $RANK --reduceTasks 10 -ow