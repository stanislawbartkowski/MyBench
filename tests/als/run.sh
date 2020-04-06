source $FUNCTIONSRC
setenv

prepare() {

  local -r BEGTEST=`testbeg prepare`

  read -r NUM_USERS_ALS NUM_PRODUCTS_ALS SPARSITY_ALS IMPLICITPREFS_ALS <<< `getconfvar als.users als.products als.sparsity als.implicitprefs`
  required_listofpars NUM_USERS_ALS NUM_PRODUCTS_ALS SPARSITY_ALS IMPLICITPREFS_ALS
  log_listofpars NUM_USERS_ALS NUM_PRODUCTS_ALS SPARSITY_ALS IMPLICITPREFS_ALS
  [ $IMPLICITPREFS_ALS == "true" ] && IMPLICIT="--implicitPrefs"

  OPTIONS="--numUsers $NUM_USERS_ALS \
           --numProducts $NUM_PRODUCTS_ALS $IMPLICIT \
           --sparsity $SPARSITY_ALS
          "

  sparkbenchjar RatingDataGenerator --dataPath $TMPINPUTDIR $OPTIONS

  testend $BEGTEST

}

old_prepare() {
  local -r TMP=`crtemp`
  read -r NUM_USERS_ALS NUM_PRODUCTS_ALS SPARSITY_ALS IMPLICITPREFS_ALS <<< `getconfvar als.users als.products als.sparsity als.implicitprefs`
  required_listofpars NUM_USERS_ALS NUM_PRODUCTS_ALS SPARSITY_ALS IMPLICITPREFS_ALS
  log_listofpars NUM_USERS_ALS NUM_PRODUCTS_ALS SPARSITY_ALS IMPLICITPREFS_ALS

  local -r BEGTEST=`testbeg prepare`

  OUTPUTPATH=$TMPINPUTDIR

  cat << EOF | cat >$TMP

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.random._
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.mllib.linalg.{Vectors, Vector}

val parallel = sc.defaultParallelism
val numPartitions = parallel / 2

val outputPath = "$OUTPUTPATH"
val numUsers = $NUM_USERS_ALS
val numProducts = $NUM_PRODUCTS_ALS
val sparsity = $SPARSITY_ALS
val implicitPrefs = $IMPLICITPREFS_ALS

val rawData: RDD[Vector] = RandomRDDs.normalVectorRDD(sc, numUsers, numProducts, numPartitions)
val rng = new java.util.Random()
val data = rawData.map{ v =>
      val a = Array.fill[Double](v.size)(0.0)
      v.foreachActive{(i,vi) =>
         if(rng.nextDouble <= sparsity){
           a(i) = vi
         }
      }
      Vectors.dense(a).toSparse
   }
data.saveAsObjectFile(outputPath)

EOF

  sparkshell $TMP

  testend $BEGTEST

}

alsrun() {
    local -r BEGTEST=`testbeg prepare`

    read -r NUM_USERS_ALS NUM_PRODUCTS_ALS SPARSITY_ALS IMPLICITPREFS_ALS <<< `getconfvar als.users als.products als.sparsity als.implicitprefs`
    required_listofpars NUM_USERS_ALS NUM_PRODUCTS_ALS SPARSITY_ALS IMPLICITPREFS_ALS
    log_listofpars NUM_USERS_ALS NUM_PRODUCTS_ALS SPARSITY_ALS IMPLICITPREFS_ALS

    read -r RANK_ALS NUM_RECOMMENDS_ALS NUM_ITERATIONS_ALS KYRO_ALS <<< `getconfvar als.rank als.recommends als.numIterations als.kyro`
    required_listofpars RANK_ALS NUM_RECOMMENDS_ALS NUM_ITERATIONS_ALS KYRO_ALS
    log_listofpars RANK_ALS NUM_RECOMMENDS_ALS NUM_ITERATIONS_ALS KYRO_ALS

    read -r PRODUCTBLOCKS_ALS USERBLOCKS_ALS LAMBDA_ALS <<< `getconfvar als.numUserBlocks als.numProductBlocks als.Lambda`
    required_listofpars PRODUCTBLOCKS_ALS USERBLOCKS_ALS LAMBDA_ALS
    log_listofpars PRODUCTBLOCKS_ALS USERBLOCKS_ALS LAMBDA_ALS


    [ $IMPLICITPREFS_ALS == "true" ] && IMPLICIT="--implicitPrefs"
    [ $KYRO_ALS == "true" ] && KRYO="--kryo"

    OPTIONS="--numUsers $NUM_USERS_ALS \
            --numProducts $NUM_PRODUCTS_ALS $IMPLICIT $KRYO\
            --rank $RANK_ALS \
            --numRecommends $NUM_RECOMMENDS_ALS \
            --numIterations $NUM_ITERATIONS_ALS \
            --numProductBlocks $PRODUCTBLOCKS_ALS \
            --numUserBlocks $USERBLOCKS_ALS \
            --lambda $LAMBDA_ALS
            "

    sparkbenchjar AvlExample --dataPath $TMPINPUTDIR $OPTIONS

    testend $BEGTEST
}

run() {
    remove_tmp
    prepare
    alsrun
}

test() {
#    remove_tmp
#    prepare
    alsrun
}

run
#test
