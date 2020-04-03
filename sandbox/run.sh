source $FUNCTIONSRC
setenv

prepare() {
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
    read -r NUM_USERS_ALS NUM_PRODUCTS_ALS SPARSITY_ALS IMPLICITPREFS_ALS <<< `getconfvar als.users als.products als.sparsity als.implicitprefs`
    required_listofpars NUM_USERS_ALS NUM_PRODUCTS_ALS SPARSITY_ALS IMPLICITPREFS_ALS
    log_listofpars NUM_USERS_ALS NUM_PRODUCTS_ALS SPARSITY_ALS IMPLICITPREFS_ALS

    OPTIONS="--numUsers $NUM_USERS_ALS \
            --numProducts $NUM_PRODUCTS_ALS
            --implicitPrefs $IMPLICITPREFS_ALS\
            "

    sparkbenchjar --dataPath $TMPINPUTDIR $OPTIONS
}

run() {
    remove_tmp
    prepare
}

#run
alsrun