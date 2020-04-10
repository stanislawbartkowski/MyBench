source $FUNCTIONSRC
setenv

prepare() {
    spark_prepare SVDDataGenerator
}

runsvd() {
    local -r BEGTEST=`testbeg runsvd`

    read -r NUM_FEATURES_SVD NUM_SINGULAR_VALUES_SVD COMPUTEU_SVD MAXRESULTSIZE_SVD <<< `getconfvar features singularvalues computeU maxresultsize`
    required_listofpars NUM_FEATURES_SVD NUM_SINGULAR_VALUES_SVD COMPUTEU_SVD MAXRESULTSIZE_SVD
    log_listofpars NUM_FEATURES_SVD NUM_SINGULAR_VALUES_SVD COMPUTEU_SVD MAXRESULTSIZE_SVD

    [ $COMPUTEU_SVD == "true" ] && COMPUTEU="--computeU"
        
    OPTIONS="--numFeatures $NUM_FEATURES_SVD \
             --numSingularValues $NUM_SINGULAR_VALUES_SVD $COMPUTEU \
             --maxResultSize $MAXRESULTSIZE_SVD \
             --dataPath $TMPINPUTDIR
            "

    sparkbenchjar SVDExample $OPTIONS
    testend $BEGTEST
}

run() {
    remove_tmp
    prepare
    runsvd
}

test() {
#    remove_tmp
#    prepare
    runsvd
}

run
#test