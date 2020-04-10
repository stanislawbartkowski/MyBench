source $FUNCTIONSRC
setenv

prepare() {
    spark_prepare SVMDataGenerator
}

runsvm() {
    local -r BEGTEST=`testbeg runsvm`

    read -r NUM_ITERATIONS_SVM STEPSIZE_SVM REGPARAM_SVM <<< `getconfvar numIterations stepSize regParam`
    required_listofpars NUM_ITERATIONS_SVM STEPSIZE_SVM REGPARAM_SVM
    log_listofpars NUM_ITERATIONS_SVM STEPSIZE_SVM REGPARAM_SVM
        
    OPTIONS="--numIterations $NUM_ITERATIONS_SVM \
             --stepSize $STEPSIZE_SVM \
             --regParam $REGPARAM_SVM \
             --dataPath $TMPINPUTDIR
            "

    sparkbenchjar SVMWithSGDExample $OPTIONS
    testend $BEGTEST
}

run() {
    remove_tmp
    prepare
    runsvm
}

test() {
#    remove_tmp
#    prepare
    runsvm
}

run
#test