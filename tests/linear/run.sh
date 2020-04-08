source $FUNCTIONSRC
setenv

prepare() {
    local -r BEGTEST=`testbeg prepare`
    read -r NUM_EXAMPLES_LINEAR NUM_FEATURES_LINEAR <<< `getconfvar examples features`
    required_listofpars NUM_EXAMPLES_LINEAR NUM_FEATURES_LINEAR
    log_listofpars NUM_EXAMPLES_LINEAR NUM_FEATURES_LINEAR


    OPTIONS="--numExamples $NUM_EXAMPLES_LINEAR \
             --numFeatures $NUM_FEATURES_LINEAR \
             --dataPath $TMPINPUTDIR
            "
    sparkbenchjar LinearRegressionDataGenerator $OPTIONS
    testend $BEGTEST
}

runlinear() {
    local -r BEGTEST=`testbeg runlinear`
    read -r NUM_ITERATIONS_LINEAR STEPSIZE_LINEAR <<< `getconfvar numIterations stepSize`
    required_listofpars NUM_ITERATIONS_LINEAR STEPSIZE_LINEAR
    log_listofpars NUM_ITERATIONS_LINEAR STEPSIZE_LINEAR

    OPTIONS="--numIterations $NUM_ITERATIONS_LINEAR \
             --stepSize $STEPSIZE_LINEAR  \
             --dataPath $TMPINPUTDIR
            "

    sparkbenchjar LinearRegression $OPTIONS
    testend $BEGTEST
}

run() {
    remove_tmp
    prepare
    runlinear
}

test() {
    runlinear
}

run
#test