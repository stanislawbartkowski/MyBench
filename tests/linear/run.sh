source $FUNCTIONSRC
setenv

prepare() {
   spark_prepare LinearRegressionDataGenerator
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