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
    sparkbenchjar LogisticRegressionDataGenerator $OPTIONS
    testend $BEGTEST
}

runlogistic() {
    local -r BEGTEST=`testbeg runlogistic`

    OPTIONS="--dataPath $TMPINPUTDIR"

    sparkbenchjar LogisticRegression $OPTIONS
    testend $BEGTEST
}

run() {
    remove_tmp
    prepare
    runlogistic
}

test() {
    runlogistic
}

run
#test