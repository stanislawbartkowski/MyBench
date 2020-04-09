source $FUNCTIONSRC
setenv

old_prepare() {
    local -r BEGTEST=`testbeg prepare`
    read -r NUM_EXAMPLES_GBT NUM_FEATURES_GBT <<< `getconfvar examples features`
    required_listofpars NUM_EXAMPLES_GBT NUM_FEATURES_GBT
    log_listofpars NUM_EXAMPLES_GBT NUM_FEATURES_GBT

    sparkbenchjar GradientBoostedTreeDataGenerator  --dataPath $TMPINPUTDIR --numExamples $NUM_EXAMPLES_GBT --numFeatures $NUM_FEATURES_GBT
    testend $BEGTEST
}

prepare() {
   spark_prepare GradientBoostedTreeDataGenerator
}

rungbt() {
    read -r NUM_CLASSES_GBT MAX_DEPTH_GBT MAX_BINS_GBT NUM_ITERATIONS_GBT LEARNING_RATE_GBT <<<`getconfvar numClasses maxDepth maxBins numIterations learningRate`

    required_listofpars NUM_CLASSES_GBT MAX_DEPTH_GBT MAX_BINS_GBT NUM_ITERATIONS_GBT LEARNING_RATE_GBT
    log_listofpars NUM_CLASSES_GBT MAX_DEPTH_GBT MAX_BINS_GBT NUM_ITERATIONS_GBT LEARNING_RATE_GBT

    INPUT_HDFS=$TMPINPUTDIR
    local -r BEGTEST=`testbeg rungbt`


    OPTIONS="--numClasses $NUM_CLASSES_GBT \
              --maxDepth $MAX_DEPTH_GBT \
              --maxBins $MAX_BINS_GBT \
              --numIterations $NUM_ITERATIONS_GBT \
              --learningRate $LEARNING_RATE_GBT \
              --dataPath $INPUT_HDFS
            "
    sparkbenchjar GradientBoostedTree $OPTIONS
    testend $BEGTEST
}

test() {
#    remove_tmp
#    prepare
    rungbt
}

run() {
    remove_tmp
    prepare
    rungbt
}

#test
run