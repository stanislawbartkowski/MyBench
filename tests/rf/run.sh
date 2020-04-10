source $FUNCTIONSRC
setenv

prepare() {
    spark_prepare RandomForestDataGenerator 
}

runrf() {
    local -r BEGTEST=`testbeg randomforest`

    read -r NUM_TREES_RF NUM_CLASSES_RF FEATURE_SUBSET_STRATEGY_RF IMPURITY_RF MAX_DEPTH_RF MAX_BINS_RF <<< `getconfvar numTrees numClasses featureSubsetStrategy impurity maxDepth maxBins`
    required_listofpars NUM_TREES_RF NUM_CLASSES_RF FEATURE_SUBSET_STRATEGY_RF IMPURITY_RF MAX_DEPTH_RF MAX_BINS_RF
    log_listofpars NUM_TREES_RF NUM_CLASSES_RF FEATURE_SUBSET_STRATEGY_RF IMPURITY_RF MAX_DEPTH_RF MAX_BINS_RF

    OPTIONS="--numTrees $NUM_TREES_RF \
        --numClasses $NUM_CLASSES_RF \
        --featureSubsetStrategy $FEATURE_SUBSET_STRATEGY_RF \
        --impurity $IMPURITY_RF \
        --maxDepth $MAX_DEPTH_RF \
        --maxBins $MAX_BINS_RF \
        --dataPath $TMPINPUTDIR"

    sparkbenchjar RandomForestClassification $OPTIONS
    testend $BEGTEST
}

run() {
    remove_tmp
    prepare
    runrf
}

test() {
#    remove_tmp
#    prepare
    runrf
}

run
#test