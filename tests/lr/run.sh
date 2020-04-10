source $FUNCTIONSRC
setenv

prepare() {
    spark_prepare LogisticRegressionDataGenerator
}

runlogistic() {
    spark_run runpca LogisticRegression
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