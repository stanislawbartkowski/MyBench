source $FUNCTIONSRC
setenv

prepare() {
    spark_prepare PCADataGenerator
}

runpca() {
    local -r BEGTEST=`testbeg runpca`
    read -r MAXRESULTSIZE_PCA <<< `getconfvar maxresultsize`
    required_listofpars MAXRESULTSIZE_PCA
    log_listofpars MAXRESULTSIZE_PCA

    OPTIONS="--dataPath $TMPINPUTDIR \
             --maxResultSize $MAXRESULTSIZE_PCA
            "

    sparkbenchjar PCAExample $OPTIONS
    testend $BEGTEST
}

run() {
    remove_tmp
    prepare
    runpca
}

test() {
#    remove_tmp
#    prepare
    runpca
}

run
#test