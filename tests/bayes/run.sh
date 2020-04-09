source $FUNCTIONSRC
setenv

prepare() {
    local -r BEGTEST=`testbeg prepare`

    local -r BAYES_BASE_HDFS=$TMPBASEDIR
    read -r PAGES CLASSES <<< `getconfvar pages classes`
    required_listofpars PAGES CLASSES
    log_listofpars PAGES CLASSES

    OPTIONS="bayes \
        -b ${BAYES_BASE_HDFS} \
        -n $INPUTDIR \
        -p ${PAGES} \
        -class ${CLASSES} \
        -o sequence"

 hadoopbenchjar $OPTIONS

 testend $BEGTEST
}

ran_bayes() {
    spark_run mlbayes SparseNaiveBayes
}

run() {
    remove_tmp
    prepare
    ran_bayes
}

test() {
    remove_tmp
    prepare
    ran_bayes

}

run
#test