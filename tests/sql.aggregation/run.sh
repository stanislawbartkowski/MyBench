source $FUNCTIONSRC
setenv

prepare() {
    local -r BEGTEST=`testbeg prepare`

    HIVE_BASE_HDFS=$TMPBASEDIR/hive
    HIVE_INPUT=$TMPINPUTDIR
    PAGES=120
    USERVISITS=1000

    OPTIONS="hive \
        -b ${HIVE_BASE_HDFS} \
        -n $HIVE_INPUT \
        -p $PAGES \
        -v $USERVISITS \
        -o sequence"

 hadoopbenchjar $OPTIONS

 testend $BEGTEST    
}

runrf() {
    local -r BEGTEST=`testbeg runaggregate`



    testend $BEGTEST
}

run() {
    remove_tmp
    prepare
    runrf
}

test() {
    remove_tmp
    prepare
#    runrf
}

#run
test