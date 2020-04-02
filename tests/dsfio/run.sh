source $FUNCTIONSRC
setenv


xrundfsio() {
    local -r testname=$1
    shift
    local -r BEGTEST=`testbeg $testname`
    read -r RD_NUM_OF_FILES RD_FILE_SIZE <<< `getconfvar read.number_of_files read.file_size`
    required_listofpars RD_NUM_OF_FILES RD_FILE_SIZE
    log_listofpars RD_NUM_OF_FILES RD_FILE_SIZE

    mapredtest TestDFSIO \
        -Dmapreduce.map.java.opts=\"-Dtest.build.data=$TMPINPUTDIR\" \
        -Dmapreduce.reduce.java.opts=\"-Dtest.build.data=$TMPINPUTDIR\" \
        -Dtest.build.data=$TMPINPUTDIR \
        -nrFiles ${RD_NUM_OF_FILES} -fileSize ${RD_FILE_SIZE}\
        $@

    testend $BEGTEST
}

rundfsio() {
    local -r testname=$1
    shift
    local -r BEGTEST=`testbeg $testname`
    read -r RD_NUM_OF_FILES RD_FILE_SIZE <<< `getconfvar read.number_of_files read.file_size`
    required_listofpars RD_NUM_OF_FILES RD_FILE_SIZE
    log_listofpars RD_NUM_OF_FILES RD_FILE_SIZE

    mapredtest TestDFSIO \
        -Dtest.build.data=$TMPINPUTDIR \
        -nrFiles ${RD_NUM_OF_FILES} -fileSize ${RD_FILE_SIZE} \
        $@

    testend $BEGTEST
}


writedsfio() {
    rundfsio writedsfio -write -bufferSize 4096
}

readdsfio() {
    rundfsio readdsfio -read -bufferSize 4096 
}

appenddsfio() {
    rundfsio appenddsfio -append -bufferSize 4096
}

readappenddsfio() {
    rundfsio readappenddsfio -read -bufferSize 4096 
}


xwritedsfio() {
    local -r BEGTEST=`testbeg writedsfio`
    read -r RD_NUM_OF_FILES RD_FILE_SIZE <<< `getconfvar read.number_of_files read.file_size`
    required_listofpars RD_NUM_OF_FILES RD_FILE_SIZE
    log_listofpars RD_NUM_OF_FILES RD_FILE_SIZE

    mapredtest TestDFSIO \
        -Dmapreduce.map.java.opts=\"-Dtest.build.data=$TMPINPUTDIR\" \
        -Dmapreduce.reduce.java.opts=\"-Dtest.build.data=$TMPINPUTDIR\" \
        -Dtest.build.data=$TMPINPUTDIR \
        -write -nrFiles ${RD_NUM_OF_FILES} -fileSize ${RD_FILE_SIZE} -bufferSize 4096

    testend $BEGTEST
}

xreaddsfio() {
    local -r BEGTEST=`testbeg readdsfio`
    read -r RD_NUM_OF_FILES RD_FILE_SIZE <<< `getconfvar read.number_of_files read.file_size`
    required_listofpars RD_NUM_OF_FILES RD_FILE_SIZE
    log_listofpars RD_NUM_OF_FILES RD_FILE_SIZE

    remove_tmpoutput1

    mapredtest TestDFSIO \
        -Dmapreduce.map.java.opts=\"-Dtest.build.data=$TMPINPUTDIR\" \
        -Dmapreduce.reduce.java.opts=\"-Dtest.build.data=$TMPINPUTDIR\" \
        -Dtest.build.data=$TMPINPUTDIR \
        -read -nrFiles ${RD_NUM_OF_FILES} -fileSize ${RD_FILE_SIZE} -bufferSize 131072 \
        -sampleInteval 200 -sumThreshold 0.5 -tputReportTotal -Dtest.build.data=$TMPINPUTDIR \
        -resFile $TMPOUTPUT1DIR/result_read.txt \
        -tputFile $TMPOUTPUT1DIR/throughput_read.csv 

    testend $BEGTEST
}

run() {
    remove_tmp
    writedsfio
    readdsfio
    appenddsfio
    readappenddsfio
}

run
#readdsfio
#appenddsfio