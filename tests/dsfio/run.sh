source $FUNCTIONSRC
setenv

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