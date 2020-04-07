source $FUNCTIONSRC
setenv

prepare() {
    local -r BEGTEST=`testbeg prepare`
    read -r NUM_DOCUMENTS_LDA NUM_VOCABULARY_LDA DOC_LEN_MIN_LDA DOC_LEN_MAX_LDA <<< `getconfvar num_of_documents num_of_vocabulary doc_len_min  doc_len_max`
    required_listofpars NUM_DOCUMENTS_LDA NUM_VOCABULARY_LDA DOC_LEN_MIN_LDA DOC_LEN_MAX_LDA
    log_listofpars NUM_DOCUMENTS_LDA NUM_VOCABULARY_LDA DOC_LEN_MIN_LDA DOC_LEN_MAX_LDA

    OPTIONS="--numDocs $NUM_DOCUMENTS_LDA \
             --numVocab $NUM_VOCABULARY_LDA \
             --docLenMin $DOC_LEN_MIN_LDA \
             --docLenMax $DOC_LEN_MAX_LDA \
             --dataPath $TMPINPUTDIR
            "
    sparkbenchjar LDADataGenerator $OPTIONS

    testend $BEGTEST
}

runlda() {
    remove_tmpoutput
    local -r BEGTEST=`testbeg runlda`
    read -r NUM_TOPICS_LDA NUM_ITERATIONS_LDA OPTIMIZER_LDA MAXRESULTSIZE_LDA <<< `getconfvar num_of_topics num_iterations optimizer maxresultsize`
    required_listofpars 
    log_listofpars 
    OPTIONS="--numTopics $NUM_TOPICS_LDA \
             --maxIterations $NUM_ITERATIONS_LDA \
             --optimizer $OPTIMIZER_LDA \
             --maxResultSize $MAXRESULTSIZE_LDA \
             --dataPath $TMPINPUTDIR \
             --outputPath $TMPOUTPUTDIR
            "

    sparkbenchjar LDAExample $OPTIONS

    testend $BEGTEST
}

run() {
    remove_tmp
    prepare    
    runlda
}

test() {
    runlda
}

run
#test