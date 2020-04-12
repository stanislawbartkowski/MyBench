source $FUNCTIONSRC
setenv

old_copy() {
    MODEL_INPUT=$TMPBASEDIR/nweight-user-features

   mkdir_hdfs $TMPBASEDIR
   copy_tohdfs resources/nweight-user-features $MODEL_INPUT
}

prepare() {
    local -r BEGTEST=`testbeg prepare`
    read -r EDGES <<<`getconfvar edges`

    verify_pars EDGES

    MODEL_INPUT=$PWD/resources/nweight-user-features

    OPTIONS="--modelPath $MODEL_INPUT \
             --totalNumRecords $EDGES \
             --dataPath $TMPINPUTDIR
            "
    sparkbenchjar NWeightDataGenerator $OPTIONS
    testend $BEGTEST
}

rungraph() {

    local -r BEGTEST=`testbeg rungraph`
    remove_tmpoutput
    read -r DEGREE MAX_OUT_EDGES STORAGE_LEVEL KRYO MODEL <<<`getconfvar degree max_out_edges storage_level disable_kryo model`
    verify_pars DEGREE MAX_OUT_EDGES STORAGE_LEVEL KRYO MODEL


    INPUT_HDFS=$TMPINPUTDIR
    OUTPUT_HDFS=$TMPOUTPUTDIR 
#    DEGREE=3
#    MAX_OUT_EDGES=30
 #   STORAGE_LEVEL=7
 #   DISABLE_KRYO=
  #  MODEL=graphx

    [ $KRYO = "true" ] && DISABLE_KRYO="--disableKryo"

    OPTIONS="--output $OUTPUT_HDFS \
              --step $DEGREE \
              --maxDegree $MAX_OUT_EDGES \
              --storageLevel $STORAGE_LEVEL  $DISABLE_KRYO \
              --model $MODEL \
              --dataPath $INPUT_HDFS
            "
    sparkbenchjar NWeight  $OPTIONS
    testend $BEGTEST
}

test() {
#    remove_tmp
#    prepare
    rungraph
}

run() {
    remove_tmp
    prepare
    rungraph
}

#test
run