source $FUNCTIONSRC
setenv

prepare() {

  local -r BEGTEST=`testbeg prepare`

  read -r NUM_USERS_ALS NUM_PRODUCTS_ALS SPARSITY_ALS IMPLICITPREFS_ALS <<< `getconfvar als.users als.products als.sparsity als.implicitprefs`
  required_listofpars NUM_USERS_ALS NUM_PRODUCTS_ALS SPARSITY_ALS IMPLICITPREFS_ALS
  log_listofpars NUM_USERS_ALS NUM_PRODUCTS_ALS SPARSITY_ALS IMPLICITPREFS_ALS
  [ $IMPLICITPREFS_ALS == "true" ] && IMPLICIT="--implicitPrefs"

  OPTIONS="--numUsers $NUM_USERS_ALS \
           --numProducts $NUM_PRODUCTS_ALS $IMPLICIT \
           --sparsity $SPARSITY_ALS
          "

  sparkbenchjar RatingDataGenerator --dataPath $TMPINPUTDIR $OPTIONS

  testend $BEGTEST
}


alsrun() {
    local -r BEGTEST=`testbeg alsrun`

    read -r NUM_USERS_ALS NUM_PRODUCTS_ALS SPARSITY_ALS IMPLICITPREFS_ALS <<< `getconfvar als.users als.products als.sparsity als.implicitprefs`
    required_listofpars NUM_USERS_ALS NUM_PRODUCTS_ALS SPARSITY_ALS IMPLICITPREFS_ALS
    log_listofpars NUM_USERS_ALS NUM_PRODUCTS_ALS SPARSITY_ALS IMPLICITPREFS_ALS

    read -r RANK_ALS NUM_RECOMMENDS_ALS NUM_ITERATIONS_ALS KYRO_ALS <<< `getconfvar als.rank als.recommends als.numIterations als.kyro`
    required_listofpars RANK_ALS NUM_RECOMMENDS_ALS NUM_ITERATIONS_ALS KYRO_ALS
    log_listofpars RANK_ALS NUM_RECOMMENDS_ALS NUM_ITERATIONS_ALS KYRO_ALS

    read -r PRODUCTBLOCKS_ALS USERBLOCKS_ALS LAMBDA_ALS <<< `getconfvar als.numUserBlocks als.numProductBlocks als.Lambda`
    required_listofpars PRODUCTBLOCKS_ALS USERBLOCKS_ALS LAMBDA_ALS
    log_listofpars PRODUCTBLOCKS_ALS USERBLOCKS_ALS LAMBDA_ALS


    [ $IMPLICITPREFS_ALS == "true" ] && IMPLICIT="--implicitPrefs"
    [ $KYRO_ALS == "true" ] && KRYO="--kryo"

    OPTIONS="--numUsers $NUM_USERS_ALS \
            --numProducts $NUM_PRODUCTS_ALS $IMPLICIT $KRYO\
            --rank $RANK_ALS \
            --numRecommends $NUM_RECOMMENDS_ALS \
            --numIterations $NUM_ITERATIONS_ALS \
            --numProductBlocks $PRODUCTBLOCKS_ALS \
            --numUserBlocks $USERBLOCKS_ALS \
            --lambda $LAMBDA_ALS
            "

    sparkbenchjar AvlExample --dataPath $TMPINPUTDIR $OPTIONS

    testend $BEGTEST
}

run() {
    remove_tmp
    prepare
    alsrun
}

test() {
#    remove_tmp
#    prepare
    alsrun
}

run
#test
