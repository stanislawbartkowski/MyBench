
BASEDIR=`dirname $0`
BASEDIR=`realpath $BASEDIR`
export FUNCTIONSRC=$BASEDIR/functions/functions.rc
export ENVRC=$BASEDIR/conf/test.rc

source $FUNCTIONSRC
setenv

DIRTEST=tests
DISABLE=disable
SANDBOX=sandbox


# ====================
# verify environment
# ====================

verifyenv() {
    log "Check environment"
    required_listofvars TESTLIST SANDBOX DIRTEST TMPOUTPUTDIR LISTSIZE BENCHSIZE TMPINPUTDIR
    required_listofvars HADOOPEXAMPLES
    # check if SIZE on the list
    onthelist $BENCHSIZE $LISTSIZE

    existfile conf/test.rc
    for dir in ${TESTLIST//,/ }; do
        DIRT=$DIRTEST/$dir
        existdir $DIRT
        existfile $DIRT/run.sh
        existfile $DIRT/$ENVCONF
        existexefile $DIRT/run.sh
        [ -f $DIRT/$DISABLE ] && log "$DIRT/$DISABLE exist, $dir test not executed"
    done
    log "Environment ok"
}

# ========================
# execute 
# ========================

preparesandbox() {
    local -r test=$1
    log "Prepare $SANDBOX for $test"
    rm -rf $SANDBOX
    mkdir -p $SANDBOX
    cp -r $DIRTEST/$test/* $SANDBOX
}

runsingletest() {
    local -r test=$1
    log "Execute $test"
    cd $SANDBOX
    if ./run.sh; then 
        log "$test passed"
    else     
        logfail "$test failed"
    fi
    cd $BASEDIR
}

runtests() {
    log "Run tests, size $SIZE"
    for test in ${TESTLIST//,/ }; do
        [ -f $DIRTEST/$test/$DISABLE ] && continue
        preparesandbox $test
        runsingletest $test
    done
}

verifyenv
runtests
