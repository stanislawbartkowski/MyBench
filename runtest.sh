BASEDIR=`dirname $0`
BASEDIR=`realpath $BASEDIR`
export FUNCTIONSRC=$BASEDIR/functions/functions.rc
export ENVRC=$BASEDIR/conf/test.rc
export CUSTOMRC=$BASEDIR/conf/custom.rc

source $FUNCTIONSRC
setenv

DIRTEST=tests
IGNORE=ignore
DISABLED=disabled
CLEAN=clean
SANDBOX=sandbox

export TMPSTORE

setuplogging() {
    REPORTDIR=$PWD/reports
    mkdir -p $REPORTDIR
    export REPORTFILE=$REPORTDIR/$BENCHSIZE.result

    # temporary files
    declare -g TMPSTORE=`mktemp`
}

# remove all tempoary files, should be called in the end
removetemp() {
  while read rmfile;  do rm $rmfile; done <$TMPSTORE
  rm $TMPSTORE
}

# ====================
# verify environment
# ====================

ignoretest() {
    local -r DIRT=$DIRTEST/$1
    [ -f $DIRT/$IGNORE ] && return 0
    return 1
}

disabledtest() {
    local -r DIRT=$DIRTEST/$1
    for w in ${LISTSIZE//,/ }; do
        [ -f  $DIRT/${DISABLED}_$w ] && return 0
        [ $w == $BENCHSIZE ] &&  return 1
    done
    return 1
}


verifyenv() {
    log "Check environment"
    required_listofvars TESTLIST SANDBOX DIRTEST TMPOUTPUTDIR LISTSIZE BENCHSIZE TMPINPUTDIR

    required_listofvars HADOOPEXAMPLES JUNITJAR HADOOPMAPREDUCETEST BENCHMARKJAR TMPBASEDIR
    required_listofvars PHOENIXDIR ZOOKEEPER
    # check if SIZE on the list
    onthelist $BENCHSIZE $LISTSIZE 

    existfile conf/test.rc
    for dir in ${TESTLIST//,/ }; do
        DIRT=$DIRTEST/$dir
        existdir $DIRT
        existfile $DIRT/run.sh
        existfile $DIRT/$ENVCONF
        existexefile $DIRT/run.sh
        if ignoretest $dir ; then log "$DIRT/$IGNORE exist, $dir test not executed"; fi
        if disabledtest $dir; then log "$DIRT/${DISABLED}_size exist, $dir test not executed for $BENCHSIZE"; fi
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
    export TESTNAME=$1
    export PAR=$2    
    log "Execute $TESTNAME $PAR"
    cd $SANDBOX
    if ./run.sh $PAR; then 
        log "$TESTNAME $PAR passed"
    else     
        logfail "$TESTNAME $PAR failed"
        markfailedtest FAILED
    fi
    cd $BASEDIR
}

rundisabledtest() {
    export TESTNAME=$1
    log "Disabled $TESTNAME"
    markfailedtest FAILED
}

runtests() {
    log "Run tests, size $BENCHSIZE"
    for test in ${TESTLIST//,/ }; do
        if ignoretest $test; then continue; fi
        if disabledtest $test; then 
           rundisabledtest $test
           continue
        fi
        preparesandbox $test
        runsingletest $test
    done
}

go() {

    verifyenv
    setuplogging

    runtests

    removetemp
}

help() {
    echo "./runtest.sh clean "
    echo "   cleanup all HDFS data in TESTLIST"
    exit 1
}

cleanproc() {
    remove_tmp

    log "Run cleanup procedure across tests"
    for test in ${TESTLIST//,/ }; do
        if  ignoretest $test; then contine; fi
        if  disabled $test; then continue; fi
        if [ -f $DIRTEST/$test/$CLEAN ]; then
            preparesandbox $test
            runsingletest $test cleanup
        fi
    done

}

cleandata() {
    log "Clean HDFS data"
    verifyenv
    setuplogging

    cleanproc

    removetemp
}

case $1 in
  cleanup) cleandata;;
  "") go ;;
  *) help;;
esac
