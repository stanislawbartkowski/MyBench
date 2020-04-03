source $FUNCTIONSRC
setenv

prepare() {
  local -r BEGTEST=`testbeg teragen`
  read -r SIZE MAPS REDUCES <<< `getconfvar datasize teragen.num.maps teragen.num.reduces`
  required_listofpars SIZE MAPS REDUCES

  log_listofpars SIZE MAPS REDUCES 

  yarn_job_examples teragen -D mapreduce.job.maps=${MAPS} -D mapreduce.job.reduces=${REDUCES} ${SIZE} $TMPINPUTDIR

  testend $BEGTEST
}

terasort() {
  local -r BEGTEST=`testbeg terasort`
  read -r REDUCES <<< `getconfvar terasort.num.reduces`
  required_listofpars REDUCES

  log_listofpars REDUCES
  remove_tmpoutput
  remove_tmpoutput1
  yarn_job_examples terasort -D mapreduce.job.reduces=$REDUCES $TMPINPUTDIR $TMPOUTPUTDIR
  yarn_job_examples teravalidate $TMPOUTPUTDIR $TMPOUTPUT1DIR

  testend $BEGTEST
}


run() {
  remove_tmp
  prepare
  terasort
}

run
#terasort