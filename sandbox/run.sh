source $FUNCTIONSRC
setenv

randomtext() {
  read -r SIZE MAPS <<< `getconfvar genword.size genword.maps`
  required_listofpars SIZE MAPS
  log_listofpars SIZE MAPS
  yarn_job_examples randomtextwriter  -D mapreduce.randomtextwriter.totalbytes=$SIZE -D mapreduce.randomtextwriter.bytespermap=$(( $SIZE / $MAPS )) $TMPINPUTDIR
}

wordcountmapreduce() {
  read -r MAPS REDUCES <<< `getconfvar wordcount.maps wordcount.reduces`
  log_listofpars MAPS REDUCES
  yarn_job_examples wordcount -D mapreduce.job.maps=$MAPS -D mapreduce.job.reduces=$REDUCES ${TMPINPUTDIR} $TMPOUTPUTDIR
}

run() {
  rmr_hdfs $TMPOUTPUTDIR
  rmr_hdfs $TMPINPUTDIR

  randomtext

  wordcountmapreduce
}

run
exit 0