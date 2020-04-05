source $FUNCTIONSRC
setenv

WORDT=wordtable

randomtext() {
  read -r SIZE MAPS <<< `getconfvar genword.size genword.maps`
  required_listofpars SIZE MAPS
  log_listofpars SIZE MAPS
  remove_tmp

  local -r BEGTEST=`testbeg randomtext`
  yarn_job_examples randomtextwriter  -D mapreduce.randomtextwriter.totalbytes=$SIZE -D mapreduce.randomtextwriter.bytespermap=$(( $SIZE / $MAPS )) $TMPINPUTDIR
  testend $BEGTEST
}

wordcountmapreduce() {
  read -r MAPS REDUCES <<< `getconfvar wordcount.maps wordcount.reduces`
  required_listofpars MAPS REDUCES
  log_listofpars MAPS REDUCES

  local -r BEGTEST=`testbeg wordcountmapreduce`
  yarn_job_examples wordcount -D mapreduce.job.maps=$MAPS -D mapreduce.job.reduces=$REDUCES ${TMPINPUTDIR} $TMPOUTPUTDIR
  testend $BEGTEST
}

runhivewordcount() {
  local -r BEGTEST=`testbeg hivewordcount`
  hivesql "DROP TABLE IF EXISTS $WORDT"
  hivesql "CREATE EXTERNAL TABLE $WORDT (line string) STORED AS SEQUENCEFILE LOCATION '${TMPINPUTDIR}'"
  hivesql "with xx as (select explode(split(line,' ')) as word from $WORDT) select word,count(*) from xx group by word"
  testend $BEGTEST
}

runpigwordcount() {
  local -r TMP=`crtemp`
  local -r BEGTEST=`testbeg pigwordcount`

  cat << EOF | cat >$TMP
input_lines = LOAD '${TMPINPUTDIR}' AS (line:chararray);
words = FOREACH input_lines GENERATE FLATTEN(TOKENIZE(line)) AS word;
filtered_words = FILTER words BY word MATCHES '\\\\w+';
word_groups = GROUP filtered_words BY word;
word_count = FOREACH word_groups GENERATE COUNT(filtered_words) AS count, group AS word;
-- necessary to evaluate lazy
dump word_count;
EOF

  pigscript $TMP
  testend $BEGTEST

}

sparkwordcount() {
  local -r TMP=`crtemp`
  remove_tmpoutput
  local -r BEGTEST=`testbeg sparkwordcount`
  read -r EXECORES DRVCORES DRVMEMORY NUMEXE <<< `getconfvar spark.executor.cores spark.driver.cores spark.driver.memory spark.num.executors`
  required_listofpars EXECORES DRVCORES DRVMEMORY NUMEXE
  log_listofpars EXECORES DRVCORES DRVMEMORY NUMEXE


  cat << EOF | cat >$TMP

val infile="$TMPINPUTDIR"
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable

val f=sc.sequenceFile(infile, classOf[Text],classOf[Text])
val a = f.flatMap{case (x, y) => (x.toString.split(' ').union(y.toString.split(' '))).map(word=>(word,1))}
val wc =  a.reduceByKey(_+_)
wc.saveAsTextFile("$TMPOUTPUTDIR")

EOF

  sparkshell $TMP --master yarn --executor-cores $EXECORES --driver-cores $DRVCORES --driver-memory $DRVMEMORY --num-executors $NUMEXE
  testend $BEGTEST
}

sparksqlwordcount() {

  local -r TMP=`crtemp`
  rmr_hdfs $TMPOUTPUTDIR
  local -r BEGTEST=`testbeg sparksqlwordcount`
  read -r EXECORES DRVCORES DRVMEMORY NUMEXE <<< `getconfvar sparksql.executor.cores sparksql.driver.cores sparksql.driver.memory sparksql.num.executors`
  required_listofpars EXECORES DRVCORES DRVMEMORY NUMEXE
  log_listofpars EXECORES DRVCORES DRVMEMORY NUMEXE

  cat << EOF | cat >$TMP

DROP TABLE IF EXISTS $WORDT;
CREATE EXTERNAL TABLE $WORDT (line string) STORED AS SEQUENCEFILE LOCATION '${TMPINPUTDIR}';
with xx as (select explode(split(line,' ')) as word from $WORDT) select word,count(*) from xx group by word;

EOF

  sparksql $TMP --master yarn --executor-cores $EXECORES --driver-cores $DRVCORES --driver-memory $DRVMEMORY --num-executors $NUMEXE
  testend $BEGTEST
}

run() {

  randomtext

  wordcountmapreduce
  runhivewordcount
  runpigwordcount
  sparkwordcount
  sparksqlwordcount
}

test() {
  randomtext
  wordcountmapreduce
  runhivewordcount
}

run

exit 0
