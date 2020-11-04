source $FUNCTIONSRC
setenv

WORDT=wordtable

randomtext() {
  read -r SIZE MAPS <<< `getconfvar genword.size genword.maps`
  verify_pars SIZE MAPS
  remove_tmp

  local -r BEGTEST=`testbeg randomtext`
  yarn_job_examples randomtextwriter  -D mapreduce.randomtextwriter.totalbytes=$SIZE -D mapreduce.randomtextwriter.bytespermap=$(( $SIZE / $MAPS )) $TMPINPUTDIR
  testend $BEGTEST
}

wordcountmapreduce() {
  read -r MAPS REDUCES <<< `getconfvar wordcount.maps wordcount.reduces`
  verify_pars MAPS REDUCES
  
  local -r BEGTEST=`testbeg wordcountmapreduce`
  yarn_job_examples wordcount -D mapreduce.job.maps=$MAPS -D mapreduce.job.reduces=$REDUCES ${TMPINPUTDIR} $TMPOUTPUTDIR
  testend $BEGTEST
}

runhivewordcount() {
  local -r BEGTEST=`testbeg hivewordcount`
  hivesql "DROP TABLE IF EXISTS $WORDT"
  hivesql "CREATE EXTERNAL TABLE $WORDT (line string) STORED AS SEQUENCEFILE LOCATION '${TMPINPUTDIR}'"
  hive_verifynonzero $WORDT

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

removeit_sparkwordcount() {
  local -r TMP=`crtemp`
  remove_tmpoutput
  local -r BEGTEST=`testbeg sparkwordcount`

  cat << EOF | cat >$TMP

val infile="$TMPINPUTDIR"
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable

val f=sc.sequenceFile(infile, classOf[Text],classOf[Text])
val a = f.flatMap{case (x, y) => (x.toString.split(' ').union(y.toString.split(' '))).map(word=>(word,1))}
val wc =  a.reduceByKey(_+_)
wc.saveAsTextFile("$TMPOUTPUTDIR")

EOF

  sparkshell $TMP
  testend $BEGTEST
}

sparkwordcount() {
  local -r TMP=`crtemp`
  remove_tmpoutput
  local -r BEGTEST=`testbeg sparkwordcount`

  OPTIONS="--outputPath $TMPOUTPUTDIR \
             --dataPath $TMPINPUTDIR
            "

  sparkbenchjar HdfsWordCount  $OPTIONS

  testend $BEGTEST
}


sparksqlwordcount() {

  local -r TMP=`crtemp`
  rmr_hdfs $TMPOUTPUTDIR
  local -r BEGTEST=`testbeg sparksqlwordcount`

  cat << EOF | cat >$TMP

DROP TABLE IF EXISTS $WORDT;
CREATE EXTERNAL TABLE $WORDT (line string) STORED AS SEQUENCEFILE LOCATION '${TMPINPUTDIR}';
with xx as (select explode(split(line,' ')) as word from $WORDT) select word,count(*) from xx group by word;

EOF

  sparksql $TMP 
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
#  randomtext
#  wordcountmapreduce
#  runhivewordcount
  sparksqlwordcount
#  sparkwordcount
#  sparksqlwordcount
}

cleanup() {
  log "Remove spark and hive $WORDT table"
  sparksqlremovetable $WORDT
  hivesqlremovetable $WORDT
}

case $1 in 
  cleanup) cleanup;; 
  *) 
    run;;
#    test;;
esac

#test

exit 0
