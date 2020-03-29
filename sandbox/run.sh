source $FUNCTIONSRC
#source $ENVRC
setenv

run() {
  rmr_hdfs $TMPOUTPUTDIR
  read -r SIZE HELLO FOO <<< `getconfvar genword.size hello`
  required_listofpars SIZE HELLO
  echo "S=",$SIZE
  echo "H=",$HELLO
}

run