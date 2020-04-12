source $FUNCTIONSRC
setenv

runsqlscan() {
    local -r begname=$1
    local -r command=$2
    local -r verify=$3

    local -r BEGTEST=`testbeg $begname`

    INPUT_HDFS=$TMPINPUTDIR
    OUTPUT_HDFS=$TMPOUTPUTDIR
    remove_tmpoutput

    local -r TMP=`crtemp`

cat <<EOF > ${TMP}
USE DEFAULT;

DROP TABLE IF EXISTS uservisits;
CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '$INPUT_HDFS/uservisits';
DROP TABLE IF EXISTS uservisits_copy;
CREATE TABLE uservisits_copy (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '$OUTPUT_HDFS/uservisits_copy' $NOTRANSACTIONAL;
INSERT OVERWRITE TABLE uservisits_copy SELECT * FROM uservisits;
EOF
    $command $TMP
    $verify uservisits_aggre

    testend $BEGTEST

}

runsqljoin() {

    local -r begname=$1
    local -r command=$2
    local -r verify=$3

    local -r BEGTEST=`testbeg $begname`

    INPUT_HDFS=$TMPINPUTDIR
    OUTPUT_HDFS=$TMPOUTPUTDIR
    remove_tmpoutput

    local -r TMP=`crtemp`

cat <<EOF > ${TMP}
USE DEFAULT;

DROP TABLE IF EXISTS rankings;
CREATE EXTERNAL TABLE rankings (pageURL STRING, pageRank INT, avgDuration INT) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '$INPUT_HDFS/rankings';
DROP TABLE IF EXISTS uservisits_copy;
CREATE EXTERNAL TABLE uservisits_copy (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '$INPUT_HDFS/uservisits';
DROP TABLE IF EXISTS rankings_uservisits_join;
CREATE TABLE rankings_uservisits_join ( sourceIP STRING, avgPageRank DOUBLE, totalRevenue DOUBLE) STORED AS  SEQUENCEFILE LOCATION '$OUTPUT_HDFS/rankings_uservisits_join' $NOTRANSACTIONAL;
INSERT OVERWRITE TABLE rankings_uservisits_join SELECT sourceIP, avg(pageRank), sum(adRevenue) as totalRevenue FROM rankings R JOIN (SELECT sourceIP, destURL, adRevenue FROM uservisits_copy UV WHERE (datediff(UV.visitDate, '1999-01-01')>=0 AND datediff(UV.visitDate, '2000-01-01')<=0)) NUV ON (R.pageURL = NUV.destURL) group by sourceIP order by totalRevenue DESC;
EOF

    $command $TMP
    $verify

    testend $BEGTEST
}

runsqlaggregation() {
    local -r begname=$1
    local -r command=$2
    local -r verify="$3"

    local -r BEGTEST=`testbeg $begname`

    INPUT_HDFS=$TMPINPUTDIR
    OUTPUT_HDFS=$TMPOUTPUTDIR
    remove_tmpoutput

    local -r TMP=`crtemp`

cat << EOF | cat >$TMP

DROP TABLE IF EXISTS uservisits;
CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '$INPUT_HDFS/uservisits';
DROP TABLE IF EXISTS uservisits_aggre;
CREATE TABLE uservisits_aggre ( sourceIP STRING, sumAdRevenue DOUBLE) STORED AS  SEQUENCEFILE LOCATION '$OUTPUT_HDFS/uservisits_aggre' $NOTRANSACTIONAL ;
INSERT OVERWRITE TABLE uservisits_aggre SELECT sourceIP, SUM(adRevenue) FROM uservisits GROUP BY sourceIP;

EOF

    $command $TMP
    $verify 

    testend $BEGTEST
}

runaggregation() {
    runsqlaggregation hiveaggregate hivesqlscript "hive_verifynonzero uservisits_aggre"
    runsqlaggregation sparkaggregate sparksql "spark_verifynonzero uservisits_aggre"
}

runjoin() {
    runsqljoin hivejoin hivesqlscript "hive_verifynonzero rankings_uservisits_join"
    runsqljoin sparkjoin sparksql "spark_verifynonzero rankings_uservisits_join"
}

runscan() {
    runsqlscan hivescan hivesqlscript "hive_verifynonzero uservisits_copy"
    runsqlscan sparksan sparksql "spark_verifynonzero uservisits_copy"
}

run() {
    remove_tmp
    prepare_sql
    runaggregation
    runjoin
    runscan
}

test() {
#    remove_tmp
#    prepare_sql
#    runscan
#    runaggregation
    runjoin
#    runscan
   
}

run
#test