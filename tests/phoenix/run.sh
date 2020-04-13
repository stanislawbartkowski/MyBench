source $FUNCTIONSRC
setenv


prepare_hivetables() {
    local -r TMP=`crtemp`
    local -r INPUT_HDFS=$TMPINPUTDIR

cat << EOF | cat >$TMP

DROP TABLE IF EXISTS uservisits;
CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '$INPUT_HDFS/uservisits';
DROP TABLE IF EXISTS rankings;
CREATE EXTERNAL TABLE rankings (pageURL STRING, pageRank INT, avgDuration INT) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' STORED AS  SEQUENCEFILE LOCATION '$INPUT_HDFS/rankings';
EOF
   hivesqlscript $TMP

}

create_phoenix_uservisits() {
    local -r TABLE=$1
    phoenixremovetable bench.$TABLE
    phoenix_command "CREATE TABLE bench.$TABLE (sourceIP varchar primary key,destURL varchar,visitDate varchar,adRevenue DOUBLE,userAgent varchar,countryCode varchar,languageCode varchar,searchWord varchar,duration INTEGER)"
}

create_phoenix_tables() {
    create_phoenix_uservisits uservisits
    phoenixremovetable bench.rankings
    phoenix_command "CREATE TABLE bench.rankings (pageURL varchar primary key, pageRank INTEGER, avgDuration INTEGER)"
}

prepare_data() {
    local -r BEGTEST=`testbeg loadphoenixdata`
    prepare_hivetables
    create_phoenix_tables   
    hive_phoeniximport uservisits bench.uservisits
    hive_phoeniximport rankings bench.rankings
    testend $BEGTEST
}

phoenix_scantest() {
    local -r BEGTEST=`testbeg scantest`
    
    create_phoenix_uservisits uservisits_copy
    phoenix_command "UPSERT INTO bench.uservisits_copy SELECT * FROM bench.uservisits"
    phoenix_verifynonzero bench.uservisits_copy
    testend $BEGTEST
}

phoenix_aggregatetest() {
    local -r BEGTEST=`testbeg aggregatetest`
    phoenixremovetable bench.uservisits_aggre
    phoenix_command "CREATE TABLE bench.uservisits_aggre ( sourceIP VARCHAR primary key, sumAdRevenue DOUBLE)"
    phoenix_command "UPSERT INTO bench.uservisits_aggre SELECT sourceIP, SUM(adRevenue) FROM bench.uservisits GROUP BY sourceIP"
    phoenix_verifynonzero bench.uservisits_aggre
    testend $BEGTEST
}


phoenix_jointest() {
    local -r BEGTEST=`testbeg jointest`
    phoenixremovetable bench.uservisits_join
    phoenix_command "CREATE TABLE bench.uservisits_join ( sourceIP VARCHAR primary key, avgPageRank DOUBLE, sumAdRevenue DOUBLE)"

    phoenix_command "UPSERT INTO bench.uservisits_join SELECT sourceIP, avg(pageRank), sum(adRevenue) as totalRevenue FROM bench.rankings R JOIN (SELECT sourceIP, destURL, adRevenue FROM bench.uservisits UV WHERE UV.visitDate >= '1999-01-01' AND UV.visitDate <= '2000-01-01') NUV ON (R.pageURL = NUV.destURL) group by sourceIP order by totalRevenue DESC"

    phoenix_verifynonzero bench.uservisits_join
    testend $BEGTEST
}


run() {
    remove_tmp
    prepare_sql   
    prepare_data
    phoenix_scantest
    phoenix_aggregatetest
    phoenix_jointest
}

test() {
#    remove_tmp
#    prepare_sql   
   prepare_data
   phoenix_scantest
   phoenix_aggregatetest
   phoenix_jointest
}

cleanup() {
    hivesqlremovetable  uservisits rankings
    phoenixremovetable bench.uservisits_join bench.uservisits_aggre bench.rankings bench.uservisits bench.uservisits_copy
}

case $1 in 
  cleanup) cleanup;; 
  *) 
    run;;
#    test;;
esac

# test

exit 0