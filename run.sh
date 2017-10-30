#!/usr/bin/bash

SPARK_HOME=/data0/spark/spark-2.0.0-bin
SPARK=$SPARK_HOME/bin

WK_DIR=`pwd`

echo "start get data"
sql=`python $WK_DIR/script/partial_join.py $WK_DIR/conf/config.conf`
echo $sql
path=`echo $sql | awk -F" " '{print $4}' | awk -F"'" '{print $2}'`
table=`echo $sql | awk -F"from" '{print $2}' | awk -F"where" '{print $1}'`
columns=`hive -e "desc $table" | awk '{a = (a","$1)}END{b=substr(a,2);print b}' | awk -F",," '{print $1}'`
leaf_path=`echo $path | awk -F"/" '{print $NF}'`
parent_path=`echo $path | awk -F"$leaf_path" '{print $1}'`
echo "path: $path"
echo "parent_path : $parent_path"
echo "$table"
echo "$columns"

echo "hadoop fs -rmr $parent_path"
hadoop fs -rmr $parent_path || echo "path $parent_path not exists"
hadoop fs -mkdir -p "$path" || true
hive -e "$sql"
echo "success get data"

hadoop fs -chmod -R 777 $parent_path 

$SPARK/spark-submit \
--class run \
--jars $WK_DIR/jar/hebe.jar,$WK_DIR/jar/protobuf-java-2.4.2.jar,$WK_DIR/jar/dom4j-1.6.1.jar,$WK_DIR/jar/commons-cli-1.2.jar,/data0/spark/spark-2.0.0-bin/jars/spark-yarn_2.11-2.0.0.jar,/data0/spark/spark-2.0.0-bin/jars/spark-core_2.11-2.0.0.jar \
--master yarn-cluster \
--executor-memory 4G \
--executor-cores 2 \
--num-executors 88 \
--driver-memory 4G \
--files $WK_DIR/conf/config.conf \
$WK_DIR/jar/etl_spark.jar \
config.conf

