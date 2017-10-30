###############################################################################
#使用方法：
# 使用方法：
#切换到需要编译、打包的根目录，传入需要编译的java文件即可，支持多java文件。
#如果修改脚本class_path变量，可增加引入的jar包。
#一个例子：
# ./create_jar.sh A.java B.java C.java ...
################################################################################

#!/bin/sh
# Program:
# History:
# Date:  2012/12/10 11:37:25
set -o errexit
set -o pipefail

#if [ "$#" -eq 0 ];then 
#    echo "[ERR] argc is -ne 0."  >&2
#    exit 1
#fi


pp=/data0/result/chenglei/hebe/jar
class_path=$pp/hadoop-ant-0.20.203.0.jar:$pp/hadoop-core-0.20.203.0.jar:$pp/hadoop-tools-0.20.203.0.jar:$pp/dom4j-1.6.1.jar:$pp/commons-cli-1.2.jar:$pp/protobuf-java-2.4.2.jar


echo "[INFO][VAR classpath:] "
echo "$class_path "

#java_file=$@
#java_file="duplicate_positive.java noise.java format_output.java feature_transform.java count_feature_pvclk.java sample.java gbdt_transform.java hebe/Hebe.java"

java_file="hebe/Hebe.java"

echo ""
echo "[INFO][CMD]"
echo "javac -classpath  $class_path   $java_file "

#javac -classpath $class_path hebe/Hebe.java
javac -classpath $class_path $java_file 
echo ""
echo "[INFO] create jar "

find ./ -name "*.class" | xargs jar -cvf ../jar/hebe.jar  

echo ""
echo "[INFO] list jar "

jar -tvf ../jar/hebe.jar


