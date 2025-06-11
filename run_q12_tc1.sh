#!/bin/bash

echo_blue() {
    echo -e "\033[0;34m$1\033[0m"
}
echo_green() {
    echo -e "\033[0;32m$1\033[0m"
}
echo_red() {
    echo -e "\033[0;31m$1\033[0m"
}

HADOOP_HOME="/opt/homebrew/Cellar/hadoop/3.4.1/libexec"
PROJECT_DIR=$(pwd)
HDFS_USER_DIR="/user/thang.nguyen"

MY_JAR="$PROJECT_DIR/target/hadoopb1-1.0-SNAPSHOT.jar"
DRIVER_CLASS_Q12="org.example.q12.FullOuterJoin"

# Input files cho Test Case 1 của Câu 12
TC1_FILE1_Q12="$PROJECT_DIR/src/main/java/org/example/q12/testcases/q12_tc1_file1.txt"
TC1_FILE2_Q12="$PROJECT_DIR/src/main/java/org/example/q12/testcases/q12_tc1_file2.txt"

# Thư mục HDFS
INPUT_DIR_Q12_TC1="$HDFS_USER_DIR/input_q12_tc1"
OUTPUT_DIR_Q12_TC1="$HDFS_USER_DIR/output_q12_tc1"

echo "===================================================="
echo "Bắt đầu chạy Câu 12 - Test Case 1"
echo "===================================================="

# 1. Dọn dẹp và chuẩn bị thư mục HDFS
echo_blue "[INFO] Dọn dẹp thư mục HDFS cũ..."
$HADOOP_HOME/bin/hdfs dfs -rm -r -f $OUTPUT_DIR_Q12_TC1
$HADOOP_HOME/bin/hdfs dfs -rm -r -f $INPUT_DIR_Q12_TC1
$HADOOP_HOME/bin/hdfs dfs -mkdir -p $INPUT_DIR_Q12_TC1

# 2. Đẩy file input lên HDFS
echo_blue "[INFO] Đẩy file input Test Case 1 của Câu 12 lên HDFS..."
if [ -f "$TC1_FILE1_Q12" ]; then
    $HADOOP_HOME/bin/hdfs dfs -put $TC1_FILE1_Q12 $INPUT_DIR_Q12_TC1/
else
    echo_red "[ERROR] Không tìm thấy file input: $TC1_FILE1_Q12"
    exit 1
fi

if [ -f "$TC1_FILE2_Q12" ]; then
    $HADOOP_HOME/bin/hdfs dfs -put $TC1_FILE2_Q12 $INPUT_DIR_Q12_TC1/
else
    echo_red "[ERROR] Không tìm thấy file input: $TC1_FILE2_Q12"
    exit 1
fi

echo_blue "[INFO] Kiểm tra file trên HDFS:"
$HADOOP_HOME/bin/hdfs dfs -ls $INPUT_DIR_Q12_TC1

# 3. Chạy MapReduce Job
echo_blue "[INFO] Chạy MapReduce job cho Câu 12 - Test Case 1..."
$HADOOP_HOME/bin/hadoop jar $MY_JAR $DRIVER_CLASS_Q12 $INPUT_DIR_Q12_TC1 $OUTPUT_DIR_Q12_TC1

if [ $? -eq 0 ]; then
    echo_green "[SUCCESS] Job MapReduce Câu 12 - Test Case 1 hoàn thành thành công."
else
    echo_red "[ERROR] Job MapReduce Câu 12 - Test Case 1 thất bại."
    exit 1
fi

# 4. Hiển thị kết quả
echo_blue "[INFO] Kết quả output của Câu 12 - Test Case 1:"
$HADOOP_HOME/bin/hdfs dfs -cat $OUTPUT_DIR_Q12_TC1/part-r-00000

echo "===================================================="
echo_green "Hoàn thành chạy Câu 12 - Test Case 1"
echo "===================================================="