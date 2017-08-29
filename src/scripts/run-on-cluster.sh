#!/bin/bash
#
# Helper script for running the compiled JAR on a Hadoop cluster.
# Supports ClueWeb09 (09), ClueWeb12 (12) and CommonCrawl (cc) at the moment.
#
# Before running this script, adjust the paths as needed.
# $input_path and $output_path are HDFS paths, $JAR_OUT_PATH is the compile output
# path on your local file system.

JAR_OUT_PATH="$(dirname "$0")/../../build/libs"

if [ "$1" == "09" ] || [ "$1" == "12" ]; then
    input_path="/corpora/clueweb/${1}/*/*/*"
    output_path="${1}-mapfile"
    format="clueweb${1}"
elif [ "$1" == "cc" ]; then
    input_path="/corpora/corpus-commoncrawl/common-crawl/crawl-data/CC-MAIN-2015-11/segments/*/warc/*"
    output_path="CC-MAIN-2015-11-mapfile"
    format="commoncrawl"
else
    echo "USAGE: $(basename "$0") 09|12|cc"
    exit 1
fi

hadoop fs -rm -r "${output_path}"
hadoop jar ${JAR_OUT_PATH}/mapfile-generator-*-all.jar de.webis.chatnoir2.mapfile_generator.app.MapFileGenerator \
    -prefix "${format}" -input "${input_path}" -format "${format}" -output "${output_path}"
