#!/bin/bash
#
# Helper script for running the compiled JAR on a Hadoop cluster.
# Supports ClueWeb09 (09), ClueWeb12 (12) and CommonCrawl (cc) at the moment.
#
# Before running this script, adjust the paths as needed.
# $input_path and $output_path are HDFS paths, $JAR_OUT_PATH is the compile output
# path on your local file system.

JAR_OUT_PATH="$(dirname "$0")/../../build/libs"

if [ "$1" == "cw09" ] || [ "$1" == "cw12" ]; then
    version=$(echo -n "$1" | cut -c3-4)
    input_path="/corpora/clueweb/${version}/*/*/*"
    output_path="${version}-mapfile"
    format="clueweb${version}"
elif [ "$1" == "cc1511" ] || [ "$1" == "cc1704" ]; then
    version="CC-MAIN-20$(echo -n "$1" | cut -c3-4)-$(echo -n "$1" | cut -c5-6)"
    input_path="/corpora/corpus-commoncrawl/common-crawl/crawl-data/${version}/segments/*/warc/*"
    output_path="${version}-mapfile"
    format="commoncrawl"
else
    echo "USAGE: $(basename "$0") cw09|cw12|cc1511|cc1704"
    exit 1
fi

hadoop fs -rm -r "${output_path}"
hadoop jar ${JAR_OUT_PATH}/mapfile-generator-*-all.jar de.webis.chatnoir2.mapfile_generator.app.MapFileGenerator \
    -prefix "${format}" -input "${input_path}" -format "${format}" -output "${output_path}"
