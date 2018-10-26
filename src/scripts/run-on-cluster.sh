#!/usr/bin/env bash
#
# Helper script for running the compiled JAR on a Hadoop cluster.
# Supports ClueWeb09 (09), ClueWeb12 (12) and CommonCrawl (cc) at the moment.
#
# Before running this script, adjust the paths as needed.
# $input_path and $output_path are HDFS paths, $JAR_OUT_PATH is the compile output
# path on your local file system.

JAR_OUT_PATH="$(dirname "$(realpath "$0")")/../../build/libs"

if [ "$1" == "" ]; then
    echo "USAGE: $(basename "$0") cw09|cw12|CC-MAIN-2015-11|CC-MAIN-2017-04|CC-... [MIN SEGMENT] [MAX SEGMENT]"
    exit 1
fi

OUTPUT_PATH="${1}-mapfile"
MIN_SEGMENT="$2"
MAX_SEGMENT="$3"

trap exit INT

if [ "$1" == "cw09" ] || [ "$1" == "cw12" ]; then
    version=$(echo -n "$1" | cut -c3-4)
    format="clueweb${version}"
    segments_path="/corpora/corpora-thirdparty/corpus-clueweb/${version}"
    warc_glob="*/*"
else
    version="$1"
    format="commoncrawl"
    segments_path="/corpora/corpus-commoncrawl/common-crawl/crawl-data/${version}/segments"
    warc_glob="warc/*"
fi

IFS=$'\n'
COUNTER=-1
for segment in $(hadoop fs -ls "${segments_path}" | awk '{ print $8; }'); do
    COUNTER=$(($COUNTER + 1))

    if [ "$MIN_SEGMENT" != "" ] && [ "$(basename ${segment})" \< "$MIN_SEGMENT" ]; then
        continue
    fi
    if [ "$MAX_SEGMENT" != "" ] && [ "$(basename ${segment})" \> "$MAX_SEGMENT" ]; then
        break
    fi

    input_path="${segment}/${warc_glob}"
    echo "Processing segment '${input_path}'..."

    hadoop jar ${JAR_OUT_PATH}/chatnoir2-mapfile-generator-*-all.jar \
        de.webis.chatnoir2.mapfile_generator.app.MapFileGenerator \
        -prefix "${format}" -input "${input_path}" -format "${format}" \
        -output "${OUTPUT_PATH}/segment-$(printf '%05d' ${COUNTER})" | tee -a job.log

    if tail -n 100 job.log | grep -q "Job failed as tasks failed"; then
        echo "Job for segment $(basename ${segment}) failed!" >&2
        exit 1
    fi
done
