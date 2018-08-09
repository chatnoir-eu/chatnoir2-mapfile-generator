# ChatNoir Map File Generator

Hadoop MapReduce tool to map raw WARC files to HDFS map files.
This is the very first step when indexing a new corpus. The map files
will serve as input to the actual indexer and will later be used
to retrieve the raw HTML contents of a document through the web frontend.

## Compiling the Sources
To build the sources, call  

    gradle shadowJar

from this source directory to download third-party dependencies and compile the sources.

The generated shadow (fat) JAR will be in `build/libs`. The JAR can be submitted to run on
a Hadoop cluster. For ease of use, there is a helper script `src/scripts/run_on_cluster.sh` for
starting the mapping process.
