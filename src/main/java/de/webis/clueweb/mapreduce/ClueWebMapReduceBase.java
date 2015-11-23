package de.webis.clueweb.mapreduce;

import org.apache.hadoop.io.*;

/**
 * Base interface for ClueWeb mappers and reducers.
 *
 * @author Janek Bevendorff
 * @version 1
 */
public interface ClueWebMapReduceBase
{
    Text OUTPUT_KEY = new Text();
    Text OUTPUT_DOC = new Text();
    Text EMPTY_TEXT = new Text();

    /**
     * MapReduce counters.
     */
    enum RecordCounters {
        /**
         * Total records read.
         */
        RECORDS,

        /**
         * Number of skipped records due to null ID.
         */
        SKIPPED_RECORDS_NULL_ID,

        /**
         * Number of skipped records that are too large.
         */
        SKIPPED_RECORDS_TOO_LARGE,

        /**
         * Number of skipped records that are too small.
         */
        SKIPPED_RECORDS_TOO_SMALL,

        /**
         * Number of skipped records that are too deeply nested.
         */
        SKIPPED_RECORDS_TOO_DEEP,

        /**
         * Number of actual JSON docs generated.
         */
        GENERATED_DOCS,

        /**
         * Number of documents with no plain-text content after reduce stage.
         */
        NO_CONTENT
    }
}
