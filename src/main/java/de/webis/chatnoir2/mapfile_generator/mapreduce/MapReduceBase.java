/*
 * Webis MapFile Generator.
 * Copyright (C) 2015-2017 Janek Bevendorff, Webis Group
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package de.webis.chatnoir2.mapfile_generator.mapreduce;

import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;

/**
 * Base interface for ClueWeb mappers and reducers.
 *
 * @author Janek Bevendorff
 */
public interface MapReduceBase
{
    Text OUTPUT_URI = new Text();
    Text OUTPUT_KEY = new Text();
    Text OUTPUT_DOC = new Text();

    String JSON_METADATA_KEY     = "metadata";
    String JSON_PAYLOAD_KEY      = "payload";
    String JSON_PAYLOAD_ENCODING = "encoding";
    String JSON_HEADERS_KEY      = "headers";
    String JSON_BODY_KEY         = "body";

    String DATA_OUTPUT_NAME = "data";
    String URI_OUTPUT_NAME  = "uri";

    Logger LOG = Logger.getLogger(BaseMapper.class);

    /**
     * MapReduce counters.
     */
    enum RecordCounters {
        /**
         * Total records read.
         */
        RECORDS,

        /**
         * Number of skipped records that are not of type "response".
         */
        SKIPPED_RECORDS,

        /**
         * Number of skipped records that are too large.
         */
        SKIPPED_RECORDS_TOO_LARGE,

        /**
         * Number of skipped records that are too small.
         */
        SKIPPED_RECORDS_TOO_SMALL,

        /**
         * Number of binary records.
         */
        BINARY_RECORDS,

        /**
         * Number of actual JSON docs generated.
         */
        GENERATED_DOCS,

        /**
         * Number of MapFile data entries generated.
         */
        MAPFILE_DATA_ENTRIES,

        /**
         * Number of MapFile URI entries generated.
         */
        MAPFILE_URI_ENTRIES
    }
}
