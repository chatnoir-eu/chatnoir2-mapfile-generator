/*
 * Copyright (C) 2015-2018 Janek Bevendorff, Webis Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
