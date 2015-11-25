/*
 * Webis MapFile generator.
 * Copyright (C) 2015 Janek Bevendorff <janek.bevendorff@uni-weimar.de>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package de.webis.mapper;

import de.webis.warc.WarcRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.json.JSONObject;

import java.io.IOException;

/**
 * MapReduce Mapper class for WARC records.
 *
 * @author Janek Bevendorff
 * @version 1
 */
public class WarcMapper extends BaseMapper<LongWritable, WarcRecord> implements WarcMapReduceBase
{
    protected static Counter recordsCounter;
    protected static Counter nullIdCounter;
    protected static Counter generatedCounter;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException
    {
        super.setup(context);
        recordsCounter   = context.getCounter(RecordCounters.RECORDS);
        nullIdCounter    = context.getCounter(RecordCounters.SKIPPED_RECORDS_NULL_ID);
        generatedCounter = context.getCounter(RecordCounters.GENERATED_DOCS);
    }

    @Override
    public void map(final LongWritable key, final WarcRecord value, final Context context) throws IOException, InterruptedException
    {
        recordsCounter.increment(1);
        OUTPUT_KEY.clear();
        OUTPUT_DOC.clear();

        final String docId = value.getDocid();

        if (null == docId) {
            LOG.info(String.format("Skipped document #%d with null ID", key.get()));
            nullIdCounter.increment(1);
            return;
        }

        LOG.debug(String.format("Mapping document %s", docId));

        // WARC headers
        final JSONObject outputJsonDoc = new JSONObject();
        outputJsonDoc.put("metadata", value.getHeader().getHeaderMetadata());

        // content headers and body
        final JSONObject payloadJson = new JSONObject();
        payloadJson.put("headers", value.getContentHeaders());
        payloadJson.put("body", value.getContent());
        outputJsonDoc.put("payload", payloadJson);

        OUTPUT_KEY.set(generateUUID(docId));
        OUTPUT_DOC.set(outputJsonDoc.toString());
        context.write(OUTPUT_KEY, OUTPUT_DOC);
        generatedCounter.increment(1);
    }
}