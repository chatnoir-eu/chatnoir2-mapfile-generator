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

package de.webis.mapreduce;

import de.webis.warc.WarcRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.json.JSONObject;

import java.io.IOException;
import java.util.TreeMap;

/**
 * MapReduce Mapper class for WARC records.
 *
 * @author Janek Bevendorff
 */
public class WarcMapper extends BaseMapper<LongWritable, WarcRecord>
{
    protected static Counter mRecordsCounter;
    protected static Counter mSkippedRecordCounter;
    protected static Counter mGeneratedCounter;
    protected static Counter mBinaryRecordCounter;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException
    {
        super.setup(context);
        mRecordsCounter       = context.getCounter(RecordCounters.RECORDS);
        mSkippedRecordCounter = context.getCounter(RecordCounters.SKIPPED_RECORDS);
        mGeneratedCounter     = context.getCounter(RecordCounters.GENERATED_DOCS);
        mBinaryRecordCounter  = context.getCounter(RecordCounters.BINARY_RECORDS);
    }

    @Override
    public void map(final LongWritable key, final WarcRecord value, final Context context) throws IOException, InterruptedException
    {
        mRecordsCounter.increment(1);
        OUTPUT_URI.clear();
        OUTPUT_KEY.clear();
        OUTPUT_DOC.clear();

        final String recordId = value.getRecordId();

        if (!value.getRecordType().equals("response") && !value.getRecordType().equals("request")) {
            LOG.debug(String.format("Skipped record %s of type %s", recordId, value.getRecordType()));
            mSkippedRecordCounter.increment(1);
            return;
        }

        LOG.debug(String.format("Mapping document %s", recordId));

        // WARC headers
        final JSONObject outputJsonDoc = new JSONObject();
        final TreeMap<String, String> warcHeaders = value.getHeader().getHeaderMetadata();
        outputJsonDoc.put(JSON_METADATA_KEY, warcHeaders);

        final String recordEncoding = value.getContentEncoding();
        if (null == recordEncoding) {
            mBinaryRecordCounter.increment(1);
        }

        // content headers and body
        final JSONObject payloadJson = new JSONObject();
        payloadJson.put(JSON_HEADERS_KEY, value.getContentHeaders());
        payloadJson.put(JSON_BODY_KEY, value.getContent());
        payloadJson.put(JSON_PAYLOAD_ENCODING, null != recordEncoding ? "plain" : "base64");
        outputJsonDoc.put(JSON_PAYLOAD_KEY, payloadJson);

        OUTPUT_KEY.set(DATA_OUTPUT_NAME + generateUUID(recordId).toString());
        OUTPUT_DOC.set(outputJsonDoc.toString());
        context.write(OUTPUT_KEY, OUTPUT_DOC);

        final String uri = warcHeaders.get("WARC-Target-URI");
        if (null != uri) {
            OUTPUT_URI.set(URI_OUTPUT_NAME + uri);
            context.write(OUTPUT_URI, OUTPUT_KEY);
        }

        mGeneratedCounter.increment(1);
    }
}