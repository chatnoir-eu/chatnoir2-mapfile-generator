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

import de.webis.chatnoir2.mapfile_generator.warc.WarcRecord;
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
        payloadJson.put(JSON_BODY_KEY, value.getContent(recordEncoding));
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