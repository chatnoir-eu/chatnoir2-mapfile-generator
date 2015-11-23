package de.webis.clueweb.mapreduce;

import de.webis.data.BaseMapper;
import de.webis.warc.GenericWarcRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * MapReduce Mapper class for ClueWeb WARC records.
 *
 * @author Janek Bevendorff
 * @version 1
 */
public class ClueWebWarcMapper extends BaseMapper<LongWritable, GenericWarcRecord> implements ClueWebMapReduceBase
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
    public void map(final LongWritable key, final GenericWarcRecord value, final Context context) throws IOException, InterruptedException
    {
        recordsCounter.increment(1);
        OUTPUT_KEY.clear();
        OUTPUT_DOC.clear();

        final String docId = value.getDocid();

        if (null == docId) {
            LOG.warn(String.format("Skipped document #%d with null ID", key.get()));
            nullIdCounter.increment(1);
            return;
        }

        LOG.debug(String.format("Mapping document %s", docId));

        final JSONObject outputJsonDoc = new JSONObject();

        // WARC headers
        final Set<Map.Entry<String, String>> headers = value.getHeaderMetadata();
        final JSONObject warcHeadersJson = new JSONObject();
        for (final Map.Entry<String, String> entry : headers) {
            warcHeadersJson.put(entry.getKey(), entry.getValue());
        }
        outputJsonDoc.put("metadata", warcHeadersJson);

        final JSONObject payloadJson = new JSONObject();
        // TODO: extract HTTP headers
        payloadJson.put("headers", new JSONObject());
        payloadJson.put("body", value.getContent());

        outputJsonDoc.put("payload", payloadJson);

        OUTPUT_KEY.set(generateUUID(getUUIDPrefix(), docId));
        context.write(OUTPUT_KEY, OUTPUT_DOC);
        generatedCounter.increment(1);
    }
}