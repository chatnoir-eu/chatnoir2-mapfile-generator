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

package de.webis.chatnoir2.mapfile_generator.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Iterator;

/**
 * Reducer for writing JSON records generated from WARCs to multiple outputs.
 *
 * @author Janek Bevendorff
 */
public class WarcReducer extends Reducer<Text, Text, Text, Text> implements MapReduceBase
{
    protected static Counter mMapFileDataCounter;
    protected static Counter mMapFileURICounter;
    protected MultipleOutputs<Text, Text> mMultipleOutputs;

    @Override
    @SuppressWarnings("unchecked")
    protected void setup(final Context context) throws IOException, InterruptedException
    {
        super.setup(context);
        mMultipleOutputs    = new MultipleOutputs(context);
        mMapFileDataCounter = context.getCounter(RecordCounters.MAPFILE_DATA_ENTRIES);
        mMapFileURICounter  = context.getCounter(RecordCounters.MAPFILE_URI_ENTRIES);
    }

    @Override
    public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException
    {
        final Iterator<Text> it = values.iterator();
        if (!it.hasNext())
            return;

        final Text data = it.next();

        final String strKey  = key.toString();
        final String strData = data.toString();

        // strip prefixes from composite keys
        if (strKey.startsWith(DATA_OUTPUT_NAME)) {
            key.set(strKey.substring(DATA_OUTPUT_NAME.length()));
            mMultipleOutputs.write(DATA_OUTPUT_NAME, key, data);
            mMapFileDataCounter.increment(1);
        } else if (strKey.startsWith(URI_OUTPUT_NAME)) {
            key.set(strKey.substring(URI_OUTPUT_NAME.length()));
            if (strData.startsWith(URI_OUTPUT_NAME))
                data.set(strData.substring(URI_OUTPUT_NAME.length()));
            mMultipleOutputs.write(URI_OUTPUT_NAME, key, data);
            mMapFileURICounter.increment(1);
        } else {
            LOG.error("Key '" + strKey + "' doesn't start with known prefix!");
        }
    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException
    {
        mMultipleOutputs.close();
    }
}
