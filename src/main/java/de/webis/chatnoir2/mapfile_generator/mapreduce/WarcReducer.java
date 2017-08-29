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
