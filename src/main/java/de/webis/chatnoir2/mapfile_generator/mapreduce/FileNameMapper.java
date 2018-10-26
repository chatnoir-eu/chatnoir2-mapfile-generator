/*
 * Webis MapFile Generator.
 * Copyright (C) 2018 Janek Bevendorff, Webis Group
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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Map file paths according to their basename portion.
 */
public class FileNameMapper extends Mapper<Text, NullWritable, Text, Text> implements MapReduceBase
{
    private static Text OUT_KEY = new Text();
    private static Counter mRecordCounter = null;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException
    {
        super.setup(context);
        mRecordCounter = context.getCounter(RecordCounters.RECORDS);
    }

    @Override
    public void map(final Text key, final NullWritable value, final Context context) throws IOException, InterruptedException
    {
        Path path = new Path(key.toString());

        if (!path.getName().startsWith(DATA_OUTPUT_NAME) && !path.getName().startsWith(URI_OUTPUT_NAME)) {
            // this is not a map file split
            return;
        }

        OUT_KEY.set(path.getName());
        context.write(OUT_KEY, key);
        mRecordCounter.increment(1);
    }
}
