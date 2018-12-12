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
