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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partition UUIDs to reduce tasks in a predictable way.
 *
 * @author Janek Bevendorff
 */
public class WarcUUIDPartitioner extends Partitioner<Text, Writable>
{
    @Override
    public int getPartition(final Text key, final Writable values, final int numPartitions)
    {
        String str = key.toString();
        // strip prefixes from composite keys
        if (str.startsWith(MapReduceBase.DATA_OUTPUT_NAME)) {
            str = str.substring(MapReduceBase.DATA_OUTPUT_NAME.length());
        } else if (str.startsWith(MapReduceBase.URI_OUTPUT_NAME)) {
            str = str.substring(MapReduceBase.URI_OUTPUT_NAME.length());
        }
        return (str.hashCode() % numPartitions + numPartitions) % numPartitions;
    }
}
