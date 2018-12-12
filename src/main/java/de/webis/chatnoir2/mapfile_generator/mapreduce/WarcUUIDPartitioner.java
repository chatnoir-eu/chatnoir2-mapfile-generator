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
