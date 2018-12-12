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

import de.webis.chatnoir2.mapfile_generator.util.MapFileMerger;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;

/**
 * Reducer for merging map files.
 */
public class MapFileReducer extends Reducer<Text, Text, NullWritable, NullWritable> implements MapReduceBase
{
    @Override
    public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException
    {
        // deduplicate paths
        HashSet<Path> paths = new HashSet<>();
        for (Text value: values) {
            paths.add(new Path(value.toString()));
        }

        if (paths.isEmpty()) {
            return;
        }

        Path outMapFile = new Path(FileOutputFormat.getOutputPath(context), key.toString());
        FileSystem fs = outMapFile.getFileSystem(context.getConfiguration());
        fs.mkdirs(outMapFile);

        LOG.info("Merging output map files...");
        Path[] sourcePaths = paths.toArray(new Path[0]);
        MapFileMerger merger = new MapFileMerger(context);
        merger.merge(sourcePaths, false, outMapFile);
    }
}
