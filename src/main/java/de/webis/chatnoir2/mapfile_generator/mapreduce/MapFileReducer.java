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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
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
        MapFile.Merger merger = new MapFile.Merger(context.getConfiguration());
        Path[] sourcePaths = paths.toArray(new Path[0]);

        // Check output file size every 30 seconds in a separate thread and report
        // progress if new size differs to avoid this task attempt being killed.
        Thread progressThread = new Thread(() -> {
            long lastSize = 0;
            final Path outDataFile = new Path(outMapFile, "data");

            while (!Thread.interrupted()) {
                try {
                    Thread.sleep(30000);

                    long newSize = fs.getFileStatus(outDataFile).getLen();
                    if (lastSize != newSize) {
                        context.progress();
                        lastSize = newSize;
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (IOException e) {
                    // If file is not accessible, log error and then do nothing.
                    // The container will time out if it keeps happening.
                    LOG.error("Failure retrieving map file status (retrying, error may be temporary)", e);
                }
            }
        });
        progressThread.start();

        merger.merge(sourcePaths, false, outMapFile);

        // we are finished, shoot the progress thread
        try {
            progressThread.interrupt();
            progressThread.join();
        } catch (InterruptedException ignored) {}

    }
}
