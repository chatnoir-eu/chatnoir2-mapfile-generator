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

package de.webis.chatnoir2.mapfile_generator.outputformats;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * MapFileOutputFormat that allows for appending to an existing map file
 * by writing to a temporary file and then merging it with the existing map file.
 */
public class MergingMapFileOutputFormat extends MapFileOutputFormat
{
    private static final Log LOG = LogFactory.getLog(MergingMapFileOutputFormat.class);

    /**
     * Check output requirements, but ignore if output directory already exists.
     *
     * @param job job context
     */
    @Override
    public void checkOutputSpecs(JobContext job) throws IOException
    {
        try {
            super.checkOutputSpecs(job);
        } catch (FileAlreadyExistsException ignored) {}
    }

    /**
     * Instantiate and return a MapFile Writer which merges the written map
     * file with an existing map file on file close.
     *
     * @param context job context
     * @return the merging map file writer
     */
    @Override
    public RecordWriter<WritableComparable<?>, Writable> getRecordWriter(TaskAttemptContext context) throws IOException
    {
        final Configuration conf = context.getConfiguration();
        CompressionCodec codec = null;
        SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.NONE;
        if (getCompressOutput(context)) {
            compressionType = SequenceFileOutputFormat.getOutputCompressionType(context);
            Class<?> codecClass = getOutputCompressorClass(context, DefaultCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        }

        final Path workOutFile = getDefaultWorkFile(context, "");
        final Path workTempFile = new Path(workOutFile.toString() + "_tmp");
        final Path realOutFile = new Path(conf.get(FileOutputFormat.OUTDIR), workOutFile.getName());
        final MapFile.Writer out = new MapFile.Writer(
                conf, workTempFile,
                MapFile.Writer.keyClass(context.getOutputKeyClass().asSubclass(WritableComparable.class)),
                MapFile.Writer.valueClass(context.getOutputValueClass().asSubclass(Writable.class)),
                MapFile.Writer.compression(compressionType, codec));

        return new RecordWriter<WritableComparable<?>, Writable>() {
            @Override
            public void write(WritableComparable<?> key, Writable value) throws IOException {
                out.append(key, value);
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException {
                out.close();

                FileSystem fs = workTempFile.getFileSystem(conf);
                if (fs.exists(realOutFile)) {
                    LOG.info("Merging output map files...");
                    MapFile.Merger merger = new MapFile.Merger(conf);
                    Path[] sourcePaths = {realOutFile, workTempFile};

                    // check output file size every 30 seconds in a separate thread and report
                    // progress if new size differs to avoid this task attempt being killed
                    Thread progressThread = new Thread(() -> {
                        long lastSize = 0;
                        final Path dataPath = new Path(workOutFile, "data");
                        while (!Thread.interrupted()) {
                            try {
                                long newSize = fs.getFileStatus(dataPath).getLen();
                                if (lastSize != newSize) {
                                    context.progress();
                                    lastSize = newSize;
                                }
                                Thread.sleep(30000);
                            } catch (InterruptedException e) {
                                return;
                            } catch (IOException ignored) {
                                // do nothing if file is not accessible, container will time out if it keeps happening
                            }
                        }
                    });
                    progressThread.start();

                    merger.merge(sourcePaths, false, workOutFile);

                    // we are finished, shoot the progress thread
                    try {
                        progressThread.interrupt();
                        progressThread.join();
                    } catch (InterruptedException ignored) {}

                    fs.delete(workTempFile, true);
                } else {
                    LOG.debug("No existing map file found, promoting output...");
                    fs.rename(workTempFile, workOutFile);
                }
            }
        };
    }
}
