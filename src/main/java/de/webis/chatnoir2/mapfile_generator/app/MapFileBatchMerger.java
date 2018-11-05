/*
 * Webis MapFile Merger.
 * Copyright (C) 2015-2018 Janek Bevendorff, Webis Group
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

package de.webis.chatnoir2.mapfile_generator.app;

import de.webis.chatnoir2.mapfile_generator.inputformats.DirNamePassthroughInputFormat;
import de.webis.chatnoir2.mapfile_generator.mapreduce.FileNameMapper;
import de.webis.chatnoir2.mapfile_generator.mapreduce.MapFileReducer;
import de.webis.chatnoir2.mapfile_generator.mapreduce.PassthroughPartitioner;
import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

/**
 * Merger for map file generator batches.
 * This tool merges all splits of the same name across the given set of input batches.
 */
public class MapFileBatchMerger extends MapFileTool
{
    private static final String[] INPUT_OPTION  = {"input",  "i"};
    private static final String[] OUTPUT_OPTION = {"output", "o"};

    @Override
    @SuppressWarnings("static-access")
    public int run(final String[] args) throws Exception
    {
        final Options options = new Options();
        options.addOption(OptionBuilder.
                withArgName("PATH").
                hasArg().
                withLongOpt(INPUT_OPTION[0]).
                withDescription("Input path pattern").
                isRequired().
                create(INPUT_OPTION[1]));
        options.addOption(OptionBuilder.
                withArgName("PATH").
                hasArg().
                withLongOpt(OUTPUT_OPTION[0]).
                withDescription("Output MapFile").
                isRequired().
                create(OUTPUT_OPTION[1]));

        final CommandLine cmdline = parseCmdline(options, args);
        if (null == cmdline) {
            return ERROR;
        }

        final String inputPathStr   = cmdline.getOptionValue(INPUT_OPTION[0]);
        final String outputPathStr  = cmdline.getOptionValue(OUTPUT_OPTION[0]);

        LOG.info("Tool name: " + MapFileBatchMerger.class.getSimpleName());
        LOG.info(" - input:  " + inputPathStr);
        LOG.info(" - output: " + outputPathStr);

        final Configuration conf = getConf();

        // disable speculative reduce execution to prevent two processes from writing to the same map file
        conf.setBoolean("mapreduce.reduce.speculative", false);

        // set task timeout to 35 minutes
        conf.setInt("mapreduce.task.timeout", 2100000);

        final Job job = Job.getInstance(conf);
        job.setJobName("chatnoir-mapfile-merger");
        job.setJarByClass(MapFileBatchMerger.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(FileNameMapper.class);

        job.setReducerClass(MapFileReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(DirNamePassthroughInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setPartitionerClass(PassthroughPartitioner.class);

        final Path outputPath = new Path(outputPathStr);
        final Path inputPath = new Path(inputPathStr);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // enable block compression
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        job.waitForCompletion(true);

        return SUCCESS;
    }

    /**
     * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
     */
    public static void main(final String[] args) throws Exception
    {
        LOG.info("Running " + MapFileBatchMerger.class.getSimpleName() + " with args " + Arrays.toString(args));
        System.exit(ToolRunner.run(new MapFileBatchMerger(), args));
    }
}
