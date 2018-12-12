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

package de.webis.chatnoir2.mapfile_generator.app;

import de.webis.chatnoir2.mapfile_generator.inputformats.DirnamePassthroughInputFormat;
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
    @SuppressWarnings({"static-access", "Duplicates"})
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

        // set task timeout to 20 minutes
        conf.setInt("mapreduce.task.timeout", 1200000);

        final Job job = Job.getInstance(conf);
        job.setJobName("chatnoir-mapfile-merger");
        job.setJarByClass(MapFileBatchMerger.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(FileNameMapper.class);

        job.setReducerClass(MapFileReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(DirnamePassthroughInputFormat.class);
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
