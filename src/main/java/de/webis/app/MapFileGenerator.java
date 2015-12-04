/*
 * Webis MapFile generator.
 * Copyright (C) 2015 Janek Bevendorff <janek.bevendorff@uni-weimar.de>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package de.webis.app;

import de.webis.inputformats.ClueWeb09InputFormat;
import de.webis.inputformats.ClueWeb12InputFormat;
import de.webis.mapper.WarcMapper;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;
import java.util.List;

/**
 * Generic MapFile generator for web corpora.
 *
 * @author Janek Bevendorff <janek.bevendorff@uni-weimar.de>
 * @version 1
 */
public class MapFileGenerator extends MapFileTool
{
    private static final String[] UUID_PREFIX_OPTION  = {"prefix", "p"};
    private static final String[] INPUT_OPTION        = {"input",  "i"};
    private static final String[] INPUT_FORMAT_OPTION = {"format", "f"};
    private static final String[] OUTPUT_OPTION       = {"output", "o"};

    private static final List<String> SUPPORTED_INPUT_FORMATS = Arrays.asList(
            "clueweb09",
            "clueweb12"
    );

    private static Class<? extends InputFormat> getInputFormatClass(final String format)
    {
        switch (format) {
            case "clueweb09":
                return ClueWeb09InputFormat.class;
            case "clueweb12":
                return ClueWeb12InputFormat.class;
            default:
                throw new RuntimeException("Unsupported input format '" + format + "'");
        }
    }

    private static Class<? extends Mapper> getMapperClass(final String format)
    {
        switch (format) {
            case "clueweb09":
            case "clueweb12":
                return WarcMapper.class;
            default:
                throw new RuntimeException("Unsupported input format '" + format + "'");
        }
    }

    @Override
    @SuppressWarnings("static-access")
    public int run(final String[] args) throws Exception
    {
        final Options options = new Options();
        options.addOption(OptionBuilder.
                withArgName("PREFIX").
                hasArg().
                withLongOpt(UUID_PREFIX_OPTION[0]).
                withDescription("Prefix to use for UUID generation").
                isRequired().
                create(UUID_PREFIX_OPTION[1]));
        options.addOption(OptionBuilder.
                withArgName("INPUT_FORMAT").
                hasArg().
                withLongOpt(INPUT_FORMAT_OPTION[0]).
                withDescription("Input format for reading the corpus (e.g. clueweb09, clueweb12, ...)").
                isRequired().
                create(INPUT_FORMAT_OPTION[1]));
        options.addOption(OptionBuilder.
                withArgName("PATH").
                hasArg().
                withLongOpt(INPUT_OPTION[0]).
                withDescription("Input corpus path").
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
            return 1;
        }

        final String uuidPrefix  = cmdline.getOptionValue(UUID_PREFIX_OPTION[0]);
        final String inputPath   = cmdline.getOptionValue(INPUT_OPTION[0]);
        final String inputFormat = cmdline.getOptionValue(INPUT_FORMAT_OPTION[0]);
        final String outputPath  = cmdline.getOptionValue(OUTPUT_OPTION[0]);

        if (!SUPPORTED_INPUT_FORMATS.contains(inputFormat)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(this.getClass().getSimpleName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            System.err.printf("Argument error: Input format '%s' is not supported.\nSupported input formats are: %s\n",
                    inputFormat, StringUtils.join(SUPPORTED_INPUT_FORMATS, ", "));
            return -1;
        }

        LOG.info("Tool name: " + MapFileGenerator.class.getSimpleName());
        LOG.info(" - prefix: "  + uuidPrefix);
        LOG.info(" - input: "   + inputPath);
        LOG.info(" - format: "  + inputFormat);
        LOG.info(" - output: "  + outputPath);

        final Configuration conf = getConf();
        conf.set("mapfile.uuid.prefix", uuidPrefix);

        final Job job = Job.getInstance(conf);
        job.setJobName(String.format("mapfile-generator-%s", inputFormat));
        job.setJarByClass(MapFileGenerator.class);
        job.setOutputFormatClass(MapFileOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(getInputFormatClass(inputFormat));
        job.setMapperClass(getMapperClass(inputFormat));

        FileInputFormat.setInputPaths(job, inputPath);
        MapFileOutputFormat.setOutputPath(job, new Path(outputPath));
        MapFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        MapFileOutputFormat.setCompressOutput(job, true);

        job.waitForCompletion(true);

        return 0;
    }
}
