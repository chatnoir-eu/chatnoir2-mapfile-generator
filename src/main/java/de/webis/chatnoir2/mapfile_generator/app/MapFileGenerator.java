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

package de.webis.chatnoir2.mapfile_generator.app;

import de.webis.chatnoir2.mapfile_generator.inputformats.ClueWeb09InputFormat;
import de.webis.chatnoir2.mapfile_generator.inputformats.ClueWeb12InputFormat;
import de.webis.chatnoir2.mapfile_generator.inputformats.CommonCrawlInputFormat;
import de.webis.chatnoir2.mapfile_generator.mapreduce.MapReduceBase;
import de.webis.chatnoir2.mapfile_generator.mapreduce.WarcUUIDPartitioner;
import de.webis.chatnoir2.mapfile_generator.mapreduce.WarcMapper;
import de.webis.chatnoir2.mapfile_generator.mapreduce.WarcReducer;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;
import java.util.List;

/**
 * Generic MapFile generator for web corpora.
 *
 * @author Janek Bevendorff <janek.bevendorff@uni-weimar.de>
 */
public class MapFileGenerator extends MapFileTool
{
    private static final String[] UUID_PREFIX_OPTION  = {"prefix", "p"};
    private static final String[] INPUT_OPTION        = {"input",  "i"};
    private static final String[] INPUT_FORMAT_OPTION = {"format", "f"};
    private static final String[] OUTPUT_OPTION       = {"output", "o"};

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
            return ERROR;
        }

        final String uuidPrefix  = cmdline.getOptionValue(UUID_PREFIX_OPTION[0]);
        final String inputPath   = cmdline.getOptionValue(INPUT_OPTION[0]);
        final String inputFormat = cmdline.getOptionValue(INPUT_FORMAT_OPTION[0]);
        final String outputPath  = cmdline.getOptionValue(OUTPUT_OPTION[0]);

        if (!MapReduceClassHelper.SUPPORTED_INPUT_FORMATS.contains(inputFormat)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(this.getClass().getSimpleName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            System.err.printf("Argument error: Input format '%s' is not supported.\nSupported input formats are: %s\n",
                    inputFormat, StringUtils.join(MapReduceClassHelper.SUPPORTED_INPUT_FORMATS, ", "));
            return ERROR;
        }

        LOG.info("Tool name: " + MapFileGenerator.class.getSimpleName());
        LOG.info(" - prefix: "  + uuidPrefix);
        LOG.info(" - input: "   + inputPath);
        LOG.info(" - format: "  + inputFormat);
        LOG.info(" - output: "  + outputPath);

        final Configuration conf = getConf();
        conf.set("mapfile.uuid.prefix", uuidPrefix);

        // enable block compression
        conf.set(FileOutputFormat.COMPRESS_TYPE, "BLOCK");

        final Job job = Job.getInstance(conf);
        job.setJobName(String.format("mapfile-generator-%s", inputFormat));
        job.setJarByClass(MapFileGenerator.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        final MapReduceClassHelper classHelper = new MapReduceClassHelper(inputFormat);
        job.setInputFormatClass(classHelper.INPUT_FORMAT);
        job.setMapperClass(classHelper.MAPPER);
        job.setPartitionerClass(classHelper.PARTITIONER);
        job.setReducerClass(classHelper.REDUCER);

        LazyOutputFormat.setOutputFormatClass(job, MapFileOutputFormat.class);
        MultipleOutputs.addNamedOutput(job, MapReduceBase.DATA_OUTPUT_NAME, MapFileOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, MapReduceBase.URI_OUTPUT_NAME, MapFileOutputFormat.class, Text.class, Text.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        FileOutputFormat.setCompressOutput(job, true);

        job.waitForCompletion(true);

        return SUCCESS;
    }

    /**
     * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
     */
    public static void main(final String[] args) throws Exception
    {
        LOG.info("Running " + MapFileGenerator.class.getSimpleName() + " with args " + Arrays.toString(args));
        System.exit(ToolRunner.run(new MapFileGenerator(), args));
    }


    /**
     * Helper class for determining classes of suitable InputFormats, Mappers,
     * Reducers and Partitioners given a certain input format string.
     */
    protected static class MapReduceClassHelper
    {
        public static final List<String> SUPPORTED_INPUT_FORMATS = Arrays.asList(
                "clueweb09",
                "clueweb12",
                "commoncrawl"
        );

        public final Class<? extends InputFormat> INPUT_FORMAT;
        public final Class<? extends Mapper>      MAPPER;
        public final Class<? extends Reducer>     REDUCER;
        public final Class<? extends Partitioner> PARTITIONER;

        public MapReduceClassHelper(final String inputFormat)
        {
            switch (inputFormat) {
                case "clueweb09":
                    INPUT_FORMAT = ClueWeb09InputFormat.class;
                    MAPPER       = WarcMapper.class;
                    PARTITIONER  = WarcUUIDPartitioner.class;
                    REDUCER      = WarcReducer.class;
                    break;
                case "clueweb12":
                    INPUT_FORMAT = ClueWeb12InputFormat.class;
                    MAPPER       = WarcMapper.class;
                    PARTITIONER  = WarcUUIDPartitioner.class;
                    REDUCER      = WarcReducer.class;
                    break;
                case "commoncrawl":
                    INPUT_FORMAT = CommonCrawlInputFormat.class;
                    MAPPER       = WarcMapper.class;
                    PARTITIONER  = WarcUUIDPartitioner.class;
                    REDUCER      = WarcReducer.class;
                    break;
                default:
                    throw new RuntimeException("Unsupported input format '" + inputFormat + "'");
            }
        }
    }
}
