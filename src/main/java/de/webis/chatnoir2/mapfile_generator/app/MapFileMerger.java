/*
 * Webis MapFile Merger.
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

import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.util.ToolRunner;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * MapFile merger.
 *
 * @author Janek Bevendorff &lt;janek.bevendorff@uni-weimar.de&gt;
 */
public class MapFileMerger extends MapFileTool
{
    private static final String[] INPUT_OPTION        = {"input",  "i"};
    private static final String[] OUTPUT_OPTION       = {"output", "o"};

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


        LOG.info("Tool name: " + MapFileMerger.class.getSimpleName());
        LOG.info(" - input: "   + inputPathStr);
        LOG.info(" - output: "  + outputPathStr);

        final Configuration conf = getConf();

        final MapFile.Merger merger = new MapFile.Merger(conf);
        final FileSystem fs = FileSystem.get(conf);
        final Path inputPath = new Path(inputPathStr);
        final Path outputPath = new Path(outputPathStr);

        final FileStatus[] stat = fs.globStatus(inputPath);
        final ArrayList<Path> pathList = new ArrayList<>();
        for (final FileStatus s : stat) {
            final Path p = s.getPath();
            if (fs.isDirectory(p) && fs.exists(new Path(p.toString() + "/data"))) {
                pathList.add(p);
            }
        }

        if (pathList.isEmpty()) {
            LOG.error("No mapfiles found in path.");
            return ERROR;
        }

        merger.merge(pathList.toArray(new Path[pathList.size()]), false, outputPath);

        return SUCCESS;
    }

    /**
     * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
     */
    public static void main(final String[] args) throws Exception
    {
        LOG.info("Running " + MapFileMerger.class.getSimpleName() + " with args " + Arrays.toString(args));
        System.exit(ToolRunner.run(new MapFileMerger(), args));
    }
}
