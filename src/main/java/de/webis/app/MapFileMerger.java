/*
 * Webis MapFile merger.
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
 * @author Janek Bevendorff <janek.bevendorff@uni-weimar.de>
 * @version 1
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
            return 1;
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
        for (final FileStatus aStat : stat) {
            final Path p = aStat.getPath();
            if (fs.isDirectory(p) && fs.exists(new Path(p.toString() + "/data"))) {
                pathList.add(p);
            }
        }

        if (pathList.isEmpty()) {
            LOG.error("No mapfiles found in path.");
            return 1;
        }

        merger.merge(pathList.toArray(new Path[pathList.size()]), false, outputPath);

        return 0;
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
