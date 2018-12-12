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

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Base class for MapFile generation tools
 *
 * @author Janek Bevendorff
 */
public abstract class MapFileTool extends Configured implements Tool
{
    protected static final Logger LOG = Logger.getLogger(MapFileGenerator.class);

    /**
     * Success return code.
     */
    protected static final int SUCCESS = 0;

    /**
     * Error return code.
     */
    protected static final int ERROR = 1;

    /**
     * Parse command line arguments
     *
     * @param options pre-defined options
     * @param args given args
     * @return parse CommandLine, may be null in case of invalid arguments
     */
    protected CommandLine parseCmdline(final Options options, final String[] args)
    {
        final CommandLineParser parser = new GnuParser();
        CommandLine cmdline;
        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(this.getClass().getSimpleName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            System.err.println("Error parsing command line: " + exp.getMessage());
            return null;
        }

        return cmdline;
    }

    /**
     * Actual run implementation. Must be provided.
     *
     * @param args given cmd args
     * @return exit code (SUCCESS | ERROR)
     * @throws Exception
     */
    @Override
    public abstract int run(final String[] args) throws Exception;
}
