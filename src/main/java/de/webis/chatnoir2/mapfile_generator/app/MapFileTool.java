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
