/*
 * Webis MapFile Browser.
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

import de.webis.WebisUUID;
import de.webis.chatnoir2.mapfile_generator.mapreduce.MapReduceBase;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.UUID;

/**
 * MapFile browser.
 *
 * @author Janek Bevendorff &lt;janek.bevendorff@uni-weimar.de&gt;
 */
public class MapFileBrowser extends MapFileTool
{
    private static final String[] INPUT_OPTION       = {"input",       "i"};
    private static final String[] PARITIONS_OPTION   = {"partitions",  "k"};
    private static final String[] PREFIX_OPTION      = {"prefix",      "p"};
    private static final String[] NAME_OPTION        = {"name",        "n"};
    private static final String[] UUID_OPTION        = {"uuid",        "u"};
    private static final String[] RECORD_ONLY_OPTION = {"record-only", "r"};
    private static final String[] VERBOSE_OPTION     = {"verbose",     "v"};
    private static final String[] URI_OPTION         = {"uri",         "l"};

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
                withArgName("NUM").
                hasArg().
                withLongOpt(PARITIONS_OPTION[0]).
                withDescription("Number of partitions").
                isRequired().
                create(PARITIONS_OPTION[1]));
        options.addOption(OptionBuilder.
                withArgName("PREFIX").
                hasArg().
                withLongOpt(PREFIX_OPTION[0]).
                withDescription("UUID name prefix (required if -uuid is not set)").
                create(PREFIX_OPTION[1]));
        options.addOption(OptionBuilder.
                withArgName("ID").
                hasArg().
                withLongOpt(NAME_OPTION[0]).
                withDescription("Internal record ID (required if -uuid is not set)").
                create(NAME_OPTION[1]));
        options.addOption(OptionBuilder.
                withArgName("UUID").
                hasArg().
                withLongOpt(UUID_OPTION[0]).
                withDescription("UUID of the record").
                create(UUID_OPTION[1]));
        options.addOption(OptionBuilder.
                withLongOpt(URI_OPTION[0]).
                withDescription("Retrieve from URI MapFile instead of the data MapFile").
                create(URI_OPTION[1]));
        options.addOption(OptionBuilder.
                withLongOpt(RECORD_ONLY_OPTION[0]).
                withDescription("Print only record, not UUID").
                create(RECORD_ONLY_OPTION[1]));
        options.addOption(OptionBuilder.
                withLongOpt(VERBOSE_OPTION[0]).
                withDescription("Verbose output").
                create(VERBOSE_OPTION[1]));


        final CommandLine cmdline = parseCmdline(options, args);
        if (null == cmdline) {
            return ERROR;
        }

        if (!cmdline.hasOption(UUID_OPTION[0]) && !cmdline.hasOption(URI_OPTION[0]) &&
                !cmdline.hasOption(PREFIX_OPTION[0]) && !cmdline.hasOption(NAME_OPTION[0])) {
            System.err.println("You need to specify either -uuid or -prefix and -id.");
            return ERROR;
        }
        if (cmdline.hasOption(URI_OPTION[0]) && !cmdline.hasOption(NAME_OPTION[0])) {
            System.err.println("You need to specify -name when -uri is set.");
            return ERROR;
        }
        if (cmdline.hasOption(URI_OPTION[0]) && cmdline.hasOption(PREFIX_OPTION[0])) {
            System.err.println("WARNING: -uri given, ignoring -prefix.");
        }
        if (cmdline.hasOption(UUID_OPTION[0]) &&
                (cmdline.hasOption(PREFIX_OPTION[0]) || cmdline.hasOption(NAME_OPTION[0]))) {
            System.err.println("WARNING: -uuid given, ignoring -prefix and -id.");
        }

        String inputPathStr           = cmdline.getOptionValue(INPUT_OPTION[0]);
        final int numPartitions       = Integer.parseInt(cmdline.getOptionValue(PARITIONS_OPTION[0]));
        final String uuidPrefix       = cmdline.getOptionValue(PREFIX_OPTION[0]);
        final String uuidName         = cmdline.getOptionValue(NAME_OPTION[0]);
        final String uuidStr          = cmdline.hasOption(UUID_OPTION[0]) ? cmdline.getOptionValue(UUID_OPTION[0]) : "";
        final boolean printOnlyRecord = cmdline.hasOption(RECORD_ONLY_OPTION[0]);
        final boolean verbose         = cmdline.hasOption(VERBOSE_OPTION[0]);

        final String recordId;
        if (cmdline.hasOption(URI_OPTION[0])) {
            recordId = uuidName;
        } else if (!uuidStr.isEmpty()) {
            recordId = uuidStr;
        } else {
            recordId = WebisUUID.generateUUID(uuidPrefix, uuidName).toString();
        }
        final int partition = getPartition(recordId, numPartitions);
        String mapfile = cmdline.hasOption(URI_OPTION[0]) ? MapReduceBase.URI_OUTPUT_NAME : MapReduceBase.DATA_OUTPUT_NAME;
        inputPathStr = String.format("%s/%s-r-%05d", inputPathStr, mapfile, partition);

        final Configuration conf    = getConf();
        final Path inputPath        = new Path(inputPathStr);
        final MapFile.Reader reader = new MapFile.Reader(inputPath, conf);

        final Text entry = (Text) reader.get(new Text(recordId), new Text());
        if (null == entry) {
            System.err.printf("No record found for UUID '%s' (prefix=%s, name=%s, part=%d)%n",
                    recordId, uuidPrefix, uuidName, partition);
            return ERROR;
        }

        if (printOnlyRecord) {
            System.out.println(entry.toString());
        } else if (verbose) {
            System.out.printf("UUID=%s%nPART=%05d%nMAPFILE=%s%n%n--- RECORD BEGIN ---%n%s%n--- RECORD END ---%n",
                    recordId, partition, inputPathStr, entry.toString());
        } else {
            System.out.printf("%s%n%s%n", recordId, entry.toString());
        }

        return SUCCESS;
    }

    /**
     * Get MapFile partition number.
     *
     * @param uuid UUID key
     * @param numPartitions total number of partitions
     * @return calculated partition number
     */
    private int getPartition(final String uuid, final int numPartitions)
    {
        return (uuid.hashCode() % numPartitions + numPartitions) % numPartitions;
    }

    public static void main(final String[] args) throws Exception
    {
        // turn off stupid INFO log messages
        Logger.getRootLogger().setLevel(Level.ERROR);

        // run the tool
        System.exit(ToolRunner.run(new MapFileBrowser(), args));
    }
}
