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
 * @author Janek Bevendorff <janek.bevendorff@uni-weimar.de>
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

        if (!cmdline.hasOption(UUID_OPTION[0]) &&
                !cmdline.hasOption(PREFIX_OPTION[0]) && !cmdline.hasOption(NAME_OPTION[0])) {
            System.err.println("You need to specify either -uuid or -prefix and -id.");
            return ERROR;
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

        final UUID uuid;
        if (!uuidStr.isEmpty()) {
            uuid = UUID.fromString(uuidStr);
        } else {
            uuid = WebisUUID.generateUUID(uuidPrefix, uuidName);
        }
        final int partition = getPartition(uuid, numPartitions);
        inputPathStr = String.format("%s/%s-r-%05d", inputPathStr, MapReduceBase.DATA_OUTPUT_NAME, partition);

        final Configuration conf    = getConf();
        final Path inputPath        = new Path(inputPathStr);
        final MapFile.Reader reader = new MapFile.Reader(inputPath, conf);

        final Text entry = (Text) reader.get(new Text(uuid.toString()), new Text());
        if (null == entry) {
            System.err.printf("No record found for UUID '%s' (prefix=%s, name=%s, part=%d)%n",
                    uuid.toString(), uuidPrefix, uuidName, partition);
            return ERROR;
        }

        if (printOnlyRecord) {
            System.out.println(entry.toString());
        } else if (verbose) {
            System.out.printf("UUID=%s%nPART=%05d%nMAPFILE=%s%n%n-- RECORD BEGIN --%n%s%n-- RECORD END ---%n",
                    uuid.toString(), partition, inputPathStr, entry.toString());
        } else {
            System.out.printf("%s%n%s%n", uuid.toString(), entry.toString());
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
    protected int getPartition(final UUID uuid, final int numPartitions)
    {
        return (uuid.toString().hashCode() % numPartitions + numPartitions) % numPartitions;
    }

    public static void main(final String[] args) throws Exception
    {
        // turn off stupid INFO log messages
        Logger.getRootLogger().setLevel(Level.ERROR);
        //Logger.getLogger(org.apache.hadoop.io.compress.zlib.ZlibFactory.class).setLevel(Level.ERROR);
        //Logger.getLogger(org.apache.hadoop.io.compress.CodecPool.class).setLevel(Level.ERROR);

        // run the tool
        System.exit(ToolRunner.run(new MapFileBrowser(), args));
    }
}
