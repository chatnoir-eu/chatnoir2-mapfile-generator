/*
 * Copyright (C) 2018 The Apache Software Foundation.
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

package de.webis.chatnoir2.mapfile_generator.util;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * Adjusted version of the Hadoop {@link MapFile.Merger} class that reports progress
 * of merged entries to avoid timeouts and improve job debugging.
 */
public class MapFileMerger {
    private TaskAttemptContext context;
    private Configuration conf;
    private WritableComparator comparator = null;
    private MapFile.Reader[] inReaders;
    private MapFile.Writer outWriter;
    private Class<Writable> valueClass = null;
    private Class<WritableComparable> keyClass = null;

    public enum MapFileMergeCounters {
        MAPFILES_TO_BE_MERGED,
        MAPFILE_MERGES_FINISHED,
        EMPTY_INPUT_MAPFILES,
        MAPFILE_INPUTS_EXHAUSTED,
        MAPFILE_ENTRIES_WRITTEN
    }

    private static Counter mapFilesToBeMergedCounter = null;
    private static Counter mapFileMergesFinishedCounter = null;
    private static Counter emptyInputMapFilesCounter = null;
    private static Counter mapFileInputsExhaustedCounter = null;
    private static Counter mapFileEntriesWrittenCounter = null;

    public MapFileMerger(TaskAttemptContext context) throws IOException {
        this.context = context;
        this.conf = context.getConfiguration();

        if (mapFilesToBeMergedCounter == null) {
            mapFilesToBeMergedCounter     = context.getCounter(MapFileMergeCounters.MAPFILES_TO_BE_MERGED);
            mapFileMergesFinishedCounter  = context.getCounter(MapFileMergeCounters.MAPFILE_MERGES_FINISHED);
            emptyInputMapFilesCounter     = context.getCounter(MapFileMergeCounters.EMPTY_INPUT_MAPFILES);
            mapFileInputsExhaustedCounter = context.getCounter(MapFileMergeCounters.MAPFILE_INPUTS_EXHAUSTED);
            mapFileEntriesWrittenCounter  = context.getCounter(MapFileMergeCounters.MAPFILE_ENTRIES_WRITTEN);
        }
    }

    /**
     * Merge multiple MapFiles to one Mapfile
     */
    public void merge(Path[] inMapFiles, boolean deleteInputs, Path outMapFile) throws IOException {
        try {
            open(inMapFiles, outMapFile);
            mergePass();
        } finally {
            close();
        }
        if (deleteInputs) {
            for (Path path : inMapFiles) {
                MapFile.delete(path.getFileSystem(conf), path.toString());
            }
        }
    }

    /*
     * Open all input files for reading and verify the key and value types. And
     * open Output file for writing
     */
    @SuppressWarnings("unchecked")
    private void open(Path[] inMapFiles, Path outMapFile) throws IOException {
        inReaders = new MapFile.Reader[inMapFiles.length];
        for (int i = 0; i < inMapFiles.length; i++) {
            MapFile.Reader reader = new MapFile.Reader(inMapFiles[i], conf);
            if (keyClass == null || valueClass == null) {
                keyClass = (Class<WritableComparable>) reader.getKeyClass();
                valueClass = (Class<Writable>) reader.getValueClass();
            } else if (keyClass != reader.getKeyClass()
                    || valueClass != reader.getValueClass()) {
                throw new HadoopIllegalArgumentException(
                        "Input files cannot be merged as they"
                                + " have different Key and Value classes");
            }
            inReaders[i] = reader;
        }

        if (comparator == null) {
            Class<? extends WritableComparable> cls;
            cls = keyClass.asSubclass(WritableComparable.class);
            this.comparator = WritableComparator.get(cls, conf);
        } else if (comparator.getKeyClass() != keyClass) {
            throw new HadoopIllegalArgumentException(
                    "Input files cannot be merged as they"
                            + " have different Key class compared to"
                            + " specified comparator");
        }

        outWriter = new MapFile.Writer(conf, outMapFile,
                MapFile.Writer.keyClass(keyClass),
                MapFile.Writer.valueClass(valueClass));
    }

    /**
     * Merge all input files to output map file.<br>
     * 1. Read first key/value from all input files to keys/values array. <br>
     * 2. Select the least key and corresponding value. <br>
     * 3. Write the selected key and value to output file. <br>
     * 4. Replace the already written key/value in keys/values arrays with the
     * next key/value from the selected input <br>
     * 5. Repeat step 2-4 till all keys are read. <br>
     */
    private void mergePass() throws IOException {
        // re-usable array
        WritableComparable[] keys = new WritableComparable[inReaders.length];
        Writable[] values = new Writable[inReaders.length];
        // Read first key/value from all inputs
        for (int i = 0; i < inReaders.length; i++) {
            keys[i] = ReflectionUtils.newInstance(keyClass, null);
            values[i] = ReflectionUtils.newInstance(valueClass, null);
            if (!inReaders[i].next(keys[i], values[i])) {
                // Handle empty files
                keys[i] = null;
                values[i] = null;
                emptyInputMapFilesCounter.increment(1);
            }
            mapFilesToBeMergedCounter.increment(1);
        }

        do {
            int currentEntry = -1;
            WritableComparable currentKey = null;
            Writable currentValue = null;
            for (int i = 0; i < keys.length; i++) {
                if (keys[i] == null) {
                    // Skip Readers reached EOF
                    continue;
                }
                if (currentKey == null || comparator.compare(currentKey, keys[i]) > 0) {
                    currentEntry = i;
                    currentKey = keys[i];
                    currentValue = values[i];
                }
            }
            if (currentKey == null) {
                // Merge Complete
                mapFileMergesFinishedCounter.increment(1);
                break;
            }
            // Write the selected key/value to merge stream
            outWriter.append(currentKey, currentValue);
            mapFileEntriesWrittenCounter.increment(1);
            // Replace the already written key/value in keys/values arrays with the
            // next key/value from the selected input
            if (!inReaders[currentEntry].next(keys[currentEntry], values[currentEntry])) {
                // EOF for this file
                keys[currentEntry] = null;
                values[currentEntry] = null;
                mapFileInputsExhaustedCounter.increment(1);
            }

            context.progress();
        } while (true);
    }

    private void close() throws IOException {
        for (int i = 0; i < inReaders.length; i++) {
            IOUtils.closeStream(inReaders[i]);
            inReaders[i] = null;
        }
        if (outWriter != null) {
            outWriter.close();
            outWriter = null;
        }
    }
}
