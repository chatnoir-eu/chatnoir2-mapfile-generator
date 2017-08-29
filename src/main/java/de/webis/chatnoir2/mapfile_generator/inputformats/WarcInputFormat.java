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
package de.webis.chatnoir2.mapfile_generator.inputformats;

import java.io.DataInputStream;
import java.io.IOException;

import de.webis.chatnoir2.mapfile_generator.warc.WarcHeader;
import de.webis.chatnoir2.mapfile_generator.warc.WarcRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public abstract class WarcInputFormat extends FileInputFormat<LongWritable, WarcRecord>
{
    private final WarcHeader.WarcVersion mWarcVersion;
    private String mWarcRecordIdField = null;

    /**
     * Constructor.
     *
     * @param warcVersion version number of the WARC file to read
     */
    public WarcInputFormat(final WarcHeader.WarcVersion warcVersion)
    {
        mWarcVersion = warcVersion;
    }

    /**
     * Constructor.
     *
     * @param warcVersion version number of the WARC file to read
     * @param warcRecordIdField Custom record ID header field
     */
    public WarcInputFormat(final WarcHeader.WarcVersion warcVersion, final String warcRecordIdField)
    {
        this(warcVersion);
        mWarcRecordIdField = warcRecordIdField;
    }

    @Override
    public RecordReader<LongWritable, WarcRecord> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException
    {
        return new WarcRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename)
    {
        return false;
    }

    public class WarcRecordReader extends RecordReader<LongWritable, WarcRecord>
    {
        private CompressionCodecFactory compressionCodecs = null;
        private long start;
        private long pos;
        private long end;
        private LongWritable key = null;
        private WarcRecord value = null;
        private Seekable filePosition;
        private CompressionCodec codec;
        private Decompressor decompressor;
        private DataInputStream in;

        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException
        {
            FileSplit split = (FileSplit) genericSplit;
            Configuration job = context.getConfiguration();
            start = split.getStart();
            end = start + split.getLength();
            final Path file = split.getPath();
            compressionCodecs = new CompressionCodecFactory(job);
            codec = compressionCodecs.getCodec(file);

            // open the file and seek to the start of the split
            FileSystem fs = file.getFileSystem(job);
            FSDataInputStream fileIn = fs.open(split.getPath());

            if (isCompressedInput()) {
                in = new DataInputStream(codec.createInputStream(fileIn, decompressor));
                filePosition = fileIn;
            } else {
                fileIn.seek(start);
                in = fileIn;
                filePosition = fileIn;
            }

            this.pos = start;
        }

        private boolean isCompressedInput()
        {
            return (codec != null);
        }

        private long getFilePosition() throws IOException
        {
            long retVal;
            if (isCompressedInput() && null != filePosition) {
                retVal = filePosition.getPos();
            } else {
                retVal = pos;
            }
            return retVal;
        }

        public boolean nextKeyValue() throws IOException
        {
            if (key == null) {
                key = new LongWritable();
            }
            key.set(pos);
            value = WarcRecord.readNextWarcRecord(in, mWarcVersion);
            if (null != mWarcRecordIdField && null != value) {
                value.setRecordIdField(mWarcRecordIdField);
            }
            return value != null;
        }

        @Override
        public LongWritable getCurrentKey()
        {
            return key;
        }

        @Override
        public WarcRecord getCurrentValue()
        {
            return value;
        }

        /**
         * Get the progress within the split
         */
        public float getProgress() throws IOException
        {
            if (start == end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (getFilePosition() - start) / (float) (end - start));
            }
        }

        public synchronized void close() throws IOException
        {
            try {
                if (in != null) {
                    in.close();
                }
            } finally {
                if (decompressor != null) {
                    CodecPool.returnDecompressor(decompressor);
                }
            }
        }
    }
}
