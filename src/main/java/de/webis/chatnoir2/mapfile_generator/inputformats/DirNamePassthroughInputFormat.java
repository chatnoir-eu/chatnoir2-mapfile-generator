/*
 * Webis MapFile Generator.
 * Copyright (C) 2018 Janek Bevendorff, Webis Group
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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Input format that only passes through directory names.
 */
public class DirNamePassthroughInputFormat extends FileInputFormat<Text, NullWritable>
{
    @Override
    public RecordReader<Text, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
    {
        return new PassthroughRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename)
    {
        return false;
    }

    public class PassthroughRecordReader extends RecordReader<Text, NullWritable>
    {
        private boolean mIsRead = false;
        private FileSplit mSplit = null;
        private TaskAttemptContext mContext = null;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
        {
            mIsRead = false;
            mSplit = (FileSplit) split;
            mContext = context;
        }

        @Override
        public boolean nextKeyValue()
        {
            if (!mIsRead) {
                mIsRead = true;
                return true;
            }
            return false;
        }

        @Override
        public Text getCurrentKey() throws IOException {
            Path path = mSplit.getPath();
            if (path.getFileSystem(mContext.getConfiguration()).isFile(path)) {
                path = path.getParent();
            }
            return new Text(path.toString());
        }

        @Override
        public NullWritable getCurrentValue()
        {
            return NullWritable.get();
        }

        @Override
        public float getProgress()
        {
            return mIsRead ? 1.0f : 0.0f;
        }

        @Override
        public void close()
        {
        }
    }
}
