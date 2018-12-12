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
public class DirnamePassthroughInputFormat extends FileInputFormat<Text, NullWritable>
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
        private Path mPath = null;
        private TaskAttemptContext mContext = null;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
        {
            mPath = ((FileSplit) split).getPath();
            mContext = context;
        }

        @Override
        public boolean nextKeyValue()
        {
            return mPath != null;
        }

        @Override
        public Text getCurrentKey() throws IOException {
            if (mPath.getFileSystem(mContext.getConfiguration()).isFile(mPath)) {
                mPath = mPath.getParent();
            }
            Path path = mPath;
            mPath = null;
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
            return mPath == null ? 1.0f : 0.0f;
        }

        @Override
        public void close()
        {
        }
    }
}
