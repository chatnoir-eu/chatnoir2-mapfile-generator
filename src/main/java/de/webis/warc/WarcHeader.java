/*
 * Webis MapFile generator.
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

package de.webis.warc;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * WARC header class.
 *
 * @author Janek Bevendorff
 */
public class WarcHeader implements Writable
{
    public enum WarcVersion {
        WARC10 {
            public String toString() {
                return "WARC/1.0";
            }
        },
        WARC018 {
            public String toString() {
                return "WARC/0.18";
            }
        }
    }

    private static final String NEWLINE = "\r\n";

    private final WarcVersion mVersion;
    private int mContentLength = 0;
    private final TreeMap<String, String> mMetadata = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public WarcHeader(final WarcVersion version)
    {
        this(version, 0);
    }

    public WarcHeader(final WarcVersion version, final int contentLength)
    {
        mVersion       = version;
        mContentLength = contentLength;
    }

    public WarcHeader(final WarcHeader header) {
        mVersion       = header.mVersion;
        mContentLength = header.mContentLength;
        mMetadata.putAll(header.mMetadata);
    }

    /**
     * Serialization output.
     *
     * @param out the data output stream
     * @throws java.io.IOException
     */
    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeInt(mMetadata.size());
        for (Map.Entry<String, String> thisEntry : mMetadata.entrySet()) {
            out.writeUTF(thisEntry.getKey());
            out.writeUTF(thisEntry.getValue());
        }
        out.writeInt(mContentLength);
    }

    /**
     * Serialization input.
     *
     * @param in the data input stream
     * @throws java.io.IOException
     */
    @Override
    public void readFields(final DataInput in) throws IOException {
        mMetadata.clear();
        int numMetaItems = in.readInt();
        for (int i = 0; i < numMetaItems; ++i) {
            String thisKey   = in.readUTF();
            String thisValue = in.readUTF();
            mMetadata.put(thisKey, thisValue);
        }
        mContentLength = in.readInt();
    }

    /**
     * Set WARC content length.
     *
     * @param length content length
     */
    public void setContentLength(final int length)
    {
        mContentLength = length;
    }

    /**
     * Set WARC content length.
     *
     * @return content length
     */
    public int getContentLength()
    {
        return mContentLength;
    }

    /**
     * Add key-value pair of metadata to header.
     *
     * @param key metadata key
     * @param value metadata value
     */
    public void addHeaderMetadata(final String key, final String value)
    {
        mMetadata.put(key, value);
    }

    /**
     * Clear all metadata from header.
     */
    public void clearHeaderMetadata()
    {
        mMetadata.clear();
    }

    /**
     * Get WARC headers as case-insensitive TreeMap.
     *
     * @return Headers as Key->Value TreeMap
     */
    public TreeMap<String, String> getHeaderMetadata()
    {
        return mMetadata;
    }

    /**
     * Get a specific Entry from header metadata.
     *
     * @param key the item
     * @return header value
     */
    public String getHeaderMetadataItem(final String key) {
        return mMetadata.get(key);
    }

    /**
     * Get WARC version.
     *
     * @return WARC version
     */
    public WarcVersion getWarcVersion()
    {
        return mVersion;
    }

    @Override
    public String toString() {
        final StringBuilder retBuffer = new StringBuilder();
        retBuffer.append(mVersion).append(NEWLINE);
        for (Map.Entry<String, String> thisEntry : mMetadata.entrySet()) {
            retBuffer.append(thisEntry.getKey()).
                    append(": ").
                    append(thisEntry.getValue()).
                    append(NEWLINE);
        }
        return retBuffer.toString();
    }
}
