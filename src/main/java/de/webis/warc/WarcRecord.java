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

import java.io.*;
import java.util.HashMap;

/**
 * ClueWeb WARC record class.
 *
 * This class is partially based on ClueWeb Tools <https://github.com/lintool/clueweb>.
 */
public class WarcRecord implements Writable
{
    private static String NEWLINE = "\n";

    private final WarcHeader mWarcHeader;
    private byte[] mWarcContent = null;

    protected WarcRecord(final WarcHeader header)
    {
        mWarcHeader = header;
    }

    /**
     * Our read line implementation. We cannot allow buffering here (for gzip
     * streams) so, we need to use DataInputStream. Also - we need to account
     * for java's UTF8 implementation
     *
     * @param in the input data stream
     * @return the read line (or null if eof)
     * @throws java.io.IOException
     */
    private static String readLineFromInputStream(final DataInputStream in) throws IOException
    {
        final byte MASK_THREE_BYTE_CHAR  = (byte) (0xE0);
        final byte MASK_TWO_BYTE_CHAR    = (byte) (0xC0);
        final byte MASK_TOPMOST_BIT      = (byte) (0x80);
        final byte MASK_BOTTOM_SIX_BITS  = (byte) (0x1F);
        final byte MASK_BOTTOM_FIVE_BITS = (byte) (0x3F);
        final byte MASK_BOTTOM_FOUR_BITS = (byte) (0x0F);

        final StringBuilder retString = new StringBuilder();
        boolean keepReading = true;
        try {
            do {
                char thisChar = 0;
                byte readByte = in.readByte();
                // check to see if it's a multibyte character
                if ((readByte & MASK_THREE_BYTE_CHAR) == MASK_THREE_BYTE_CHAR) {
                    // need to read the next 2 bytes
                    if (in.available() < 2) {
                        // treat these all as individual characters
                        retString.append((char) readByte);
                        int numAvailable = in.available();
                        for (int i = 0; i < numAvailable; i++) {
                            retString.append((char) (in.readByte()));
                        }
                        continue;
                    }
                    byte secondByte = in.readByte();
                    byte thirdByte = in.readByte();
                    // ensure the topmost bit is set
                    if (((secondByte & MASK_TOPMOST_BIT) != MASK_TOPMOST_BIT)
                            || ((thirdByte & MASK_TOPMOST_BIT) != MASK_TOPMOST_BIT)) {
                        // treat these as individual characters
                        retString.append((char) readByte);
                        retString.append((char) secondByte);
                        retString.append((char) thirdByte);
                        continue;
                    }
                    int finalVal = (thirdByte & MASK_BOTTOM_FIVE_BITS) + 64
                            * (secondByte & MASK_BOTTOM_FIVE_BITS) + 4096
                            * (readByte & MASK_BOTTOM_FOUR_BITS);
                    thisChar = (char) finalVal;
                } else if ((readByte & MASK_TWO_BYTE_CHAR) == MASK_TWO_BYTE_CHAR) {
                    // need to read next byte
                    if (in.available() < 1) {
                        // treat this as individual characters
                        retString.append((char) readByte);
                        continue;
                    }
                    byte secondByte = in.readByte();
                    if ((secondByte & MASK_TOPMOST_BIT) != MASK_TOPMOST_BIT) {
                        retString.append((char) readByte);
                        retString.append((char) secondByte);
                        continue;
                    }
                    int finalVal = (secondByte & MASK_BOTTOM_FIVE_BITS) + 64
                            * (readByte & MASK_BOTTOM_SIX_BITS);
                    thisChar = (char) finalVal;
                } else {
                    // interpret it as a single byte
                    thisChar = (char) readByte;
                }
                if (thisChar == '\n') {
                    keepReading = false;
                } else {
                    retString.append(thisChar);
                }
            } while (keepReading);
        } catch (EOFException eofEx) {
            return null;
        }
        if (retString.length() == 0) {
            return "";
        }
        return retString.toString();
    }

    /**
     * The actual heavy lifting of reading in the next WARC record.
     *
     * @param in           the data input stream
     * @param headerBuffer a blank string buffer to contain the WARC header
     * @param warcVersion  WARC version
     * @return the content byts (w/ the headerBuffer populated)
     * @throws java.io.IOException
     */
    private static byte[] readNextRecord(final DataInputStream in, final StringBuilder headerBuffer,
                                         final WarcHeader.WarcVersion warcVersion) throws IOException
    {
        if (in == null) {
            return null;
        }
        if (headerBuffer == null) {
            return null;
        }
        String line;
        boolean foundMark = false;
        byte[] retContent;

        // cannot be using a buffered reader here!!!!
        // just read the header
        // first - find our WARC header
        while ((!foundMark) && ((line = readLineFromInputStream(in)) != null)) {
            if (line.startsWith(warcVersion.toString())) {
                foundMark = true;
            }
        }
        // no WARC mark?
        if (!foundMark) {
            return null;
        }

        // then read to the first newline
        // make sure we get the content length here
        int contentLength = -1;
        boolean foundContentLength = false;
        while (!foundContentLength && ((line = readLineFromInputStream(in)) != null) && line.trim().length() != 0) {
            headerBuffer.append(line);
            headerBuffer.append(NEWLINE);
            String[] thisHeaderPieceParts = line.split(":", 2);
            if (thisHeaderPieceParts[0].toLowerCase().trim().equals("content-length")) {
                foundContentLength = true;
                try {
                    if (2 == thisHeaderPieceParts.length) {
                        contentLength = Integer.parseInt(thisHeaderPieceParts[1].trim());
                    } else {
                        contentLength = -1;
                    }
                } catch (NumberFormatException nfEx) {
                    contentLength = -1;
                }
            }
        }

        if (contentLength < 0) {
            return null;
        }
        // now read the bytes of the content
        retContent = new byte[contentLength];
        int totalWant = contentLength;
        int totalRead = 0;
        while (totalRead < contentLength) {
            try {
                int numRead = in.read(retContent, totalRead, totalWant);
                if (numRead < 0) {
                    return null;
                } else {
                    totalRead += numRead;
                    totalWant = contentLength - totalRead;
                }
            } catch (EOFException eofEx) {
                // resize to what we have
                if (totalRead > 0) {
                    byte[] newReturn = new byte[totalRead];
                    System.arraycopy(retContent, 0, newReturn, 0, totalRead);
                    return newReturn;
                } else {
                    return null;
                }
            }
        }

        return retContent;
    }

    /**
     * Read in a WARC record from a data input stream.
     *
     * @param in the input stream
     * @param warcVersion WARC version
     * @return a WARC record (or null if eof)
     * @throws java.io.IOException
     */
    public static WarcRecord readNextWarcRecord(final DataInputStream in, final WarcHeader.WarcVersion warcVersion) throws IOException
    {
        final StringBuilder recordHeader = new StringBuilder();
        final byte[] recordContent = readNextRecord(in, recordHeader, warcVersion);

        if (recordContent == null) {
            return null;
        }

        // extract out our header information
        final String thisHeaderString = recordHeader.toString();
        final String[] headerLines = thisHeaderString.split(NEWLINE);

        final WarcRecord retRecord = new WarcRecord(new WarcHeader(warcVersion));
        retRecord.mWarcHeader.setContentLength(recordContent.length);
        for (final String headerLine : headerLines) {
            String[] pieces = headerLine.split(":", 2);
            if (2 != pieces.length) {
                retRecord.mWarcHeader.addHeaderMetadata(pieces[0], "");
                continue;
            }
            String thisKey = pieces[0].trim();
            String thisValue = pieces[1].trim();

            retRecord.mWarcHeader.addHeaderMetadata(thisKey, thisValue);
        }

        retRecord.setContent(recordContent);
        return retRecord;
    }

    /**
     * Serialization output.
     *
     * @param out output stream
     * @throws java.io.IOException
     */
    @Override
    public void write(final DataOutput out) throws IOException {
        mWarcHeader.write(out);
        out.write(mWarcContent);
    }
    /**
     * Serialization input.
     *
     * @param in input stream
     * @throws java.io.IOException
     */
    @Override
    public void readFields(final DataInput in) throws IOException {
        mWarcHeader.readFields(in);
        mWarcContent = new byte[mWarcHeader.getContentLength()];
        in.readFully(mWarcContent);
    }

    /**
     * Get record TREC ID.
     *
     * @return WARC TREC ID (may be null if record is not a valid WARC document)
     */
    public String getDocid() {
        return mWarcHeader.getHeaderMetadata().get("WARC-TREC-ID");
    }

    /**
     * Set byte content for this record.
     *
     * @param content record content
     */
    public void setContent(final byte[] content) {
        mWarcContent = content;
    }
    /**
     * Set String content for this record.
     *
     * @param content record content
     */
    public void setContent(final String content) {
        setContent(content.getBytes());
    }

    /**
     * Get raw byte content of this record.
     */
    public byte[] getByteContent() {
        return mWarcContent;
    }

    /**
     * Get content of this record as UTF-8-encoded string.
     *
     * @return full content.
     */
    public String getFullContentString() {
        String retString;
        try {
            retString = new String(mWarcContent, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            retString = new String(mWarcContent);
        }
        return retString;
    }

    /**
     * Get body part of content.
     *
     * @return UTF-8-encoded body part of content
     */
    public String getContent() {
        final String str = getFullContentString();
        int i = str.indexOf(NEWLINE + NEWLINE);

        return (-1 != i) ? str.substring(i + 1) : "";
    }

    /**
     * Get header part of content.
     *
     * @return UTF-8-encoded body part of content
     */
    public String getContentHeaderString() {
        final String str = getFullContentString();
        int i = str.indexOf(NEWLINE + NEWLINE);

        return (-1 != i) ? str.substring(0, i) : "";
    }

    /**
     * Get content headers as Entry Set
     *
     * @return UTF-8-encoded body part of content
     */
    public HashMap<String, String> getContentHeaders() {
        final String headerString  = getContentHeaderString();
        final String[] headerLines = headerString.split(NEWLINE);

        final HashMap<String, String> headers = new HashMap<>();
        for (final String headerLine : headerLines) {
            String[] pieces = headerLine.split(":", 2);
            if (pieces[0].trim().isEmpty()) {
                continue;
            }

            if (2 != pieces.length) {
                headers.put(pieces[0].trim(), "");
                continue;
            }
            headers.put(pieces[0].trim(), pieces[1].trim());
        }

        return headers;
    }

    /**
     * Get WARC header.
     */
    public WarcHeader getHeader()
    {
        return mWarcHeader;
    }

    @Override
    public String toString() {
        return getFullContentString();
    }
}