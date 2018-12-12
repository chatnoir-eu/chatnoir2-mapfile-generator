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

package de.webis.chatnoir2.mapfile_generator.warc;

import org.apache.hadoop.io.Writable;
import org.apache.xerces.impl.dv.util.Base64;
import org.mozilla.universalchardet.UniversalDetector;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Generic WARC record parser class.
 *
 * @author Janek Bevendorff
 */
public class WarcRecord implements Writable
{
    private static final String NEWLINE = "\r\n";

    private final WarcHeader mWarcHeader;
    private byte[] mBodyHeaders = null;
    private byte[] mBodyContent = null;
    private TreeMap<String, String> mHttpHeaderCache = null;

    /**
     * Sets the record ID field from the WARC header.
     */
    private String mRecordIdField = "WARC-Record-ID";

    protected WarcRecord(final WarcHeader header)
    {
        if (null == header)
            throw new IllegalArgumentException("WARC header cannot be null!");

        mWarcHeader = header;
    }

    /**
     * Our read line implementation. We cannot allow buffering here (for gzip
     * streams) so, we need to use DataInputStream. Also - we need to account
     * for java's UTF-8 implementation
     * This method is based on the implementation in ClueWeb Tools &lt;https://github.com/lintool/clueweb&gt;.
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
                char thisChar;
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
     * @return the content bytes (w/ the headerBuffer populated)
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

        // find the next WARC header
        while (!foundMark && ((line = readLineFromInputStream(in)) != null)) {
            if (line.startsWith(warcVersion.toString())) {
                foundMark = true;
            }
        }
        if (!foundMark) {
            return null;
        }

        // read WARC header block
        int contentLength = -1;
        while ((line = readLineFromInputStream(in)) != null) {
            if (line.trim().length() == 0) {
                if (contentLength < 0) {
                    // continue when we haven't found a Content-Length header yet,
                    // since we are probably dealing with not sufficiently sanitized WARC-Target-URI headers
                    continue;
                }
                // break once we reached the end of the header block
                break;
            }
            headerBuffer.append(line);
            headerBuffer.append(NEWLINE);
            String[] thisHeaderPieceParts = line.split(":", 2);
            if (thisHeaderPieceParts[0].toLowerCase().trim().equals("content-length")) {
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
            final String[] pieces = headerLine.split(":", 2);
            if (pieces[0].trim().isEmpty()) {
                continue;
            }
            if (2 != pieces.length) {
                retRecord.mWarcHeader.addHeaderMetadata(pieces[0].trim(), "");
                continue;
            }
            retRecord.mWarcHeader.addHeaderMetadata(pieces[0].trim(), pieces[1].trim());
        }

        retRecord.setContent(recordContent);
        return retRecord;
    }

    /**
     * Update ContentLength attribute of WARC header according to current byte content.
     */
    private void updateRecordContentLength()
    {
        final int headerLength = null != mBodyHeaders ? mBodyHeaders.length : 0;
        final int bodyLength   = null != mBodyContent ? mBodyContent.length : 0;
        mWarcHeader.setContentLength(headerLength + bodyLength + NEWLINE.getBytes().length);
    }

    /**
     * Serialization output.
     *
     * @param out output stream
     * @throws java.io.IOException
     */
    @Override
    public void write(final DataOutput out) throws IOException
    {
        updateRecordContentLength();
        mWarcHeader.write(out);
        out.write(mBodyHeaders);
        out.write(NEWLINE.getBytes());
        out.write(mBodyContent);
    }

    /**
     * Serialization input.
     *
     * @param in input stream
     * @throws java.io.IOException
     */
    @Override
    public void readFields(final DataInput in) throws IOException
    {
        mWarcHeader.readFields(in);
        final byte[] b = new byte[mWarcHeader.getContentLength()];
        in.readFully(b);
        setContent(b);
    }

    /**
     * Get record ID.
     * If no other field has been specified using {@link #setRecordIdField(String)},
     * the Warc-Record-ID header will be used by default. If that header
     * does not exist, a random UUID is generated and returned as a URN in the
     * form &lt;urn:uuid:RANDOM_UUID&gt;.
     *
     * @return record id
     */
    public String getRecordId()
    {
        final String id = mWarcHeader.getHeaderMetadataItem(mRecordIdField);
        return null != id ? id : "<urn:uuid:" + UUID.randomUUID() + ">";
    }

    /**
     * Change the record ID header field. The default is "WARC-Record-ID".
     *
     * @param idField new field name, null to reset to default
     */
    public void setRecordIdField(final String idField)
    {
        mRecordIdField = null != idField ? idField : "WARC-Record-ID";
    }

    /**
     * Get type of WARC record, i.e. the value of WARC-Type.
     * Allowed values according to the specification are 'warcinfo', 'response', 'resource',
     * 'request', 'metadata', 'revisit', 'conversion', and 'continuation'.
     *
     * @return WARC record type, null if unknown
     */
    public String getRecordType()
    {
        return mWarcHeader.getHeaderMetadataItem("WARC-Type");
    }

    /**
     * Set byte content for this record (including HTTP headers).
     *
     * @param c record content as byte array
     */
    public void setContent(final byte[] c)
    {
        final String warcContentType = mWarcHeader.getHeaderMetadata().get("Content-Type");
        if (null != warcContentType) {
            String[] parts = warcContentType.split(";");
            if (parts.length < 2 || !parts[0].trim().equals("application/http") || !parts[1].trim().equals("msgtype=response")) {
                // don't try to split off headers if this is no HTTP response record
                mBodyContent = c;
            } else {
                int headerEnd = 0;
                int bodyStart = 0;
                final byte[] crlf = {13, 10};

                // check for double CRLF or double LF to mark end of HTTP header section
                for (int i = 3; i < c.length; ++i) {
                    bodyStart = i + 1;
                    if (c[i - 3] == crlf[0] && c[i - 2] == crlf[1] && c[i - 1] == crlf[0] && c[i] == crlf[1]) {
                        headerEnd = i - 1;
                        break;
                    }
                    if (c[i - 1] == crlf[1] && c[i] == crlf[1]) {
                        headerEnd = i;
                        break;
                    }
                }

                mBodyHeaders = new byte[headerEnd];
                mBodyContent = new byte[c.length - bodyStart];
                System.arraycopy(c, 0, mBodyHeaders, 0, mBodyHeaders.length);
                System.arraycopy(c, bodyStart, mBodyContent, 0, mBodyContent.length);
            }
        } else {
            mBodyContent = c;
        }

        mHttpHeaderCache = null;
        updateRecordContentLength();
    }

    /**
     * Set String content for this record.
     *
     * @param c record content as String
     */
    public void setContent(final String c)
    {
        setContent(c.getBytes());
    }

    /**
     * Get raw byte content of this record (body part only).
     */
    public byte[] getByteContent()
    {
        return mBodyContent;
    }

    /**
     * Get body part of content as String.
     * The original encoding will automatically be deduced.
     *
     * @return UTF-8-encoded body as String, base64-encoded String if body is binary
     */
    public String getContent()
    {
        return getContent(getContentEncoding());
    }

    /**
     * Get body part of content as String with the given encoding.
     *
     * @param encoding encoding to apply, null for binary
     * @return UTF-8-encoded body as String, base64-encoded String if body is binary
     */
    public String getContent(String encoding)
    {
        if (null == mBodyContent || mBodyContent.length == 0) {
            return "";
        }

        if (null != encoding) {
            try {
                return new String(mBodyContent, encoding);
            } catch (UnsupportedEncodingException ignored) {}
        }

        return Base64.encode(mBodyContent);
    }

    /**
     * Get header part of content.
     *
     * @return UTF-8-encoded header part of content
     */
    public String getContentHeaderString()
    {
        if (null == mBodyHeaders) {
            return "";
        }

        return new String(mBodyHeaders, StandardCharsets.US_ASCII);
    }

    /**
     * Get parsed content headers.
     *
     * @return Case-insensitive TreeMap of HTTP headers
     */
    public TreeMap<String, String> getContentHeaders()
    {
        if (null == mHttpHeaderCache) {
            mHttpHeaderCache           = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            final String headerString  = getContentHeaderString();
            final String[] headerLines = headerString.split("\\r?\\n");

            for (final String headerLine : headerLines) {
                String[] pieces = headerLine.split(":", 2);
                if (pieces[0].trim().isEmpty()) {
                    continue;
                }

                String key = pieces[0].trim();
                String value = "";
                if (2 != pieces.length) {
                    if (key.startsWith("HTTP/1.")) {
                        value = key;
                        key = "__HTTP_STATUS__";
                    }
                } else {
                    value = pieces[1].trim();
                }

                mHttpHeaderCache.put(key, value);
            }
        }

        return mHttpHeaderCache;
    }

    /**
     * Determine encoding of raw byte content.
     *
     * @return Encoding String identifier (e.g. UTF-8, ISO-8859-1, ...), null if unknown/binary content
     */
    public String getContentEncoding()
    {
        if (null == mBodyContent) {
            return null;
        }

        final String warcType = mWarcHeader.getHeaderMetadataItem("WARC-Type");
        if (null != warcType && warcType.equals("request")) {
            return "US-ASCII";
        }

        final String contentType = getContentHeaders().get("Content-Type");
        if (null != contentType) {
            final String[] parts = contentType.split(";");
            for (int i = 1; i < parts.length; ++i) {
                final int pos = parts[i].indexOf("charset=");
                if (-1 != pos) {
                    return parts[i].substring(pos + 8).trim().toUpperCase();
                }
            }
        }

        // if no charset header found, try to detect encoding heuristically
        UniversalDetector detector = new UniversalDetector(null);
        detector.handleData(mBodyContent, 0, mBodyContent.length);
        detector.dataEnd();
        final String encoding = detector.getDetectedCharset();
        detector.reset();
        if (null != encoding) {
            return encoding;
        }

        // if we still have no definite encoding, check if the first 512 bytes contain non-printable data
        final int length = Math.min(512, mBodyContent.length);
        int binaryCounter = 0;
        for (int i = 0; i < length; ++i) {
            if ((mBodyContent[i] >= (byte) 0x00 && mBodyContent[i] <= (byte) 0x08) ||
                    (mBodyContent[i] >= (byte) 0x0e && mBodyContent[i] <= (byte) 0x1f) ||
                    (mBodyContent[i] >= (byte) 0x80 && mBodyContent[i] <= (byte) 0x9f)) {
                ++binaryCounter;
            }
            if (binaryCounter > 5) {
                // declare as binary if more than 5 non-printable characters found
                return null;
            }
        }

        // if content doesn't seem to be binary, assume ISO-8859-1 which is the default encoding for HTTP/1.1
        return "ISO-8859-1";
    }

    /**
     * Get WARC header.
     *
     * @return {@link WarcHeader} instance for this record
     */
    public WarcHeader getHeader()
    {
        return mWarcHeader;
    }

    @Override
    public String toString()
    {
        return getContentHeaderString() + NEWLINE + getContent();
    }
}