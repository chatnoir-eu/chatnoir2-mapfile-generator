package de.webis.warc;

import de.webis.data.Indexable;

import java.util.Map;
import java.util.Set;

/**
 * Public interface for WarcRecords.
 */
public interface GenericWarcRecord extends Indexable
{
    /**
     * Retrieve the total record length (header and content).
     *
     * @return total record length
     */
    int getTotalRecordLength();

    /**
     * Get the file path from this WARC file (if set).
     */
    String getWarcFilePath();

    /**
     * Get the set of metadata items from the header.
     */
    Set<Map.Entry<String, String>> getHeaderMetadata();

    /**
     * Get a value for a specific header metadata key.
     *
     * @param key key of the entry
     */
    String getHeaderMetadataItem(String key);

    /**
     * Retrieve the byte content for this record.
     */
    byte[] getByteContent();

    /**
     * Retrieve the bytes content as a UTF-8 string
     */
    String getContentUTF8();

    /**
     * Get the header record type string.
     */
    String getHeaderRecordType();

    /**
     * Get the WARC header as a string
     */
    String getHeaderString();

    /**
     * Get Warc document ID.
     */
    String getDocid();

    /**
     * Get Warc document contents.
     */
    String getContent();
}
