package de.webis.inputformats;

import de.webis.warc.WarcHeader;

/**
 * Input format class for ClueWeb12 WARC records
 */
public class ClueWeb12InputFormat extends ClueWebInputFormat
{
    public ClueWeb12InputFormat()
    {
        super(WarcHeader.WarcVersion.WARC10);
    }
}
