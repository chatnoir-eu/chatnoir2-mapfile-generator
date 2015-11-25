package de.webis.inputformats;

import de.webis.warc.WarcHeader;

/**
 * Input format class for ClueWeb09 WARC records
 */
public class ClueWeb09InputFormat extends ClueWebInputFormat
{
    public ClueWeb09InputFormat()
    {
        super(WarcHeader.WarcVersion.WARC018);
    }
}
