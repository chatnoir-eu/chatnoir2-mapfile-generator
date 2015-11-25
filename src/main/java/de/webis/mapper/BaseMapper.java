package de.webis.mapper;

import de.webis.WebisUUID;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Abstract base class for mappers to generate MapFiles.
 *
 * @author Janek Bevendorff
 * @version 1
 */
public abstract class BaseMapper <K extends Writable, V extends Writable> extends Mapper<K, V, Text, Text>
{
    protected static final Logger LOG = Logger.getLogger(BaseMapper.class);

    private String mUUIDPrefix = "";
    private WebisUUID mUUIDGenerator;

    protected String getUUIDPrefix()
    {
        return mUUIDPrefix;
    }

    protected String generateUUID(final String internalId)
    {
        return mUUIDGenerator.generateUUID(internalId);
    }

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException
    {
        super.setup(context);
        mUUIDPrefix    = context.getConfiguration().get("mapfile.uuid.prefix");
        mUUIDGenerator = new WebisUUID(getUUIDPrefix());
    }
}