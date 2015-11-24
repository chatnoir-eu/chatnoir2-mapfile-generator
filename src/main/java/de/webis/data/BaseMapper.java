package de.webis.data;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Abstract base class for mappers to generate MapFiles.
 *
 * @author Janek Bevendorff
 * @version 1
 */
public abstract class BaseMapper <K extends Writable, V extends Writable> extends Mapper<K, V, Text, Text>
{
    // RFC 4122 defines 6ba7b811-9dad-11d1-80b4-00c04fd430c8
    private static final byte[] UUID_NAMESPACE_URL = { 107, -89, -72, 17, -99, -83, 17, -47, -128, -76, 0, -64, 79, -44, 48, -56 };

    protected static final Logger LOG = Logger.getLogger(BaseMapper.class);

    private String mUUIDPrefix = "";

    protected String getUUIDPrefix()
    {
        return mUUIDPrefix;
    }

    /**
     * Generate a version 5 (name-based SHA1) UUID within NameSpace_URL (RFC 4122).
     * The hashed name part is prefix:internalId.
     *
     * @param prefix the scheme prefix
     * @param internalId internal ID (scheme-specific part)
     * @return generated version 5 UUID
     */
    protected String generateUUID(final String prefix, final String internalId)
    {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA1");
            md.update(UUID_NAMESPACE_URL);
            md.update((prefix + ":" + internalId).getBytes());
            final byte[] digest = md.digest();
            final byte[] shortened = new byte[16];
            System.arraycopy(digest, 0, shortened, 0, 16);

            // set version
            shortened[6] &= 0x0f;
            shortened[6] |= 0x50;

            // set variant
            shortened[8] &= 0x3f;
            shortened[8] |= 0x90;

            // format string
            final String encodedHex = Hex.encodeHexString(shortened);
            final StringBuilder strBuilder = new StringBuilder();
            for (int i = 0; i < encodedHex.length(); ++i) {
                if (8 == i || 12 == i || 16 == i || 20 == i) {
                    strBuilder.append("-");
                }
                strBuilder.append(encodedHex.charAt(i));
            }

            return strBuilder.toString();
        } catch (NoSuchAlgorithmException e) {
            LOG.error(e.getMessage());
        }
        return null;
    }

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException
    {
        super.setup(context);
        mUUIDPrefix = context.getConfiguration().get("mapfile.uuid.prefix");
    }
}