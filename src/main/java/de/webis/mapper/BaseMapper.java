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