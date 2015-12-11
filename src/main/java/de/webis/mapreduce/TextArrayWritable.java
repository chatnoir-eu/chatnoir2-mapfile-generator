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

package de.webis.mapreduce;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Wrapper class for ArrayWritable to enable default-construction.
 *
 * @author Janek Bevendorff
 */
public class TextArrayWritable extends ArrayWritable
{
    public TextArrayWritable()
    {
        super(Text.class);
    }

    public TextArrayWritable(final Text[] texts)
    {
        super(Text.class, texts);
    }

    @Override
    public Text[] get()
    {
        final Writable[] writables = super.get();
        final Text[] texts         = new Text[writables.length];

        for (int i = 0; i < writables.length; ++i) {
            texts[i] = (Text) writables[i];
        }

        return texts;
    }
}