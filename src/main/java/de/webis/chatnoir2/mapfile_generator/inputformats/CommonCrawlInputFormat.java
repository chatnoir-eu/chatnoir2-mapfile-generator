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

package de.webis.chatnoir2.mapfile_generator.inputformats;

import de.webis.chatnoir2.mapfile_generator.warc.WarcHeader;

/**
 * Input format class for CommonCrawl WARC records
 */
public class CommonCrawlInputFormat extends WarcInputFormat
{
    public CommonCrawlInputFormat()
    {
        super(WarcHeader.WarcVersion.WARC10);
    }
}
