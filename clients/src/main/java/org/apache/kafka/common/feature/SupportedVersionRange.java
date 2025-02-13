/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.feature;

import java.util.Map;

/**
 * An extended {@link BaseVersionRange} representing the min/max versions for a supported feature.
 */
public class SupportedVersionRange extends BaseVersionRange {
    // Label for the min version key, that's used only to convert to/from a map.
    private static final String MIN_VERSION_KEY_LABEL = "min_version";

    // Label for the max version key, that's used only to convert to/from a map.
    private static final String MAX_VERSION_KEY_LABEL = "max_version";

    public SupportedVersionRange(short minVersion, short maxVersion) {
        super(MIN_VERSION_KEY_LABEL, minVersion, MAX_VERSION_KEY_LABEL, maxVersion);
    }

    public SupportedVersionRange(short maxVersion) {
        this((short) 0, maxVersion);
    }

    public static SupportedVersionRange fromMap(Map<String, Short> versionRangeMap) {
        return new SupportedVersionRange(BaseVersionRange.valueOrThrow(MIN_VERSION_KEY_LABEL, versionRangeMap), BaseVersionRange.valueOrThrow(MAX_VERSION_KEY_LABEL, versionRangeMap));
    }

    /**
     * Checks if the version level does *NOT* fall within the [min, max] range of this SupportedVersionRange.
     * @param version the version to be checked
     * @return - true, if the version levels are incompatible
     * - false otherwise
     */
    public boolean isIncompatibleWith(short version) {
        return min() > version || max() < version;
    }
}
