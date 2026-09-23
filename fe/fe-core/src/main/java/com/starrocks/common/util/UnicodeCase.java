// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/sql/optimizer/rewrite/ScalarOperatorFunctions.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common.util;

// Full default Unicode 17 casing, independent of the JDK Unicode version and default locale.
public final class UnicodeCase {
    private UnicodeCase() {
    }

    public static String lower(String input) {
        return convert(input, false);
    }

    public static String upper(String input) {
        return convert(input, true);
    }

    private static String convert(String input, boolean upper) {
        if (input == null) {
            return null;
        }
        int[] mappings = upper ? UnicodeCaseData.UPPER : UnicodeCaseData.LOWER;
        StringBuilder result = new StringBuilder(input.length());
        for (int offset = 0; offset < input.length();) {
            int cp = input.codePointAt(offset);
            int next = offset + Character.charCount(cp);
            if (!upper && cp == 0x3A3 && finalSigma(input, offset, next)) {
                result.appendCodePoint(0x3C2);
            } else if (cp < 128) {
                int first = upper ? 'a' : 'A';
                result.append((char) (cp >= first && cp < first + 26 ? cp + (upper ? -32 : 32) : cp));
            } else {
                int index = findRange(mappings, 7, cp);
                if (index < 0 || (cp - mappings[index]) % mappings[index + 2] != 0) {
                    result.appendCodePoint(cp);
                } else if (mappings[index + 3] == 1) {
                    result.appendCodePoint(cp + mappings[index + 4]);
                } else {
                    for (int i = 0; i < mappings[index + 3]; ++i) {
                        result.appendCodePoint(mappings[index + 4 + i]);
                    }
                }
            }
            offset = next;
        }
        return result.toString();
    }

    private static int findRange(int[] ranges, int stride, int cp) {
        int lo = 0;
        int hi = ranges.length / stride;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            int index = mid * stride;
            if (cp < ranges[index]) {
                hi = mid;
            } else if (cp > ranges[index + 1]) {
                lo = mid + 1;
            } else {
                return index;
            }
        }
        return -1;
    }

    private static boolean finalSigma(String input, int offset, int next) {
        boolean precededByCased = false;
        for (int i = offset; i > 0;) {
            int cp = input.codePointBefore(i);
            i -= Character.charCount(cp);
            if (findRange(UnicodeCaseData.IGNORABLE, 2, cp) >= 0) {
                continue;
            }
            precededByCased = findRange(UnicodeCaseData.CASED, 2, cp) >= 0;
            break;
        }
        if (!precededByCased) {
            return false;
        }
        for (int i = next; i < input.length();) {
            int cp = input.codePointAt(i);
            i += Character.charCount(cp);
            if (findRange(UnicodeCaseData.IGNORABLE, 2, cp) >= 0) {
                continue;
            }
            return findRange(UnicodeCaseData.CASED, 2, cp) < 0;
        }
        return true;
    }
}
