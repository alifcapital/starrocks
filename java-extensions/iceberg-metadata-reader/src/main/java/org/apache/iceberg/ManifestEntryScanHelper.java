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

package org.apache.iceberg;

import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;

import java.util.List;
import java.util.Map;

public class ManifestEntryScanHelper {
    private ManifestEntryScanHelper() {}

    @SuppressWarnings("unchecked")
    public static CloseableIterator<ManifestEntryWrapper> open(
            ManifestFile manifestFile,
            FileIO io,
            Map<Integer, PartitionSpec> specs,
            List<String> columns) {
        CloseableIterator<ManifestEntry<?>> entries;
        if (manifestFile.content() == ManifestContent.DATA) {
            CloseableIterator<ManifestEntry<DataFile>> dataEntries = ManifestFiles.read(manifestFile, io, specs)
                    .select(columns)
                    .caseSensitive(false)
                    .entries()
                    .iterator();
            entries = (CloseableIterator<ManifestEntry<?>>) (CloseableIterator<?>) dataEntries;
        } else {
            CloseableIterator<ManifestEntry<DeleteFile>> deleteEntries =
                    ManifestFiles.readDeleteManifest(manifestFile, io, specs)
                            .select(columns)
                            .caseSensitive(false)
                            .entries()
                            .iterator();
            entries = (CloseableIterator<ManifestEntry<?>>) (CloseableIterator<?>) deleteEntries;
        }
        return CloseableIterator.transform(entries,
                entry -> new ManifestEntryWrapper(entry.snapshotId(), entry.file()));
    }
}
