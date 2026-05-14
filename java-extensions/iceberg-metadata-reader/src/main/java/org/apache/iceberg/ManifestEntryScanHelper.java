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

import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;

import java.util.List;
import java.util.Map;

// Lives in org.apache.iceberg to access ManifestReader.liveEntries(), which is package-private
// in iceberg 1.10. Iceberg's own PartitionsTable uses the same API to compute last_updated_at /
// last_updated_snapshot_id per partition by iterating ManifestEntry rather than ContentFile,
// so we mirror that here to preserve per-entry snapshot id after manifest rewrites.
public final class ManifestEntryScanHelper {
    private ManifestEntryScanHelper() {
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static CloseableIterable<ManifestEntry<? extends ContentFile<?>>> liveEntries(
            ManifestFile manifest,
            FileIO io,
            Map<Integer, PartitionSpec> specs,
            List<String> select) {
        ManifestReader<?> reader = manifest.content() == ManifestContent.DATA
                ? ManifestFiles.read(manifest, io, specs).select(select).caseSensitive(false)
                : ManifestFiles.readDeleteManifest(manifest, io, specs).select(select).caseSensitive(false);
        return (CloseableIterable) reader.liveEntries();
    }
}
