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

package com.starrocks.persist;

import com.google.gson.stream.JsonWriter;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.persist.metablock.SRMetaBlockWriterV2;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

public class ImageWriter {
    private final String imageDir;
    private final long imageJournalId;

    private CheckedOutputStream checkedOutputStream;
    private JsonWriter jsonWriter;
    private DataOutputStream dataOutputStream;

    public ImageWriter(String imageDir, long imageJournalId) {
        this.imageDir = imageDir;
        this.imageJournalId = imageJournalId;
    }

    public void setOutputStream(OutputStream outputStream) {
        this.checkedOutputStream = new CheckedOutputStream(outputStream, new CRC32());
        this.dataOutputStream = new DataOutputStream(checkedOutputStream);
        // JsonWriter writes every name, separator and value with its own call, and OutputStreamWriter allocates a
        // char array and a CharBuffer for each call. The buffer turns them into one call per 64 KB. Each block
        // writer flushes it when it closes, before the next block or the checksum.
        this.jsonWriter = new JsonWriter(new BufferedWriter(
                new OutputStreamWriter(checkedOutputStream, StandardCharsets.UTF_8), 64 * 1024));
    }

    public SRMetaBlockWriter getBlockWriter(SRMetaBlockID id, int numJson) throws SRMetaBlockException {
        return new SRMetaBlockWriterV2(jsonWriter, id, numJson);
    }

    public DataOutputStream getDataOutputStream() {
        return dataOutputStream;
    }

    public void saveChecksum() throws IOException {
        File checksumFile = Path.of(imageDir, Storage.CHECKSUM + "." + imageJournalId).toFile();
        String checksum = String.valueOf(checkedOutputStream.getChecksum().getValue());
        try (FileOutputStream fos = new FileOutputStream(checksumFile)) {
            fos.write(checksum.getBytes(StandardCharsets.UTF_8));
            fos.getChannel().force(true);
        }
    }
}
