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

#pragma once

#include <avrocpp/Generic.hh>

#include "exec/file_scanner/avro_datum_path.h"
#include "exec/file_scanner/confluent_avro_decoder.h"
#include "exec/file_scanner/file_scanner.h"
#include "formats/avro/cpp/column_reader.h"

namespace starrocks {

class SequentialFile;

// Routine-load (Kafka/Pulsar) Avro scanner on the avrocpp stack. Reads one Confluent-framed message
// per pipe buffer, decodes it to an avro::GenericDatum, and fills columns through the avrocpp column
// readers (which support STRUCT/MAP/ARRAY). Gated by the enable_avro_routine_load_cpp_reader config
// flag (default off).
class AvroStreamScanner final : public FileScanner {
public:
    AvroStreamScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRange& scan_range,
                      ScannerCounter* counter);
    ~AvroStreamScanner() override;

    Status open() override;
    StatusOr<ChunkPtr> get_next() override;
    void close() override;

private:
    Status create_column_readers();
    Status create_src_chunk(ChunkPtr* chunk);
    // Decodes one message and appends one row; per-field "not found" becomes null, while decode and
    // structural failures return an error and fail the task.
    Status parse_one_message(const uint8_t* data, size_t size, Chunk* chunk);
    Status fill_row(const avro::GenericDatum& datum, Chunk* chunk);
    void materialize_src_chunk_adaptive_nullable_column(ChunkPtr& chunk);

    const TBrokerScanRange& _scan_range;
    const int _max_chunk_size;
    cctz::time_zone _timezone;
    std::unique_ptr<ConfluentAvroDecoder> _decoder;
    std::vector<avrocpp::ColumnReaderUniquePtr> _column_readers;
    std::vector<std::vector<SimpleJsonPath>> _json_paths;
    std::shared_ptr<SequentialFile> _file;
    // Reused across messages; the GenericDatum from the previous message is overwritten on each decode.
    avro::GenericDatum _datum;
    bool _closed = false;
};

} // namespace starrocks
