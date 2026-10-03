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

#include "exprs/stats_functions.h"

#include <charconv>
#include <iomanip>
#include <queue>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/agg/exact_degree_state.h"
#include "runtime/runtime_state.h"
#include "util/hash_util.hpp"

namespace starrocks {

StatusOr<ColumnPtr> StatsFunctions::tuple_key(FunctionContext* context, const Columns& columns) {
    std::vector<ColumnViewer<TYPE_VARCHAR>> viewers;
    viewers.reserve(columns.size());
    for (const auto& column : columns) {
        viewers.emplace_back(column);
    }

    const size_t size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> builder(size);
    std::string key;
    for (size_t row = 0; row < size; ++row) {
        key.clear();
        for (size_t i = 0; i < viewers.size(); ++i) {
            if (i > 0) {
                key.push_back('#');
            }
            if (viewers[i].is_null(row)) {
                key += "\\N";
                continue;
            }
            Slice value = viewers[i].value(row);
            for (size_t j = 0; j < value.size; ++j) {
                char c = value.data[j];
                if (c == '\\' || c == '#') {
                    key.push_back('\\');
                }
                key.push_back(c);
            }
        }
        builder.append(Slice(key));
    }
    return builder.build(ColumnHelper::is_all_const(columns));
}

namespace {
void degree_check_cancel(FunctionContext* context, uint64_t& visits) {
    if ((++visits & 4095) == 0 && context->state() != nullptr && context->state()->is_cancelled()) {
        throw std::runtime_error("Exact degree collection cancelled");
    }
}
ExactDegreeState degree_read(const ColumnViewer<TYPE_VARBINARY>& column, size_t row) {
    return column.is_null(row) ? ExactDegreeState() : ExactDegreeState::deserialize(column.value(row));
}
// Views refer only to immutable input columns during this function call. Never retain them
// in FunctionContext or a process cache: subsequent chunks can reuse the backing buffers.
class DegreeChunkCache {
public:
    const ExactDegreeState& read(const ColumnViewer<TYPE_VARBINARY>& input, size_t row) {
        if (input.is_null(row)) return _empty;
        Slice bytes = input.value(row);
        std::string_view key(bytes.data, bytes.size);
        auto found = _states.find(key);
        if (found != _states.end()) return found->second;
        _scratch = ExactDegreeState::deserialize(bytes);
        size_t charge = 512 + 2 * _scratch.keys.get_size_in_bytes() + 17 * _scratch.extra.capacity();
        if (_states.size() < 64 && charge <= BUDGET - _bytes) {
            _bytes += charge;
            return _states.emplace(key, std::move(_scratch)).first->second;
        }
        return _scratch;
    }

private:
    static constexpr size_t BUDGET = 4 * 1024 * 1024;
    std::unordered_map<std::string_view, ExactDegreeState> _states;
    ExactDegreeState _empty;
    ExactDegreeState _scratch;
    size_t _bytes = 0;
};
} // namespace

StatusOr<ColumnPtr> StatsFunctions::degree_info(FunctionContext* context, const Columns& columns) {
    try {
        ColumnViewer<TYPE_VARBINARY> input(columns[0]);
        ColumnBuilder<TYPE_VARCHAR> out(columns[0]->size());
        for (size_t row = 0; row < columns[0]->size(); ++row) {
            auto state = degree_read(input, row);
            uint64_t ndv = state.keys.cardinality(), maximum = ndv != 0;
            std::array<long double, 10> moments;
            moments.fill(ndv);
            for (auto [key, extra] : state.extra) {
                uint64_t d = extra + 1;
                maximum = std::max(maximum, d);
                long double power = 1;
                for (int p = 0; p < 10; ++p) {
                    power *= d;
                    moments[p] += power - 1;
                }
            }
            std::ostringstream json;
            json << std::setprecision(17) << "{\"rows\":" << state.rows << ",\"ndv\":" << ndv << ",\"max\":" << maximum
                 << ",\"extra_keys\":" << state.extra.size() << ",\"moments\":[";
            for (int p = 0; p < 10; ++p) {
                if (p) json << ',';
                json << double(moments[p]);
            }
            json << "]}";
            auto value = json.str();
            out.append(Slice(value));
        }
        return out.build(ColumnHelper::is_all_const(columns));
    } catch (const std::exception& e) {
        return Status::InvalidArgument(e.what());
    }
}

StatusOr<ColumnPtr> StatsFunctions::degree_pair(FunctionContext* context, const Columns& columns) {
    try {
        ColumnViewer<TYPE_VARBINARY> left(columns[0]), right(columns[1]);
        ColumnBuilder<TYPE_VARCHAR> out(columns[0]->size());
        DegreeChunkCache left_cache, right_cache;
        uint64_t visits = 0;
        for (size_t row = 0; row < columns[0]->size(); ++row) {
            degree_check_cancel(context, visits);
            const auto& l = left_cache.read(left, row);
            const auto& r = right_cache.read(right, row);
            auto values = l.pair(r);
            std::ostringstream json;
            json << std::setprecision(17) << '[';
            for (int i = 0; i < 4; ++i) {
                if (i) json << ',';
                json << values[i];
            }
            json << ']';
            auto value = json.str();
            out.append(Slice(value));
        }
        return out.build(ColumnHelper::is_all_const(columns));
    } catch (const std::exception& e) {
        return Status::InvalidArgument(e.what());
    }
}

StatusOr<ColumnPtr> StatsFunctions::degree_head(FunctionContext* context, const Columns& columns) {
    try {
        std::vector<ColumnViewer<TYPE_VARBINARY>> input;
        for (int i = 0; i < 4; ++i) input.emplace_back(columns[i]);
        ColumnViewer<TYPE_INT> limits(columns[4]);
        ColumnBuilder<TYPE_VARBINARY> out(columns[0]->size());
        for (size_t row = 0; row < columns[0]->size(); ++row) {
            int limit = limits.value(row);
            if (limit < 0 || limit > 16384) return Status::InvalidArgument("Invalid exact degree head budget");
            std::array<ExactDegreeState, 4> source;
            for (int i = 0; i < 4; ++i) source[i] = degree_read(input[i], row);
            BitmapValue candidates;
            for (int i = 0; i < 4; ++i)
                for (int j = 0; j < i; ++j) {
                    BitmapValue common = source[i].keys;
                    common &= source[j].keys;
                    candidates |= common;
                }
            int active = 0;
            bool unit = true;
            for (const auto& part : source) {
                active += part.rows != 0;
                unit &= part.extra.empty();
            }
            if (active == 2 && unit) {
                BitmapValueIter it;
                it.reset(candidates);
                std::vector<uint64_t> keys(limit);
                auto n = it.next_batch(keys.data(), keys.size());
                ExactDegreeState head;
                for (size_t i = 0; i < n; ++i) head.update(ExactDegreeState::decode(keys[i]));
                auto value = head.serialize();
                out.append(Slice(value));
                continue;
            }
            using Entry = std::pair<double, uint64_t>;
            auto better = [](const Entry& l, const Entry& r) {
                return l.first > r.first || (l.first == r.first && l.second < r.second);
            };
            std::priority_queue<Entry, std::vector<Entry>, decltype(better)> best(better);
            uint64_t visits = 0;
            if (limit > 0)
                ExactDegreeState::each(candidates, [&](uint64_t key) {
                    degree_check_cancel(context, visits);
                    std::array<double, 4> n;
                    for (int i = 0; i < 4; ++i) n[i] = source[i].frequency(key);
                    double score = 0;
                    for (int i = 0; i < 4; ++i)
                        for (int j = 0; j < i; ++j) score += n[i] * n[j];
                    Entry entry(score, key);
                    if (best.size() < limit)
                        best.push(entry);
                    else if (better(entry, best.top())) {
                        best.pop();
                        best.push(entry);
                    }
                });
            ExactDegreeState head;
            while (!best.empty()) {
                head.update(ExactDegreeState::decode(best.top().second));
                best.pop();
            }
            auto value = head.serialize();
            out.append(Slice(value));
        }
        return out.build(ColumnHelper::is_all_const(columns));
    } catch (const std::exception& e) {
        return Status::InvalidArgument(e.what());
    }
}

StatusOr<ColumnPtr> StatsFunctions::degree_tail(FunctionContext* context, const Columns& columns) {
    try {
        ColumnViewer<TYPE_VARBINARY> input(columns[0]), heads(columns[1]);
        ColumnBuilder<TYPE_VARCHAR> out(columns[0]->size());
        constexpr std::array<int, 8> orders{0, 2, 3, 4, 6, 8, 9, 12};
        const std::array<std::string, 3> salts{"join-statistics-0", "join-statistics-1", "join-statistics-2"};
        for (size_t row = 0; row < columns[0]->size(); ++row) {
            auto source = degree_read(input, row), head = degree_read(heads, row);
            std::array<std::array<long double, 768>, 8> moments{};
            auto buckets = [&](uint64_t key) {
                char number[32];
                auto last = std::to_chars(number, number + sizeof(number), ExactDegreeState::decode(key));
                uint64_t seed = HashUtil::xx_hash3_64(number, last.ptr - number, HashUtil::XXHASH3_64_SEED);
                std::array<int, 3> b;
                for (int layout = 0; layout < 3; ++layout) {
                    b[layout] = (HashUtil::xx_hash3_64(salts[layout].data(), salts[layout].size(), seed) & 255) +
                                layout * 256;
                }
                return b;
            };
            uint64_t visits = 0;
            ExactDegreeState::each(source.keys, [&](uint64_t key) {
                degree_check_cancel(context, visits);
                if (!head.keys.contains(key))
                    for (int b : buckets(key)) moments[0][b] += 1;
            });
            for (int p = 1; p < 8; ++p) moments[p] = moments[0];
            for (auto [key, extra] : source.extra) {
                if (head.keys.contains(key)) continue;
                auto b = buckets(key);
                long double degree = extra + 1, value = 1;
                int slot = 1;
                for (int p = 1; p <= 12; ++p) {
                    value *= degree;
                    if (slot < 8 && p == orders[slot]) {
                        for (int bucket : b) moments[slot][bucket] += value - 1;
                        ++slot;
                    }
                }
            }
            std::ostringstream json;
            json << std::setprecision(17) << "{\"head\":[";
            bool comma = false;
            ExactDegreeState::each(head.keys, [&](uint64_t key) {
                uint64_t d = source.frequency(key);
                if (d == 0) return;
                if (comma) json << ',';
                comma = true;
                json << '[' << ExactDegreeState::decode(key) << ',' << d << ']';
            });
            json << "],\"tail\":[";
            for (int p = 0; p < 8; ++p) {
                if (p) json << ',';
                json << '[';
                for (int b = 0; b < 768; ++b) {
                    if (b) json << ',';
                    json << double(moments[p][b]);
                }
                json << ']';
            }
            json << "]}";
            auto value = json.str();
            out.append(Slice(value));
        }
        return out.build(ColumnHelper::is_all_const(columns));
    } catch (const std::exception& e) {
        return Status::InvalidArgument(e.what());
    }
}

namespace {
// A rectangle contains 256 left keys x 128 right keys. Its local pair ID
// uses 15 value bits and two NULL flags; original shards retain all signed key bits.
uint64_t degree_pair_id(uint64_t key) {
    int64_t value = ExactDegreeState::decode(key);
    if (value < 0 || value >= 131072) throw std::runtime_error("Invalid local degree pair ID");
    return value;
}
uint64_t degree_original_key(int64_t shard, uint64_t offset, int bits) {
    if (shard < (std::numeric_limits<int64_t>::min() >> bits) ||
        shard > (std::numeric_limits<int64_t>::max() >> bits)) {
        throw std::runtime_error("Invalid degree pair shard");
    }
    return ExactDegreeState::encode(std::bit_cast<int64_t>((uint64_t(shard) << bits) | offset));
}
} // namespace

StatusOr<ColumnPtr> StatsFunctions::degree_project(FunctionContext* context, const Columns& columns) {
    try {
        ColumnViewer<TYPE_VARBINARY> input(columns[0]);
        ColumnViewer<TYPE_BIGINT> shards(columns[1]);
        ColumnViewer<TYPE_INT> sides(columns[2]);
        ColumnBuilder<TYPE_VARBINARY> out(columns[0]->size());
        for (size_t row = 0; row < columns[0]->size(); ++row) {
            ExactDegreeState result;
            if (!shards.is_null(row) && !sides.is_null(row)) {
                int side = sides.value(row);
                if (side != 0 && side != 1) throw std::runtime_error("Invalid degree projection side");
                auto source = degree_read(input, row);
                std::array<uint64_t, 256> counts{};
                uint64_t visits = 0;
                ExactDegreeState::each(source.keys, [&](uint64_t key) {
                    degree_check_cancel(context, visits);
                    uint64_t pair = degree_pair_id(key);
                    if (pair & (uint64_t(1) << (15 + side))) return;
                    pair &= 32767;
                    auto extra = source.extra.find(key);
                    ExactDegreeState::checked_add(counts[side == 0 ? pair >> 7 : pair & 127],
                                                  extra == source.extra.end() ? 1 : extra->second + 1);
                });
                for (size_t i = 0; i < (side == 0 ? 256 : 128); ++i) {
                    if (!counts[i]) continue;
                    uint64_t key = degree_original_key(shards.value(row), i, side == 0 ? 8 : 7);
                    result.keys.add(key);
                    ExactDegreeState::checked_add(result.rows, counts[i]);
                    if (counts[i] > 1) result.extra.emplace(key, counts[i] - 1);
                }
            }
            auto bytes = result.serialize();
            out.append(Slice(bytes));
        }
        return out.build(ColumnHelper::is_all_const(columns));
    } catch (const std::exception& e) {
        return Status::InvalidArgument(e.what());
    }
}

StatusOr<ColumnPtr> StatsFunctions::degree_intra(FunctionContext* context, const Columns& columns) {
    try {
        ColumnViewer<TYPE_VARBINARY> input(columns[0]), left(columns[1]), right(columns[2]);
        ColumnViewer<TYPE_BIGINT> ls(columns[3]), rs(columns[4]);
        ColumnBuilder<TYPE_VARCHAR> out(columns[0]->size());
        for (size_t row = 0; row < columns[0]->size(); ++row) {
            uint64_t support = 0;
            std::array<long double, 3> moments{};
            if (!ls.is_null(row) && !rs.is_null(row)) {
                auto pairs = degree_read(input, row), l = degree_read(left, row), r = degree_read(right, row);
                std::array<uint64_t, 256> lf;
                std::array<uint64_t, 128> rf;
                for (size_t i = 0; i < lf.size(); ++i) lf[i] = l.frequency(degree_original_key(ls.value(row), i, 8));
                for (size_t i = 0; i < rf.size(); ++i) rf[i] = r.frequency(degree_original_key(rs.value(row), i, 7));
                uint64_t visits = 0;
                ExactDegreeState::each(pairs.keys, [&](uint64_t key) {
                    degree_check_cancel(context, visits);
                    uint64_t pair = degree_pair_id(key);
                    if (pair & 98304) return; // Ordinary equality excludes either NULL key.
                    uint64_t a = lf[pair >> 7], b = rf[pair & 127];
                    if (!a || !b) throw std::runtime_error("Missing degree pair marginal");
                    ++support;
                    // Existing intra constraints count distinct pairs, not source rows.
                    double weight = double(a) * double(b), power = weight;
                    for (int p = 0; p < 3; ++p) {
                        moments[p] += power;
                        power *= weight;
                    }
                });
            }
            std::ostringstream json;
            json << std::setprecision(17) << '[' << support;
            for (auto value : moments) json << ',' << double(value);
            json << ']';
            auto value = json.str();
            out.append(Slice(value));
        }
        return out.build(ColumnHelper::is_all_const(columns));
    } catch (const std::exception& e) {
        return Status::InvalidArgument(e.what());
    }
}

} // namespace starrocks

#include "gen_cpp/opcode/StatsFunctions.inc"
