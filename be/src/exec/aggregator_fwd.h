#pragma once

#include <cstddef>
#include <memory>

#include "common/system/cpu_info.h"

namespace starrocks {
namespace agg {
// Threshold (in bytes) at which a single-level agg hash table flips to
// two-level.  Sized from CpuInfo::get_cache_sizes() so the single-level
// table stays L3-resident on the actual host; falls back to 32 MiB when
// L3 detection returns 0 (preserves the pre-CpuInfo behaviour).  Header-
// only so consumers in the Exprs library (e.g. distinct.h) link without
// pulling in Exec.
#ifdef NDEBUG
inline size_t two_level_memory_threshold() {
    constexpr size_t kDefaultL3 = 32ULL * 1024 * 1024;
    // 512 MiB sanity ceiling: current real-world max is ~384 MiB
    // (EPYC Genoa M7a/C7a).  Anything beyond that is almost certainly
    // a bogus sysconf value; ops can lift via the
    // `two_level_memory_threshold` BE config if a future CPU lands.
    constexpr size_t kMaxL3 = 512ULL * 1024 * 1024;
    const auto& cache_sizes = CpuInfo::get_cache_sizes();
    // Fall back to the default if CpuInfo has not been initialised yet
    // (cache_sizes default-constructs empty) or the entry is missing /
    // unknown -- otherwise indexing past end() is UB and a standalone
    // Exprs consumer that calls this helper before daemon init would
    // crash instead of getting the historical 32 MiB threshold.
    if (cache_sizes.size() <= static_cast<size_t>(CpuInfo::L3_CACHE)) {
        return kDefaultL3;
    }
    const long detected = cache_sizes[CpuInfo::L3_CACHE];
    if (detected <= 0) {
        return kDefaultL3;
    }
    const auto v = static_cast<size_t>(detected);
    return v > kMaxL3 ? kMaxL3 : v;
}
#else
inline size_t two_level_memory_threshold() {
    return 64;
}
#endif
} // namespace agg

class Aggregator;
class SortedStreamingAggregator;
using AggregatorPtr = std::shared_ptr<Aggregator>;
using SortedStreamingAggregatorPtr = std::shared_ptr<SortedStreamingAggregator>;

template <class HashMapWithKey>
struct AllocateState;

template <class T>
class AggregatorFactoryBase;

using AggregatorFactory = AggregatorFactoryBase<Aggregator>;
using AggregatorFactoryPtr = std::shared_ptr<AggregatorFactory>;

using StreamingAggregatorFactory = AggregatorFactoryBase<SortedStreamingAggregator>;
using StreamingAggregatorFactoryPtr = std::shared_ptr<StreamingAggregatorFactory>;

} // namespace starrocks