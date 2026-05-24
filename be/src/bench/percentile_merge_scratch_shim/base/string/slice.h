// Build-only minimal Slice stub for the standalone percentile_merge_scratch_bench.
// tdigest.h references Slice only in an unused ctor; this avoids pulling
// base/string/memcmp.h -> gutil/strings/fastmem.h. Used ONLY via the explicit -I
// in run_percentile_merge_scratch_bench.sh; never on the normal cmake build path.
#pragma once
#include <cstddef>
#include <cstring>
#include <string>
namespace starrocks {
struct Slice {
    const char* data = nullptr;
    size_t size = 0;
    Slice() = default;
    Slice(const char* d) : data(d), size(d ? std::strlen(d) : 0) {}
    Slice(const char* d, size_t n) : data(d), size(n) {}
    Slice(const std::string& s) : data(s.data()), size(s.size()) {}
};
} // namespace starrocks
