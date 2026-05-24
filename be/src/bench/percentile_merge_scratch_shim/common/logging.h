// Build-only stub so percentile_merge_scratch_bench can compile types/tdigest.cpp
// standalone (no glog/gutil/Base link). Used ONLY via the explicit -I in
// run_percentile_merge_scratch_bench.sh; never on the normal cmake build path.
#pragma once
#include <iostream>
namespace starrocks {
struct _NL {
    template <class T>
    _NL& operator<<(const T&) {
        return *this;
    }
};
} // namespace starrocks
#define VLOG(x) ::starrocks::_NL()
#define LOG(x) ::starrocks::_NL()
#define DCHECK(x) ::starrocks::_NL()
#define DCHECK_EQ(a, b) ::starrocks::_NL()
#define DCHECK_NE(a, b) ::starrocks::_NL()
#define DCHECK_GT(a, b) ::starrocks::_NL()
#define DCHECK_GE(a, b) ::starrocks::_NL()
#define DCHECK_LT(a, b) ::starrocks::_NL()
#define DCHECK_LE(a, b) ::starrocks::_NL()
#define CHECK(x) ::starrocks::_NL()
