#include "util/cpu_info.h"

#include <unistd.h>

namespace tpl::util {

bool CpuInfo::initialized_ = false;
u64 CpuInfo::kCacheSizes_[CpuInfo::kNumCacheLevels];
u64 CpuInfo::kCacheLineSizes_[CpuInfo::kNumCacheLevels];

void CpuInfo::InitCpuInfo() {
  TPL_ASSERT(!initialized_, "Should not call post-initialization");
}

void CpuInfo::InitCacheInfo() {
  TPL_ASSERT(!initialized_, "Should not call post-initialization");

  // Use sysconf to determine cache sizes
  kCacheSizes_[L1_CACHE] = static_cast<u64>(sysconf(_SC_LEVEL1_DCACHE_SIZE));
  kCacheSizes_[L2_CACHE] = static_cast<u64>(sysconf(_SC_LEVEL2_CACHE_SIZE));
  kCacheSizes_[L3_CACHE] = static_cast<u64>(sysconf(_SC_LEVEL3_CACHE_SIZE));

  kCacheLineSizes_[L1_CACHE] = static_cast<u64>(sysconf(_SC_LEVEL1_DCACHE_LINESIZE));
  kCacheLineSizes_[L2_CACHE] = static_cast<u64>(sysconf(_SC_LEVEL2_CACHE_LINESIZE));
  kCacheLineSizes_[L3_CACHE] = static_cast<u64>(sysconf(_SC_LEVEL3_CACHE_LINESIZE));
}

void CpuInfo::Init() {
  if (initialized_) {
    return;
  }

  // Initialize each component sequentially
  InitCpuInfo();
  InitCacheInfo();

  // Mark done
  initialized_ = true;
}

}  // namespace tpl::util