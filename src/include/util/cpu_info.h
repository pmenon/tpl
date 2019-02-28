#pragma once

#include "util/common.h"
#include "util/macros.h"

namespace tpl::util {

class CpuInfo {
 public:
  // Cache levels
  enum CacheLevel : u8 { L1_CACHE = 0, L2_CACHE = 1, L3_CACHE = 2 };

  // Number of caches
  static constexpr const u32 kNumCacheLevels = CacheLevel::L3_CACHE + 1;

  /// Initialize
  static void Init();

  /// Return the size of the cache at level \a level in bytes
  static u64 GetCacheSize(CacheLevel level) {
    TPL_ASSERT(initialized_, "CpuInfo not initialized! Call CpuInfo::Init()");
    return kCacheSizes_[level];
  }

  /// Return the size of a cache line at level \a level
  static u64 GetCacheLineSize(CacheLevel level) {
    TPL_ASSERT(initialized_, "CpuInfo not initialized! Call CpuInfo::Init()");
    return kCacheLineSizes_[level];
  }

 private:
  // Dispatched from Init() to initialize various components
  static void InitCpuInfo();
  static void InitCacheInfo();

 private:
  static bool initialized_;
  static u64 kCacheSizes_[kNumCacheLevels];
  static u64 kCacheLineSizes_[kNumCacheLevels];
};

}  // namespace tpl::util