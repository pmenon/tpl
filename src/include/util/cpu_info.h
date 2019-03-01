#pragma once

#include "util/bit_util.h"
#include "util/common.h"
#include "util/macros.h"

namespace tpl::util {

class CpuInfo {
 public:
  // -------------------------------------------------------
  // CPU Features
  // -------------------------------------------------------

  enum Feature : u8 {
    SSE_4_2 = 0,
    AVX = 1,
    AVX2 = 2,
    AVX512 = 3,
  };

  // -------------------------------------------------------
  // Caches
  // -------------------------------------------------------

  // Cache levels
  enum CacheLevel : u8 { L1_CACHE = 0, L2_CACHE = 1, L3_CACHE = 2 };
  // Number of cache levels
  static constexpr const u32 kNumCacheLevels = CacheLevel::L3_CACHE + 1;

  /// Initialize
  static void Init();

  /// Return the number of logical cores in the system
  static u32 GetNumCores() { return kNumCores_; }

  /// Return the size of the cache at level \a level in bytes
  static u32 GetCacheSize(CacheLevel level) {
    TPL_ASSERT(kInitialized_, "CpuInfo not initialized! Call CpuInfo::Init()");
    return kCacheSizes_[level];
  }

  /// Return the size of a cache line at level \a level
  static u32 GetCacheLineSize(CacheLevel level) {
    TPL_ASSERT(kInitialized_, "CpuInfo not initialized! Call CpuInfo::Init()");
    return kCacheLineSizes_[level];
  }

  /// Pretty print CPU information to a string
  static std::string PrettyPrintInfo();

 private:
  // Dispatched from Init() to initialize various components
  static void InitCpuInfo();
  static void InitCacheInfo();

 private:
  static bool kInitialized_;
  static u32 kNumCores_;
  static std::string kModelName_;
  static double kCpuMhz_;
  static u32 kCacheSizes_[kNumCacheLevels];
  static u32 kCacheLineSizes_[kNumCacheLevels];
  static InlinedBitVector<64> kHardwareFlags_;
};

}  // namespace tpl::util