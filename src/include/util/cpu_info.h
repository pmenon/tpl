#pragma once

#include <bitset>
#include <string>

#include "util/common.h"

namespace llvm {
class StringRef;
}  // namespace llvm

namespace tpl {

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

    // Don't add any features below this comment. If you add more features,
    // remember to modify the value of MAX below.
    MAX,
  };

  // -------------------------------------------------------
  // Caches
  // -------------------------------------------------------

  // Cache levels
  enum CacheLevel : u8 { L1_CACHE = 0, L2_CACHE = 1, L3_CACHE = 2 };

  // Number of cache levels
  static constexpr const u32 kNumCacheLevels = CacheLevel::L3_CACHE + 1;

  // -------------------------------------------------------
  // Main API
  // -------------------------------------------------------

  /**
   * Singletons are bad blah blah blah
   */
  static CpuInfo *Instance() {
    static CpuInfo instance;
    return &instance;
  }

  /**
   * Return the total number of physical processor packages in the system.
   */
  u32 GetNumProcessors() const noexcept { return num_processors_; }

  /**
   * Return the total number of physical cores in the system.
   */
  u32 GetNumPhysicalCores() const noexcept { return num_physical_cores_; }

  /**
   * Return the total number of logical cores in the system.
   */
  u32 GetNumLogicalCores() const noexcept { return num_logical_cores_; }

  /**
   * Return the size of the cache at level @em level in bytes.
   */
  u32 GetCacheSize(const CacheLevel level) const noexcept { return cache_sizes_[level]; }

  /**
   * Return the size of a cache line at level @em level.
   */
  u32 GetCacheLineSize(const CacheLevel level) const noexcept { return cache_line_sizes_[level]; }

  /**
   * Does the CPU have the given hardware feature?
   */
  bool HasFeature(const Feature feature) const noexcept { return hardware_flags_[feature]; }

  /**
   * Pretty print CPU information to a string.
   */
  std::string PrettyPrintInfo() const;

 private:
  // Initialize
  void InitCpuInfo();
  void InitCacheInfo();

  void ParseCpuFlags(llvm::StringRef flags);

 private:
  CpuInfo();

 private:
  u32 num_logical_cores_;
  u32 num_physical_cores_;
  u32 num_processors_;
  std::string model_name_;
  double cpu_mhz_;
  u32 cache_sizes_[kNumCacheLevels];
  u32 cache_line_sizes_[kNumCacheLevels];
  std::bitset<Feature::MAX> hardware_flags_;
};

}  // namespace tpl
