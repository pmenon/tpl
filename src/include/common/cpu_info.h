#pragma once

#include <bitset>
#include <string>

#include "common/common.h"

namespace llvm {
class StringRef;
}  // namespace llvm

namespace tpl {

class CpuInfo {
 public:
  // -------------------------------------------------------
  // CPU Features
  // -------------------------------------------------------

  enum Feature : uint8_t {
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
  enum CacheLevel : uint8_t { L1_CACHE = 0, L2_CACHE = 1, L3_CACHE = 2 };

  // Number of cache levels
  static constexpr const uint32_t kNumCacheLevels = CacheLevel::L3_CACHE + 1;

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
   * @return The total number of physical processor packages in the system.
   */
  uint32_t GetNumProcessors() const noexcept { return num_processors_; }

  /**
   * @return The total number of physical cores in the system.
   */
  uint32_t GetNumPhysicalCores() const noexcept { return num_physical_cores_; }

  /**
   * @return The total number of logical cores in the system.
   */
  uint32_t GetNumLogicalCores() const noexcept { return num_logical_cores_; }

  /**
   * @return The size of the cache at level @em level in bytes.
   */
  uint32_t GetCacheSize(const CacheLevel level) const noexcept { return cache_sizes_[level]; }

  /**
   * @return The size of a cache line at level @em level.
   */
  uint32_t GetCacheLineSize(const CacheLevel level) const noexcept {
    return cache_line_sizes_[level];
  }

  /**
   * @return True if the CPU has the input hardware feature @em feature; false otherwise;
   */
  bool HasFeature(const Feature feature) const noexcept { return hardware_flags_[feature]; }

  /**
   * Pretty print CPU information to a string.
   * @return A string-representation of the CPU information.
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
  uint32_t num_logical_cores_;
  uint32_t num_physical_cores_;
  uint32_t num_processors_;
  std::string model_name_;
  double cpu_mhz_;
  uint32_t cache_sizes_[kNumCacheLevels];
  uint32_t cache_line_sizes_[kNumCacheLevels];
  std::bitset<Feature::MAX> hardware_flags_;
};

}  // namespace tpl
