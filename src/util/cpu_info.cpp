#include "util/cpu_info.h"

#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/StringRef.h"

#include <unistd.h>
#include <fstream>
#include <sstream>

namespace tpl {

struct {
  CpuInfo::Feature feature;
  llvm::SmallVector<const char *, 4> names;
} features[] = {
    {CpuInfo::SSE_4_2, {"sse4_2"}},
    {CpuInfo::AVX, {"avx"}},
    {CpuInfo::AVX2, {"avx2"}},
    {CpuInfo::AVX512, {"avx512f", "avx512cd"}},
};

void CpuInfo::ParseCpuFlags(llvm::StringRef flags) {
  for (const auto &[feature, names] : features) {
    bool has_feature = true;

    // Check if all feature flag names exist in the flags string. Only if all
    // exist do we claim the whole feature exists.
    for (const auto &name : names) {
      if (!flags.contains(name)) {
        has_feature = false;
        break;
      }
    }

    // Set or don't
    if (has_feature) {
      hardware_flags_.Set(feature);
    } else {
      hardware_flags_.Unset(feature);
    }

  }
}

CpuInfo::CpuInfo() {
  InitCpuInfo();
  InitCacheInfo();
}

void CpuInfo::InitCpuInfo() {
#ifdef __APPLE__
#error "Fix me"
#else
  // On linux, just read /proc/cpuinfo
  std::string line;
  std::ifstream infile("/proc/cpuinfo");
  while (std::getline(infile, line)) {
    llvm::StringRef str(line);

    auto [name, value] = str.split(":");
    value = value.trim(" ");

    if (name.startswith("processor")) {
      num_cores_++;
    } else if (name.startswith("model")) {
      model_name_ = value.str();
    } else if (name.startswith("cpu MHz")) {
      double cpu_mhz;
      value.getAsDouble(cpu_mhz);
      cpu_mhz_ = std::max(cpu_mhz_, cpu_mhz);
    } else if (name.startswith("flags")) {
      ParseCpuFlags(value);
    }
  }
#endif
}

void CpuInfo::InitCacheInfo() {
  // Use sysconf to determine cache sizes
  cache_sizes_[L1_CACHE] = static_cast<u32>(sysconf(_SC_LEVEL1_DCACHE_SIZE));
  cache_sizes_[L2_CACHE] = static_cast<u32>(sysconf(_SC_LEVEL2_CACHE_SIZE));
  cache_sizes_[L3_CACHE] = static_cast<u32>(sysconf(_SC_LEVEL3_CACHE_SIZE));

  cache_line_sizes_[L1_CACHE] =
      static_cast<u32>(sysconf(_SC_LEVEL1_DCACHE_LINESIZE));
  cache_line_sizes_[L2_CACHE] =
      static_cast<u32>(sysconf(_SC_LEVEL2_CACHE_LINESIZE));
  cache_line_sizes_[L3_CACHE] =
      static_cast<u32>(sysconf(_SC_LEVEL3_CACHE_LINESIZE));
}

std::string CpuInfo::PrettyPrintInfo() const {
  std::stringstream ss;

  // clang-format off
  ss << "CPU Info: " << std::endl;
  ss << "  Model:  " << model_name_ << std::endl;
  ss << "  Cores:  " << num_cores_ << std::endl;
  ss << "  Mhz:    " << cpu_mhz_ << std::endl;
  ss << "  Caches: " << std::endl;
  ss << "    L1: " << (cache_sizes_[L1_CACHE] / 1024.0) << " KB (" << cache_line_sizes_[L1_CACHE] << " byte line)" << std::endl;
  ss << "    L2: " << (cache_sizes_[L2_CACHE] / 1024.0) << " KB (" << cache_line_sizes_[L2_CACHE] << " byte line)" << std::endl;
  ss << "    L3: " << (cache_sizes_[L3_CACHE] / 1024.0) << " KB (" << cache_line_sizes_[L3_CACHE] << " byte line)" << std::endl;
  // clang-format on

  ss << "Features: ";
  for (const auto &[feature, names] : features) {
    if (HasFeature(feature)) {
      for (const auto &name : names) {
        ss << name << " ";
      }
    }
  }
  ss << std::endl;

  return ss.str();
}

}  // namespace tpl