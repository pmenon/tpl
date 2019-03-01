#include "util/cpu_info.h"

#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/StringRef.h"

#include <unistd.h>
#include <fstream>
#include <sstream>

namespace tpl::util {

bool CpuInfo::kInitialized_ = false;
u32 CpuInfo::kNumCores_ = 0;
std::string CpuInfo::kModelName_;
double CpuInfo::kCpuMhz_ = 0;
u32 CpuInfo::kCacheSizes_[CpuInfo::kNumCacheLevels];
u32 CpuInfo::kCacheLineSizes_[CpuInfo::kNumCacheLevels];
InlinedBitVector<64> CpuInfo::kHardwareFlags_;

struct {
  CpuInfo::Feature feature;
  llvm::SmallVector<const char *, 4> names;
} interested_flags[] = {
    {CpuInfo::SSE_4_2, {"sse4_2"}},
    {CpuInfo::AVX, {"avx"}},
    {CpuInfo::AVX2, {"avx2"}},
    {CpuInfo::AVX512, {"avx512f", "avx512cd"}},
};

void ParseCpuFlags(UNUSED llvm::StringRef flags) {}

void CpuInfo::InitCpuInfo() {
  TPL_ASSERT(!kInitialized_, "Should not call post-initialization");

#ifdef __APPLE__f
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
      kNumCores_++;
    } else if (name.startswith("model")) {
      kModelName_ = value.str();
    } else if (name.startswith("cpu MHz")) {
      double cpu_mhz;
      value.getAsDouble(cpu_mhz);
      kCpuMhz_ = std::max(kCpuMhz_, cpu_mhz);
    } else if (name.startswith("flags")) {
      ParseCpuFlags(value);
    }
  }
#endif
}

void CpuInfo::InitCacheInfo() {
  TPL_ASSERT(!kInitialized_, "Should not call post-initialization");

  // Use sysconf to determine cache sizes
  kCacheSizes_[L1_CACHE] = static_cast<u32>(sysconf(_SC_LEVEL1_DCACHE_SIZE));
  kCacheSizes_[L2_CACHE] = static_cast<u32>(sysconf(_SC_LEVEL2_CACHE_SIZE));
  kCacheSizes_[L3_CACHE] = static_cast<u32>(sysconf(_SC_LEVEL3_CACHE_SIZE));

  kCacheLineSizes_[L1_CACHE] =
      static_cast<u32>(sysconf(_SC_LEVEL1_DCACHE_LINESIZE));
  kCacheLineSizes_[L2_CACHE] =
      static_cast<u32>(sysconf(_SC_LEVEL2_CACHE_LINESIZE));
  kCacheLineSizes_[L3_CACHE] =
      static_cast<u32>(sysconf(_SC_LEVEL3_CACHE_LINESIZE));
}

void CpuInfo::Init() {
  if (kInitialized_) {
    return;
  }

  // Initialize each component sequentially
  InitCpuInfo();
  InitCacheInfo();

  // Mark done
  kInitialized_ = true;
}

std::string CpuInfo::PrettyPrintInfo() {
  std::stringstream ss;

  // clang-format off
  ss << "CPU Info:" << std::endl;
  ss << "  Model: " << kModelName_ << std::endl;
  ss << "  Cores: " << kNumCores_ << std::endl;
  ss << "  Mhz:   " << kCpuMhz_ << std::endl;
  ss << "  Caches: " << kNumCacheLevels << std::endl;
  ss << "    L1: " << (kCacheSizes_[L1_CACHE] / 1024.0) << " KB (" << kCacheLineSizes_[L1_CACHE] << " byte line)" << std::endl;
  ss << "    L2: " << (kCacheSizes_[L2_CACHE] / 1024.0) << " KB (" << kCacheLineSizes_[L2_CACHE] << " byte line)" << std::endl;
  ss << "    L3: " << (kCacheSizes_[L3_CACHE] / 1024.0) << " KB (" << kCacheLineSizes_[L3_CACHE] << " byte line)" << std::endl;
  // clang-format on

  return ss.str();
}

}  // namespace tpl::util