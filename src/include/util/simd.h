#pragma once

#include "util/common.h"
#include "util/macros.h"

namespace tpl::util::simd {

struct Bitwidth {
  static constexpr const uint32_t
#if defined(__AVX512F__)
      value = 512;
#elif defined(__AVX2__)
      value = 256;
#else
      value = 256;
#endif
};

template <typename T>
struct Lane {
  static constexpr const uint32_t count = Bitwidth::value / (sizeof(T) * 8);
};

}  // namespace tpl::util::simd

#define SIMD_TOP_LEVEL

#if defined(__AVX512F__)
#include "util/simd/avx512.h"
#elif defined(__AVX2__)
#include "util/simd/avx2.h"
#else
#error "Compiler must support at least AVX2"
#endif

#undef SIMD_TOP_LEVEL
