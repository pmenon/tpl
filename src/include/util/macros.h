#pragma once

#include <cassert>
#include <cstdint>

#include "llvm/Support/ErrorHandling.h"

#define UNUSED __attribute__((unused))
#define ALWAYS_INLINE __attribute__((noinline))

#define DISALLOW_COPY_AND_MOVE(class)  \
  class(class &&) = delete;            \
  class(const class &) = delete;       \
  class &operator=(class &&) = delete; \
  class &operator=(const class &) = delete;

#define TPL_LIKELY(x) LLVM_LIKELY(x)
#define TPL_UNLIKELY(x) LLVM_UNLIKELY(x)

#ifdef NDEBUG
#define TPL_ASSERT(expr, msg) ((void)0)
#else
#define TPL_ASSERT(expr, msg) assert((expr) && (msg))
#endif

#define UNREACHABLE(msg) llvm_unreachable(msg)