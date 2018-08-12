#pragma once

#include <cassert>
#include <cstdint>
#include <cstdio>

#define UNUSED __attribute__((unused))

#define DISALLOW_COPY_AND_MOVE(class)  \
  class(class &&) = delete;            \
  class(const class &) = delete;       \
  class &operator=(class &&) = delete; \
  class &operator=(const class &) = delete;

#define TPL_LIKELY(x) __builtin_expect(!!(x), 1)
#define TPL_UNLIKELY(x) __builtin_expect(!!(x), 0)

#ifdef NDEBUG
#define TPL_ASSERT(expr) ((void)0)
#else
#define TPL_ASSERT(expr) assert((expr))
#endif

#ifdef NEBUG
#define UNREACHABLE() __builtin_unreachable();
#else
#define UNREACHABLE()                       \
  do {                                      \
    fprintf(stderr, "unreachable code!\n"); \
    __builtin_unreachable();                \
  } while (false);
#endif