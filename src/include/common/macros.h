#pragma once

#include <cassert>

// 99% of cache-lines are 64 bytes
#define CACHELINE_SIZE 64

#define RESTRICT __restrict__
#define UNUSED __attribute__((unused))
#define ALWAYS_INLINE __attribute__((always_inline))
#define NEVER_INLINE __attribute__((noinline))
#define PACKED __attribute__((packed))
#define FALLTHROUGH [[fallthrough]]
#define NORETURN __attribute((noreturn))

// ---------------------------------------------------------
// Macros to force classes to be non-copyable, non-movable,
// or both
// ---------------------------------------------------------

// ---------------------------------------------------------
// Macros to force classes to be non-copyable, non-movable,
// or both
// ---------------------------------------------------------

#define DISALLOW_COPY(klazz)     \
  klazz(const klazz &) = delete; \
  klazz &operator=(const klazz &) = delete;

#define DISALLOW_MOVE(klazz) \
  klazz(klazz &&) = delete;  \
  klazz &operator=(klazz &&) = delete;

#define DISALLOW_COPY_AND_MOVE(klazz) \
  DISALLOW_COPY(klazz)                \
  DISALLOW_MOVE(klazz)

// ---------------------------------------------------------
// Handy branch hints
// ---------------------------------------------------------

#define TPL_LIKELY(expr) __builtin_expect((bool)(expr), true)
#define TPL_UNLIKELY(expr) __builtin_expect((bool)(expr), false)

// ---------------------------------------------------------
// Suped up assertions
// ---------------------------------------------------------

#ifdef NDEBUG
#define TPL_ASSERT(expr, msg) ((void)0)
#else
#define TPL_ASSERT(expr, msg) assert((expr) && (msg))
#endif

// ---------------------------------------------------------
// Indicate that some piece of code is programmer-guaranteed
// unreachable.
// ---------------------------------------------------------

namespace tpl {
NORETURN void tpl_unreachable(const char *msg, const char *file, unsigned int line);
}  // namespace tpl

#define UNREACHABLE(msg) tpl::tpl_unreachable(msg, __FILE__, __LINE__)

// ---------------------------------------------------------
// Google Test ONLY
// ---------------------------------------------------------

#ifdef NDEBUG
#define GTEST_DEBUG_ONLY(TestName) DISABLED_##TestName
#else
#define GTEST_DEBUG_ONLY(TestName) TestName
#endif

#define FRIEND_TEST(test_case_name, test_name) friend class test_case_name##_##test_name##_Test
#define FRIEND_BENCHMARK(bench_name, bench) friend class bench_name##_##bench##_Benchmark