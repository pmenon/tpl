#pragma once

#include <cstddef>
#include <cstdint>

/// Common integral type shorthands
using byte = std::byte;
using sel_t = uint16_t;
using hash_t = uint64_t;
using int128_t = __int128;
using uint128_t = unsigned __int128;

#define FOR_EACH_SIGNED_INT_TYPE(F, ...) \
  F(int8_t, __VA_ARGS__)                 \
  F(int16_t, __VA_ARGS__)                \
  F(int32_t, __VA_ARGS__)                \
  F(int64_t, __VA_ARGS__)

#define FOR_EACH_UNSIGNED_INT_TYPE(F, ...) \
  F(uint8_t, __VA_ARGS__)                  \
  F(uint16_t, __VA_ARGS__)                 \
  F(uint32_t, __VA_ARGS__)                 \
  F(uint64_t, __VA_ARGS__)

#define FOR_EACH_FLOAT_TYPE(F, ...) \
  F(float, __VA_ARGS__)             \
  F(double, __VA_ARGS__)

#define INT_TYPES(F, ...)                  \
  FOR_EACH_SIGNED_INT_TYPE(F, __VA_ARGS__) \
  FOR_EACH_UNSIGNED_INT_TYPE(F, __VA_ARGS__)

#define ALL_NUMERIC_TYPES(F, ...) \
  INT_TYPES(F, __VA_ARGS__)       \
  FOR_EACH_FLOAT_TYPE(F, __VA_ARGS__)

namespace tpl {

/**
 * A compact structure used during parsing to capture and describe the position in a source file.
 * Tracked as a 1-based line and column number.
 */
struct SourcePosition {
  uint64_t line;
  uint64_t column;
};

/**
 * Base for classes that should NOT be instantiated, i.e., classes that only have static functions.
 */
class AllStatic {
#ifndef NDEBUG
 public:
  AllStatic() = delete;
#endif
};

/**
 * Enumeration used to classify locality of reference for memory accesses.
 */
enum class Locality : uint8_t { None = 0, Low = 1, Medium = 2, High = 3 };

/** The number of bits per byte */
static constexpr const uint32_t kBitsPerByte = 8;

/** The default vector size to use when performing vectorized iteration */
static constexpr const uint32_t kDefaultVectorSize = 2048;

/** The default prefetch distance to use */
static constexpr const uint32_t kPrefetchDistance = 16;

/** Common memory sizes */
static constexpr const uint32_t KB = 1024;
static constexpr const uint32_t MB = KB * KB;
static constexpr const uint32_t GB = KB * KB * KB;

}  // namespace tpl
