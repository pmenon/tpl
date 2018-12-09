#pragma once

#include <cstddef>
#include <cstdint>

using i8 = int8_t;
using i16 = int16_t;
using i32 = int32_t;
using i64 = int64_t;
using i128 = __int128;
using u8 = uint8_t;
using u16 = uint16_t;
using u32 = uint32_t;
using u64 = uint64_t;
using f32 = float;
using f64 = double;
using byte = std::byte;

using hash_t = u64;

#define FOR_EACH_SIGNED_INT_TYPE(F) \
  F(i8)                             \
  F(i16)                            \
  F(i32)                            \
  F(i64)

#define FOR_EACH_UNSIGNED_INT_TYPE(F) \
  F(u8)                               \
  F(u16)                              \
  F(u32)                              \
  F(u64)

#define INT_TYPES(F)          \
  FOR_EACH_SIGNED_INT_TYPE(F) \
  FOR_EACH_UNSIGNED_INT_TYPE(F)

namespace tpl {

static constexpr const u32 kBitsPerByte = 8;

/**
 * Describes the position in the source as 1-based line and column numbers
 */
struct SourcePosition {
  u64 line;
  u64 column;
};

}  // namespace tpl
