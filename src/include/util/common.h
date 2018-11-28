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

#define INT_TYPES(_) \
  _(i8)              \
  _(i16)             \
  _(i32)             \
  _(i64)             \
  _(u8)              \
  _(u16)             \
  _(u32)             \
  _(u64)

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
