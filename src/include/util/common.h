#pragma once

#include <cstdint>

namespace tpl {

using i8 = int8_t;
using u8 = uint8_t;
using i16 = int16_t;
using u16 = uint16_t;
using i32 = int32_t;
using u32 = uint32_t;
using i64 = int64_t;
using u64 = uint64_t;
using f32 = float;
using f64 = double;

static constexpr const u32 kBitsPerByte = 8;

/**
 * Describes the position in the source as 1-based line and column numbers
 */
struct SourcePosition {
  u64 line;
  u64 column;
};

}  // namespace tpl
