#pragma once

#include <cstdint>

namespace tpl {

static constexpr const uint32_t kBitsPerByte = 8;

/**
 * Describes the position in the source as 1-based line and column numbers
 */
struct SourcePosition {
  uint64_t line;
  uint64_t column;
};

}  // namespace tpl
