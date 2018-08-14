#pragma once

namespace tpl {

/**
 * Describes the position in the source as 1-based line and column numbers
 */
struct SourcePosition {
  uint64_t line;
  uint64_t column;
};

}  // namespace tpl
