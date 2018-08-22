#pragma once

#include <cstdint>

namespace tpl::util {

class MathUtil {
 public:
  /**
   * Return true if the input value is a power of two > 0
   *
   * @param val The value to check
   * @return True if the value is a power of two > 0
   */
  static constexpr inline bool IsPowerOf2(uint64_t val) {
    return (val > 0) && ((val & (val - 1)) == 0);
  }
};

}  // namespace tpl::util
