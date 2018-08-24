#pragma once

#include <cstdint>
#include <cstdlib>

#include "util/macros.h"

namespace tpl::util {

class MathUtil {
 public:
  /**
   * Return true if the input value is a power of two > 0
   *
   * @param val The value to check
   * @return True if the value is a power of two > 0
   */
  static constexpr bool IsPowerOf2(uint64_t val) {
    return (val > 0) && ((val & (val - 1)) == 0);
  }

  /**
   * Compute the next power of two greater than the input
   *
   * @param val The input
   * @return The next power of two greater than 'val'
   */
  static constexpr inline uint64_t NextPowerOf2(uint64_t val) {
    val |= (val >> 1);
    val |= (val >> 2);
    val |= (val >> 4);
    val |= (val >> 8);
    val |= (val >> 16);
    val |= (val >> 32);
    return val + 1;
  }

  /**
   * Align the provided input address to the given alignment
   *
   * @param addr
   * @param alignment
   * @return
   */
  static constexpr uintptr_t AlignAddress(uintptr_t addr,
                                          std::size_t alignment) {
    TPL_ASSERT(alignment > 0 && MathUtil::IsPowerOf2(alignment),
               "Alignment is not a power of two!");
    return (addr + alignment - 1) & ~(alignment - 1);
  }

  /**
   * Return the number of bytes needed to make the input address have the
   * desired alignment
   *
   * @param addr
   * @param alignment
   * @return
   */
  static constexpr uintptr_t AlignmentAdjustment(uintptr_t addr,
                                                 size_t alignment) {
    return MathUtil::AlignAddress(addr, alignment) - addr;
  }
};

}  // namespace tpl::util
